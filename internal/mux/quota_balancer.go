package mux

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"
)

const (
	quotaBalanceInterval      = 60 * time.Second
	quotaRebalanceMinGap      = 15.0
	quotaRebalanceCooldown    = 5 * time.Minute
	quotaRebalanceBoundaryLag = 100 * time.Millisecond

	quotaRebalanceShortGuard  = 80.0
	quotaRebalanceWeeklyGuard = 90.0
	quotaRebalanceGuardGap    = 8.0
)

type quotaPressure struct {
	combined float64
	short    float64
	weekly   float64
}

type quotaRebalancePlan struct {
	root       string
	source     string
	target     string
	generation uint64
}

func clampQuotaPercent(value float64) float64 {
	return math.Max(
		0,
		math.Min(
			100,
			value,
		),
	)
}

func quotaPressureForSnapshot(
	snapshot AccountSnapshot,
) (
	quotaPressure,
	bool,
	bool,
) {
	if !snapshot.Enabled ||
		!snapshot.Connected ||
		snapshot.AuthType != "chatgpt" ||
		snapshot.RateLimits == nil {
		return quotaPressure{},
			false,
			false
	}

	weekly, short :=
		longestAndShortestWindow(
			snapshot.RateLimits,
		)

	pressure := quotaPressure{}

	if short != nil {
		pressure.short =
			clampQuotaPercent(
				short.UsedPercent,
			)
	}

	if weekly != nil {
		pressure.weekly =
			clampQuotaPercent(
				weekly.UsedPercent,
			)
	}

	pressure.combined =
		math.Max(
			pressure.short,
			pressure.weekly,
		)

	usable :=
		rateLimitsHaveCapacity(
			snapshot.RateLimits,
		)

	return pressure,
		usable,
		true
}

func shouldQuotaRebalance(
	source quotaPressure,
	target quotaPressure,
) bool {
	if source.combined-
		target.combined >=
		quotaRebalanceMinGap {
		return true
	}

	if source.short >=
		quotaRebalanceShortGuard &&
		source.short-target.short >=
			quotaRebalanceGuardGap {
		return true
	}

	if source.weekly >=
		quotaRebalanceWeeklyGuard &&
		source.weekly-target.weekly >=
			quotaRebalanceGuardGap {
		return true
	}

	return false
}

func betterQuotaRebalanceTarget(
	candidateID string,
	candidate quotaPressure,
	currentID string,
	current quotaPressure,
) bool {
	if currentID == "" {
		return true
	}

	if math.Abs(
		candidate.combined-
			current.combined,
	) > 0.001 {
		return candidate.combined <
			current.combined
	}

	if math.Abs(
		candidate.short-
			current.short,
	) > 0.001 {
		return candidate.short <
			current.short
	}

	if math.Abs(
		candidate.weekly-
			current.weekly,
	) > 0.001 {
		return candidate.weekly <
			current.weekly
	}

	return candidateID < currentID
}

func (m *Multiplexer) quotaRebalanceNow() time.Time {
	if m.now != nil {
		return m.now()
	}

	return time.Now()
}

func (m *Multiplexer) quotaBalanceLoop(
	ctx context.Context,
) {
	ticker :=
		time.NewTicker(
			quotaBalanceInterval,
		)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			passCtx, cancel :=
				context.WithTimeout(
					ctx,
					2*requestTimeout,
				)

			m.planQuotaRebalances(
				passCtx,
			)

			cancel()
		}
	}
}

func (m *Multiplexer) planQuotaRebalances(
	ctx context.Context,
) {
	snapshots :=
		m.accountSnapshots(
			ctx,
			false,
		)

	type quotaState struct {
		pressure quotaPressure
		usable   bool
	}

	states :=
		make(
			map[string]quotaState,
			len(snapshots),
		)

	for _, snapshot := range snapshots {
		pressure, usable, known :=
			quotaPressureForSnapshot(
				snapshot,
			)

		if !known {
			continue
		}

		states[snapshot.ID] =
			quotaState{
				pressure: pressure,
				usable:   usable,
			}
	}

	m.activeTurnMu.Lock()

	activeCopy :=
		make(
			map[string]activeTurn,
			len(m.activeTurns),
		)

	for threadID, active := range m.activeTurns {
		activeCopy[threadID] =
			active
	}

	m.activeTurnMu.Unlock()

	now := m.quotaRebalanceNow()

	for threadID, active := range activeCopy {
		root :=
			m.rootThreadID(
				threadID,
			)

		if root == "" {
			root = threadID
		}

		if root != threadID {
			continue
		}

		if active.accountID == "" ||
			active.turnID == "" ||
			active.recovering ||
			active.parked ||
			active.agentMessageComplete {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		if m.threadQuotaBucket(
			root,
			active.params,
		) != quotaBucketNormal {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		if !active.lastRebalance.IsZero() &&
			now.Sub(
				active.lastRebalance,
			) < quotaRebalanceCooldown {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		if m.accountQuotaBlockedFor(
			active.accountID,
			quotaBucketNormal,
		) {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		sourceState, sourceKnown :=
			states[active.accountID]

		if !sourceKnown {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		bestID := ""
		bestPressure :=
			quotaPressure{}

		for accountID, state := range states {
			if accountID ==
				active.accountID ||
				!state.usable ||
				m.accountQuotaBlockedFor(
					accountID,
					quotaBucketNormal,
				) {
				continue
			}

			if betterQuotaRebalanceTarget(
				accountID,
				state.pressure,
				bestID,
				bestPressure,
			) {
				bestID =
					accountID
				bestPressure =
					state.pressure
			}
		}

		if bestID == "" ||
			!shouldQuotaRebalance(
				sourceState.pressure,
				bestPressure,
			) {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		m.setPlannedQuotaRebalance(
			root,
			active.accountID,
			active.generation,
			bestID,
		)
	}
}

func (m *Multiplexer) setPlannedQuotaRebalance(
	root string,
	source string,
	generation uint64,
	target string,
) {
	if root == "" ||
		source == "" ||
		target == "" ||
		source == target {
		return
	}

	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok :=
		m.activeTurns[root]

	if !ok ||
		active.accountID != source ||
		active.generation != generation ||
		active.recovering ||
		active.parked ||
		active.agentMessageComplete {
		return
	}

	active.rebalanceTarget =
		target

	m.activeTurns[root] =
		active
}

func (m *Multiplexer) clearPlannedQuotaRebalance(
	root string,
	source string,
	generation uint64,
) {
	if root == "" {
		return
	}

	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok :=
		m.activeTurns[root]

	if !ok ||
		active.accountID != source ||
		active.generation != generation {
		return
	}

	active.rebalanceTarget = ""

	m.activeTurns[root] =
		active
}

func (m *Multiplexer) restorePlannedQuotaRebalance(
	plan quotaRebalancePlan,
) {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok :=
		m.activeTurns[plan.root]

	if !ok ||
		active.accountID != plan.source ||
		active.generation !=
			plan.generation ||
		active.recovering ||
		active.parked ||
		active.agentMessageComplete ||
		active.rebalanceTarget != "" {
		return
	}

	active.rebalanceTarget =
		plan.target

	m.activeTurns[plan.root] =
		active
}

func (m *Multiplexer) scheduleQuotaRebalanceBoundary(
	threadID string,
	accountID string,
) {
	root :=
		m.rootThreadID(
			threadID,
		)

	if root == "" {
		root = threadID
	}

	if root == "" {
		return
	}

	m.activeTurnMu.Lock()

	active, ok :=
		m.activeTurns[root]

	pending :=
		ok &&
			active.rebalanceTarget != "" &&
			(accountID == "" ||
				active.accountID ==
					accountID)

	m.activeTurnMu.Unlock()

	if !pending {
		return
	}

	go func() {
		timer :=
			time.NewTimer(
				quotaRebalanceBoundaryLag,
			)
		defer timer.Stop()

		ctx := m.runCtx

		if ctx == nil {
			<-timer.C
		} else {
			select {
			case <-ctx.Done():
				return

			case <-timer.C:
			}
		}

		m.maybeRunQuotaRebalanceBoundary(
			root,
			accountID,
		)
	}()
}

func (m *Multiplexer) maybeRunQuotaRebalanceBoundary(
	root string,
	accountID string,
) {
	if !m.quotaRebalanceBoundarySafe(
		root,
		accountID,
		0,
	) {
		return
	}

	plan, ok :=
		m.claimQuotaRebalancePlan(
			root,
			accountID,
		)

	if !ok {
		return
	}

	m.executeQuotaRebalance(
		plan,
	)
}

func (m *Multiplexer) claimQuotaRebalancePlan(
	root string,
	accountID string,
) (
	quotaRebalancePlan,
	bool,
) {
	m.activeTurnMu.Lock()
	defer m.activeTurnMu.Unlock()

	active, ok :=
		m.activeTurns[root]

	if !ok ||
		active.rebalanceTarget == "" ||
		active.accountID == "" ||
		(accountID != "" &&
			active.accountID !=
				accountID) ||
		active.recovering ||
		active.parked ||
		active.agentMessageComplete {
		return quotaRebalancePlan{},
			false
	}

	plan :=
		quotaRebalancePlan{
			root:       root,
			source:     active.accountID,
			target:     active.rebalanceTarget,
			generation: active.generation,
		}

	active.rebalanceTarget = ""

	m.activeTurns[root] =
		active

	return plan, true
}

func (m *Multiplexer) quotaRebalanceBoundarySafe(
	root string,
	source string,
	expectedGeneration uint64,
) bool {
	tree :=
		m.treeThreadIDs(
			root,
		)

	if len(tree) == 0 {
		tree = []string{root}
	}

	m.activeTurnMu.Lock()

	active, ok :=
		m.activeTurns[root]

	if !ok ||
		active.accountID == "" ||
		(source != "" &&
			active.accountID != source) ||
		(expectedGeneration != 0 &&
			active.generation !=
				expectedGeneration) ||
		active.turnID == "" ||
		active.recovering ||
		active.parked ||
		active.agentMessageComplete {
		m.activeTurnMu.Unlock()
		return false
	}

	if !active.lastRebalance.IsZero() &&
		m.quotaRebalanceNow().Sub(
			active.lastRebalance,
		) < quotaRebalanceCooldown {
		m.activeTurnMu.Unlock()
		return false
	}

	for _, threadID := range tree {
		if threadID == root {
			continue
		}

		child, childActive :=
			m.activeTurns[threadID]

		if childActive &&
			child.accountID != "" &&
			!child.parked {
			m.activeTurnMu.Unlock()
			return false
		}
	}

	m.activeTurnMu.Unlock()

	m.commandMu.Lock()
	defer m.commandMu.Unlock()

	for _, threadID := range tree {
		if len(
			m.commandPIDs[threadID],
		) > 0 {
			return false
		}
	}

	return true
}

func (m *Multiplexer) quotaRebalanceTreeIdle(
	root string,
) bool {
	tree :=
		m.treeThreadIDs(
			root,
		)

	if len(tree) == 0 {
		tree = []string{root}
	}

	m.activeTurnMu.Lock()

	for _, threadID := range tree {
		if threadID == root {
			continue
		}

		child, exists :=
			m.activeTurns[threadID]

		if exists &&
			child.accountID != "" &&
			!child.parked {
			m.activeTurnMu.Unlock()
			return false
		}
	}

	m.activeTurnMu.Unlock()

	m.commandMu.Lock()
	defer m.commandMu.Unlock()

	for _, threadID := range tree {
		if len(
			m.commandPIDs[threadID],
		) > 0 {
			return false
		}
	}

	return true
}

func (m *Multiplexer) validateQuotaRebalancePlan(
	ctx context.Context,
	plan quotaRebalancePlan,
) bool {
	snapshots :=
		m.accountSnapshots(
			ctx,
			false,
		)

	var sourcePressure quotaPressure
	var targetPressure quotaPressure

	sourceKnown := false
	targetKnown := false
	targetUsable := false

	for _, snapshot := range snapshots {
		pressure, usable, known :=
			quotaPressureForSnapshot(
				snapshot,
			)

		if !known {
			continue
		}

		switch snapshot.ID {
		case plan.source:
			sourcePressure =
				pressure
			sourceKnown = true

		case plan.target:
			targetPressure =
				pressure
			targetKnown = true
			targetUsable =
				usable
		}
	}

	if !sourceKnown ||
		!targetKnown ||
		!targetUsable ||
		m.accountQuotaBlockedFor(
			plan.target,
			quotaBucketNormal,
		) {
		return false
	}

	return shouldQuotaRebalance(
		sourcePressure,
		targetPressure,
	)
}

func (m *Multiplexer) quotaRebalanceExclusions(
	keep string,
) map[string]struct{} {
	excluded :=
		make(
			map[string]struct{},
		)

	for _, account := range m.store.Accounts() {
		if account.ID == keep {
			continue
		}

		excluded[account.ID] =
			struct{}{}
	}

	return excluded
}

func (m *Multiplexer) noteQuotaRebalanceFinished(
	root string,
	source string,
	expectedGeneration uint64,
) {
	m.activeTurnMu.Lock()

	active, ok :=
		m.activeTurns[root]

	if !ok ||
		active.generation !=
			expectedGeneration ||
		active.recovering ||
		active.parked {
		m.activeTurnMu.Unlock()
		return
	}

	target :=
		active.accountID

	active.lastRebalance =
		m.quotaRebalanceNow()

	active.rebalanceTarget = ""

	m.activeTurns[root] =
		active

	m.activeTurnMu.Unlock()

	if target != "" &&
		target != source {
		fmt.Fprintf(
			os.Stderr,
			"codex-mux: proactively rebalanced autonomous thread %s %s -> %s\n",
			root,
			source,
			target,
		)
	}
}

func (m *Multiplexer) executeQuotaRebalance(
	plan quotaRebalancePlan,
) {
	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			2*requestTimeout,
		)

	valid :=
		m.validateQuotaRebalancePlan(
			ctx,
			plan,
		)

	cancel()

	if !valid {
		return
	}

	if !m.quotaRebalanceBoundarySafe(
		plan.root,
		plan.source,
		plan.generation,
	) {
		m.restorePlannedQuotaRebalance(
			plan,
		)
		return
	}

	active, ok :=
		m.beginRecovery(
			plan.root,
			plan.source,
		)

	if !ok {
		return
	}

	if !m.quotaRebalanceTreeIdle(
		plan.root,
	) {
		m.setRecoveryFailed(
			plan.root,
			plan.source,
			active.generation,
		)
		return
	}

	// Unlike hard quota recovery, proactive balancing only reaches this
	// point when the tree has no active child agents and no tracked shell
	// commands. Do not terminate commands here.
	m.markTreeSourceStale(
		plan.root,
		plan.source,
	)

	m.bestEffortInterruptTree(
		plan.root,
		plan.source,
	)

	time.Sleep(
		recoveryFlushDelay,
	)

	targetOnly :=
		m.quotaRebalanceExclusions(
			plan.target,
		)

	err :=
		m.performThreadRecovery(
			plan.root,
			plan.source,
			active,
			targetOnly,
			"proactive quota rebalance",
			false,
		)

	if err != nil &&
		m.recoveryLeaseCurrent(
			plan.root,
			plan.source,
			active.generation,
		) {
		// A proactive handoff must never sacrifice continuity. If the chosen
		// target became unavailable after the quota snapshot, recover again
		// with the normal candidate pool, preferring the original source.
		err =
			m.performThreadRecovery(
				plan.root,
				plan.source,
				active,
				nil,
				"proactive quota rebalance fallback",
				true,
			)
	}

	if err != nil {
		if m.recoveryLeaseCurrent(
			plan.root,
			plan.source,
			active.generation,
		) &&
			m.setRecoveryParked(
				plan.root,
				plan.source,
				"proactive quota rebalance retry",
				nil,
				active.generation,
			) {
			go m.waitForParkedRecovery(
				plan.root,
			)
		}

		return
	}

	m.noteQuotaRebalanceFinished(
		plan.root,
		plan.source,
		active.generation,
	)
}
