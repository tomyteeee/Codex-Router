package mux

import (
	"context"
	"fmt"
	"os"
	"time"
)

const (
	quotaBalanceInterval      = 60 * time.Second
	quotaRebalanceCooldown    = 5 * time.Minute
	quotaRebalanceBoundaryLag = 100 * time.Millisecond
)

type quotaRebalancePlan struct {
	root       string
	source     string
	target     string
	generation uint64
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

func (m *Multiplexer) quotaPacingStates(
	ctx context.Context,
) map[string]quotaPacingState {
	snapshots :=
		m.accountSnapshots(
			ctx,
			false,
		)

	now :=
		m.quotaRebalanceNow()

	states :=
		make(
			map[string]quotaPacingState,
			len(snapshots),
		)

	for _, snapshot := range snapshots {
		if !snapshot.Enabled ||
			!snapshot.Connected ||
			snapshot.AuthType !=
				"chatgpt" ||
			snapshot.RateLimits == nil {
			continue
		}

		state :=
			quotaPacingStateForLimits(
				now,
				snapshot.RateLimits,
				resetCreditMetadata{},
			)

		states[snapshot.ID] =
			state
	}

	return states
}

func (m *Multiplexer) planQuotaRebalances(
	ctx context.Context,
) {
	states :=
		m.quotaPacingStates(
			ctx,
		)

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

	now :=
		m.quotaRebalanceNow()

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

		source, known :=
			states[active.accountID]

		if !known ||
			!source.usable {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		eligible :=
			make(
				map[string]quotaPacingState,
			)

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

			eligible[accountID] =
				state
		}

		targetID, decision :=
			bestQuotaMigrationTarget(
				active.accountID,
				source,
				eligible,
			)

		if targetID == "" ||
			!decision.migrate {
			m.clearPlannedQuotaRebalance(
				root,
				active.accountID,
				active.generation,
			)
			continue
		}

		target :=
			eligible[targetID]

		if m.setPlannedQuotaRebalance(
			root,
			active.accountID,
			active.generation,
			targetID,
		) {
			fmt.Fprintf(
				os.Stderr,
				"codex-mux: quota rebalance planned thread=%s source=%s target=%s reason=%s advantage=%.3f source={%s} target={%s}\n",
				root,
				active.accountID,
				targetID,
				decision.reason,
				decision.advantage,
				quotaPacingSummary(
					source,
				),
				quotaPacingSummary(
					target,
				),
			)
		}
	}
}

func (m *Multiplexer) setPlannedQuotaRebalance(
	root string,
	source string,
	generation uint64,
	target string,
) bool {
	if root == "" ||
		source == "" ||
		target == "" ||
		source == target {
		return false
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
		return false
	}

	if active.rebalanceTarget ==
		target {
		return false
	}

	active.rebalanceTarget =
		target

	m.activeTurns[root] =
		active

	return true
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
) (
	quotaMigrationDecision,
	quotaPacingState,
	quotaPacingState,
	bool,
) {
	states :=
		m.quotaPacingStates(
			ctx,
		)

	source, sourceKnown :=
		states[plan.source]

	target, targetKnown :=
		states[plan.target]

	if !sourceKnown ||
		!targetKnown ||
		!target.usable ||
		m.accountQuotaBlockedFor(
			plan.target,
			quotaBucketNormal,
		) {
		return quotaMigrationDecision{},
			source,
			target,
			false
	}

	decision :=
		quotaRebalanceDecision(
			source,
			target,
		)

	return decision,
		source,
		target,
		decision.migrate
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

func shouldRetryQuotaRebalanceWithNormalPool(
	err error,
) bool {
	return err != nil
}

func (m *Multiplexer) noteQuotaRebalanceFinished(
	root string,
	source string,
	expectedGeneration uint64,
	reason string,
	advantage float64,
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
			"codex-mux: proactively rebalanced autonomous thread %s %s -> %s reason=%s advantage=%.3f\n",
			root,
			source,
			target,
			reason,
			advantage,
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

	decision,
		sourceState,
		targetState,
		valid :=
		m.validateQuotaRebalancePlan(
			ctx,
			plan,
		)

	cancel()

	if !valid {
		fmt.Fprintf(
			os.Stderr,
			"codex-mux: quota rebalance suppressed thread=%s source=%s target=%s reason=STALE_OR_NO_LONGER_BETTER source={%s} target={%s}\n",
			plan.root,
			plan.source,
			plan.target,
			quotaPacingSummary(
				sourceState,
			),
			quotaPacingSummary(
				targetState,
			),
		)
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
			"proactive reset-aware quota rebalance",
			false,
		)

	if shouldRetryQuotaRebalanceWithNormalPool(
		err,
	) &&
		m.recoveryLeaseCurrent(
			plan.root,
			plan.source,
			active.generation,
		) {
		err =
			m.performThreadRecovery(
				plan.root,
				plan.source,
				active,
				nil,
				"proactive reset-aware quota rebalance fallback",
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
				"proactive reset-aware quota rebalance retry",
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
		decision.reason,
		decision.advantage,
	)
}
