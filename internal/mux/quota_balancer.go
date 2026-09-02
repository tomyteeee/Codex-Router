package mux

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	// Correctness is event-driven. This is only a liveness watchdog if
	// an upstream lifecycle/quota notification is ever lost.
	quotaBalanceFallbackInterval = 5 * time.Minute

	// Hysteresis, not boundary detection.
	quotaRebalanceCooldown = 5 * time.Minute
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

func (m *Multiplexer) requestQuotaBalance() {
	if m.quotaBalanceWake == nil {
		return
	}

	select {
	case m.quotaBalanceWake <- struct{}{}:
	default:
		// Coalesce bursts. The queued pass observes newest state.
	}
}

func (m *Multiplexer) runQuotaBalancePass(
	ctx context.Context,
) {
	passCtx, cancel :=
		context.WithTimeout(
			ctx,
			2*requestTimeout,
		)
	defer cancel()

	m.planQuotaRebalances(
		passCtx,
	)
}

func (m *Multiplexer) quotaBalanceLoop(
	ctx context.Context,
) {
	ticker :=
		time.NewTicker(
			quotaBalanceFallbackInterval,
		)
	defer ticker.Stop()

	m.requestQuotaBalance()

	for {
		select {
		case <-ctx.Done():
			return

		case <-m.quotaBalanceWake:
			m.runQuotaBalancePass(
				ctx,
			)

		case <-ticker.C:
			// Watchdog only.
			m.runQuotaBalancePass(
				ctx,
			)
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

	active, ok :=
		m.activeTurns[root]

	if !ok ||
		active.accountID != source ||
		active.generation != generation ||
		active.recovering ||
		active.parked ||
		active.agentMessageComplete {
		m.activeTurnMu.Unlock()
		return false
	}

	changed :=
		active.rebalanceTarget !=
			target

	active.rebalanceTarget =
		target

	boundaryPending :=
		active.rebalanceBoundaryPending

	m.activeTurns[root] =
		active

	m.activeTurnMu.Unlock()

	// quota calculation and execution events race in either direction.
	// If item/completed won first and no new item started, consume that
	// still-open boundary now.
	if boundaryPending {
		m.maybeRunQuotaRebalanceBoundary(
			root,
			source,
		)
	}

	return changed
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

func (m *Multiplexer) quotaRebalanceRoot(
	threadID string,
) string {
	root :=
		m.rootThreadID(
			threadID,
		)

	if root == "" {
		root = threadID
	}

	return root
}

func (m *Multiplexer) markQuotaRebalanceBoundary(
	threadID string,
	accountID string,
) string {
	root :=
		m.quotaRebalanceRoot(
			threadID,
		)

	// A descendant's item completion is not itself a root autonomous
	// boundary. The root lifecycle must expose the boundary.
	if root == "" ||
		root != threadID {
		return ""
	}

	m.activeTurnMu.Lock()

	active, ok :=
		m.activeTurns[root]

	if ok &&
		active.turnID != "" &&
		(accountID == "" ||
			active.accountID ==
				accountID) &&
		!active.recovering &&
		!active.parked &&
		!active.agentMessageComplete {
		active.rebalanceBoundaryPending =
			true

		m.activeTurns[root] =
			active
	}

	m.activeTurnMu.Unlock()

	return root
}

func (m *Multiplexer) clearQuotaRebalanceBoundary(
	threadID string,
	accountID string,
) {
	root :=
		m.quotaRebalanceRoot(
			threadID,
		)

	if root == "" {
		return
	}

	// Activity anywhere in the execution tree closes the old root boundary.
	m.activeTurnMu.Lock()

	active, ok :=
		m.activeTurns[root]

	if ok &&
		(accountID == "" ||
			active.accountID ==
				accountID) {
		active.rebalanceBoundaryPending =
			false

		m.activeTurns[root] =
			active
	}

	m.activeTurnMu.Unlock()
}

func (m *Multiplexer) scheduleQuotaRebalanceBoundary(
	threadID string,
	accountID string,
) {
	// Every completion is useful pacing information, including descendants.
	m.requestQuotaBalance()

	root :=
		m.markQuotaRebalanceBoundary(
			threadID,
			accountID,
		)

	if root == "" {
		return
	}

	// item/completed is itself the event boundary.
	// No 100 ms sleep and no wall-clock sampling.
	m.maybeRunQuotaRebalanceBoundary(
		root,
		accountID,
	)
}

func (m *Multiplexer) maybeRunQuotaRebalanceBoundary(
	root string,
	accountID string,
) {
	plan,
		active,
		ok :=
		m.claimQuotaRebalanceBoundary(
			root,
			accountID,
		)

	if !ok {
		return
	}

	// Safe boundary ownership has already been converted into the normal
	// generation/recovery lease. Slow recovery work can now be asynchronous.
	go m.executeQuotaRebalance(
		plan,
		active,
	)
}

func (m *Multiplexer) claimQuotaRebalanceBoundary(
	root string,
	accountID string,
) (
	quotaRebalancePlan,
	activeTurn,
	bool,
) {
	if !m.quotaRebalanceBoundarySafe(
		root,
		accountID,
		0,
	) {
		return quotaRebalancePlan{},
			activeTurn{},
			false
	}

	m.activeTurnMu.Lock()

	active, ok :=
		m.activeTurns[root]

	if !ok ||
		active.accountID == "" ||
		active.turnID == "" ||
		active.rebalanceTarget == "" ||
		!active.rebalanceBoundaryPending ||
		(accountID != "" &&
			active.accountID !=
				accountID) ||
		active.recovering ||
		active.parked ||
		active.agentMessageComplete {
		m.activeTurnMu.Unlock()

		return quotaRebalancePlan{},
			activeTurn{},
			false
	}

	if !active.lastRebalance.IsZero() &&
		m.quotaRebalanceNow().Sub(
			active.lastRebalance,
		) < quotaRebalanceCooldown {
		m.activeTurnMu.Unlock()

		return quotaRebalancePlan{},
			activeTurn{},
			false
	}

	target :=
		active.rebalanceTarget

	// Critical transition:
	//
	// item/completed boundary
	//        ->
	// generation/recovery lease
	//
	// There is no arbitrary delay between these concepts.
	active.rebalanceTarget = ""
	active.rebalanceBoundaryPending = false
	active.recovering = true
	active.parked = false
	active.generation++
	active.lastActivity =
		m.quotaRebalanceNow()

	m.activeTurns[root] =
		active

	plan :=
		quotaRebalancePlan{
			root:       root,
			source:     active.accountID,
			target:     target,
			generation: active.generation,
		}

	snapshot :=
		active

	snapshot.params =
		append(
			json.RawMessage(nil),
			active.params...,
		)

	snapshot.excluded =
		cloneAccountSet(
			active.excluded,
		)

	snapshot.failureRaw =
		append(
			[]byte(nil),
			active.failureRaw...,
		)

	m.activeTurnMu.Unlock()

	return plan,
		snapshot,
		true
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
	active activeTurn,
) {
	if !m.recoveryLeaseCurrent(
		plan.root,
		plan.source,
		active.generation,
	) {
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

	// If work raced immediately after item/completed, the already-acquired
	// boundary lease wins. Do not let that raced command pin the source.
	m.terminateTrackedCommands(
		plan.root,
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
			"proactive event-driven quota rebalance",
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
				"proactive event-driven quota rebalance fallback",
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
				"proactive event-driven quota rebalance retry",
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
		"event-driven pacing",
		0,
	)

	m.requestQuotaBalance()
}
