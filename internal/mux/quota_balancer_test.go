package mux

import (
	"errors"
	"testing"
	"time"
)

func pacingWindow(
	used float64,
	duration time.Duration,
	resetAfter time.Duration,
	now time.Time,
) *RateLimitWindow {
	minutes :=
		int64(
			duration /
				time.Minute,
		)

	var reset *int64

	if resetAfter != 0 {
		value :=
			now.Add(
				resetAfter,
			).Unix()

		reset = &value
	}

	return &RateLimitWindow{
		UsedPercent:        used,
		WindowDurationMins: &minutes,
		ResetsAt:           reset,
	}
}

func pacingLimits(
	now time.Time,
	shortUsed float64,
	shortReset time.Duration,
	weeklyUsed float64,
	weeklyReset time.Duration,
) *RateLimits {
	return &RateLimits{
		Primary: pacingWindow(
			shortUsed,
			5*time.Hour,
			shortReset,
			now,
		),
		Secondary: pacingWindow(
			weeklyUsed,
			7*24*time.Hour,
			weeklyReset,
			now,
		),
	}
}

func pacingState(
	now time.Time,
	shortUsed float64,
	shortReset time.Duration,
	weeklyUsed float64,
	weeklyReset time.Duration,
) quotaPacingState {
	return quotaPacingStateForLimits(
		now,
		pacingLimits(
			now,
			shortUsed,
			shortReset,
			weeklyUsed,
			weeklyReset,
		),
		resetCreditMetadata{},
	)
}

func TestResetAwareImminentResetBeatsRawPercentage(
	t *testing.T,
) {
	now :=
		time.Date(
			2026,
			time.August,
			30,
			12,
			0,
			0,
			0,
			time.UTC,
		)

	source :=
		pacingState(
			now,
			85,
			4*time.Minute,
			30,
			3*24*time.Hour,
		)

	target :=
		pacingState(
			now,
			20,
			4*time.Hour,
			30,
			3*24*time.Hour,
		)

	decision :=
		quotaRebalanceDecision(
			source,
			target,
		)

	if decision.migrate {
		t.Fatalf(
			"imminently resetting 85%% source was abandoned: %#v",
			decision,
		)
	}
}

func TestResetAwareDistantHighPressureAccountIsProtected(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			85,
			4*time.Hour,
			30,
			3*24*time.Hour,
		)

	target :=
		pacingState(
			now,
			20,
			4*time.Hour,
			30,
			3*24*time.Hour,
		)

	decision :=
		quotaRebalanceDecision(
			source,
			target,
		)

	if !decision.migrate {
		t.Fatal(
			"distant high-pressure source should migrate toward healthier capacity",
		)
	}
}

func TestResetAwareUnusedCapacityAboutToExpireIsValuable(
	t *testing.T,
) {
	now := time.Now()

	imminent :=
		pacingState(
			now,
			30,
			5*time.Minute,
			30,
			3*24*time.Hour,
		)

	distant :=
		pacingState(
			now,
			30,
			4*time.Hour,
			30,
			3*24*time.Hour,
		)

	if imminent.desirability <=
		distant.desirability {
		t.Fatalf(
			"imminent unused capacity should be more desirable: imminent=%f distant=%f",
			imminent.desirability,
			distant.desirability,
		)
	}
}

func TestResetAwareWeeklyConstraintOverridesShortGreed(
	t *testing.T,
) {
	now := time.Now()

	constrained :=
		pacingState(
			now,
			30,
			5*time.Minute,
			95,
			3*24*time.Hour,
		)

	healthy :=
		pacingState(
			now,
			45,
			2*time.Hour,
			30,
			4*24*time.Hour,
		)

	if constrained.desirability >=
		healthy.desirability {
		t.Fatalf(
			"weekly-constrained account won short-window greed: constrained=%f healthy=%f",
			constrained.desirability,
			healthy.desirability,
		)
	}
}

func TestResetAwareShortWindowDepletionProtection(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			96,
			4*time.Hour,
			30,
			3*24*time.Hour,
		)

	target :=
		pacingState(
			now,
			30,
			4*time.Hour,
			30,
			3*24*time.Hour,
		)

	decision :=
		quotaRebalanceDecision(
			source,
			target,
		)

	if !decision.migrate {
		t.Fatal(
			"near-depletion source should migrate",
		)
	}

	if decision.reason !=
		quotaDecisionDepletion {
		t.Fatalf(
			"reason=%q want %q",
			decision.reason,
			quotaDecisionDepletion,
		)
	}
}

func TestResetAwareCriticalShortReserveOverridesExpiryOpportunity(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			95,
			4*time.Minute,
			30,
			4*time.Minute,
		)

	target :=
		pacingState(
			now,
			30,
			4*time.Hour,
			30,
			3*24*time.Hour,
		)

	decision :=
		quotaRebalanceDecision(
			source,
			target,
		)

	if !decision.migrate {
		t.Fatalf(
			"critical short-window reserve was overridden by expiry opportunity: source=%#v target=%#v decision=%#v",
			source,
			target,
			decision,
		)
	}

	if decision.reason !=
		quotaDecisionDepletion {
		t.Fatalf(
			"reason=%q want %q",
			decision.reason,
			quotaDecisionDepletion,
		)
	}
}

func TestResetAwareCriticalWeeklyReserveOverridesShortExpiry(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			30,
			5*time.Minute,
			94,
			2*time.Hour,
		)

	target :=
		pacingState(
			now,
			45,
			2*time.Hour,
			30,
			4*24*time.Hour,
		)

	decision :=
		quotaRebalanceDecision(
			source,
			target,
		)

	if !decision.migrate {
		t.Fatalf(
			"critical weekly reserve was overridden by short-window opportunity: source=%#v target=%#v decision=%#v",
			source,
			target,
			decision,
		)
	}

	if decision.reason !=
		quotaDecisionDepletion {
		t.Fatalf(
			"reason=%q want %q",
			decision.reason,
			quotaDecisionDepletion,
		)
	}
}

func TestQuotaPacingSelectionRejectsCriticalCandidate(
	t *testing.T,
) {
	now := time.Now()

	critical :=
		pacingState(
			now,
			95,
			4*time.Minute,
			30,
			4*time.Minute,
		)

	healthyUnknownReset :=
		pacingState(
			now,
			35,
			0,
			30,
			0,
		)

	if compareQuotaPacingSelection(
		critical,
		healthyUnknownReset,
	) >= 0 {
		t.Fatalf(
			"critical candidate outranked healthy candidate: critical=%#v healthy=%#v",
			critical,
			healthyUnknownReset,
		)
	}

	if compareQuotaPacingSelection(
		healthyUnknownReset,
		critical,
	) <= 0 {
		t.Fatal(
			"selection comparator was not antisymmetric for critical candidate",
		)
	}
}

func TestQuotaPacingSelectionMixedResetKnowledgeIsTransitive(
	t *testing.T,
) {
	now := time.Now()

	states :=
		[]quotaPacingState{
			pacingState(
				now,
				30,
				5*time.Minute,
				30,
				3*24*time.Hour,
			),
			pacingState(
				now,
				45,
				0,
				35,
				0,
			),
			pacingState(
				now,
				70,
				4*time.Hour,
				50,
				5*24*time.Hour,
			),
			pacingState(
				now,
				95,
				4*time.Minute,
				30,
				3*24*time.Hour,
			),
		}

	for i := range states {
		for j := range states {
			leftRight :=
				compareQuotaPacingSelection(
					states[i],
					states[j],
				)

			rightLeft :=
				compareQuotaPacingSelection(
					states[j],
					states[i],
				)

			if leftRight !=
				-rightLeft {
				t.Fatalf(
					"comparator is not antisymmetric: i=%d j=%d ij=%d ji=%d",
					i,
					j,
					leftRight,
					rightLeft,
				)
			}

			for k := range states {
				if compareQuotaPacingSelection(
					states[i],
					states[j],
				) > 0 &&
					compareQuotaPacingSelection(
						states[j],
						states[k],
					) > 0 &&
					compareQuotaPacingSelection(
						states[i],
						states[k],
					) <= 0 {
					t.Fatalf(
						"comparator is not transitive: %d > %d > %d",
						i,
						j,
						k,
					)
				}
			}
		}
	}
}

func TestResetAwareUnknownResetFallsBackDeterministically(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			70,
			0,
			70,
			0,
		)

	target :=
		pacingState(
			now,
			30,
			0,
			30,
			0,
		)

	if source.resetAware ||
		target.resetAware {
		t.Fatal(
			"unknown reset unexpectedly became reset-aware",
		)
	}

	decision :=
		quotaRebalanceDecision(
			source,
			target,
		)

	if !decision.migrate ||
		decision.reason !=
			quotaDecisionLoad {
		t.Fatalf(
			"unknown-reset fallback decision=%#v",
			decision,
		)
	}
}

func TestResetAwarePastResetIsStaleNotUrgent(
	t *testing.T,
) {
	now := time.Now()
	minutes := int64(300)
	past :=
		now.Add(
			-time.Minute,
		).Unix()

	window :=
		quotaWindowPacingFor(
			now,
			&RateLimitWindow{
				UsedPercent:        20,
				WindowDurationMins: &minutes,
				ResetsAt:           &past,
			},
			5*time.Hour,
			10,
			30*time.Minute,
		)

	if window.resetKnown {
		t.Fatal(
			"past reset was treated as current",
		)
	}

	if !window.staleReset {
		t.Fatal(
			"past reset was not marked stale",
		)
	}

	if window.opportunity != 0 {
		t.Fatalf(
			"stale reset got expiry opportunity %f",
			window.opportunity,
		)
	}
}

func TestResetAwareStalePlanRevalidationChangesDecision(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			80,
			4*time.Hour,
			40,
			3*24*time.Hour,
		)

	oldTarget :=
		pacingState(
			now,
			20,
			4*time.Hour,
			30,
			4*24*time.Hour,
		)

	if !quotaRebalanceDecision(
		source,
		oldTarget,
	).migrate {
		t.Fatal(
			"initial plan was not attractive",
		)
	}

	newTarget :=
		pacingState(
			now,
			99,
			4*time.Hour,
			99,
			4*24*time.Hour,
		)

	if quotaRebalanceDecision(
		source,
		newTarget,
	).migrate {
		t.Fatal(
			"stale target remained eligible after becoming exhausted",
		)
	}
}

func TestResetAwareHysteresisSuppressesSmallDifference(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			50,
			0,
			50,
			0,
		)

	target :=
		pacingState(
			now,
			42,
			0,
			42,
			0,
		)

	if quotaRebalanceDecision(
		source,
		target,
	).migrate {
		t.Fatal(
			"small fallback fluctuation caused migration",
		)
	}
}

func TestQuotaRebalanceBoundaryRejectsActiveChild(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID:  "account-1",
				turnID:     "turn-root",
				generation: 4,
			},
			"child-1": {
				accountID: "account-1",
				turnID:    "turn-child",
			},
		},
		threadParents: map[string]string{
			"child-1": "root-1",
		},
		commandPIDs: make(
			map[string]map[int]string,
		),
		now: time.Now,
	}

	if m.quotaRebalanceBoundarySafe(
		"root-1",
		"account-1",
		4,
	) {
		t.Fatal(
			"active child was treated as a safe rebalance boundary",
		)
	}
}

func TestQuotaRebalanceBoundaryRejectsRunningCommand(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID:  "account-1",
				turnID:     "turn-root",
				generation: 4,
			},
		},
		threadParents: make(
			map[string]string,
		),
		commandPIDs: map[string]map[int]string{
			"root-1": {
				1234: "process",
			},
		},
		now: time.Now,
	}

	if m.quotaRebalanceBoundarySafe(
		"root-1",
		"account-1",
		4,
	) {
		t.Fatal(
			"running command was treated as safe",
		)
	}
}

func TestQuotaRebalanceBoundaryRejectsStaleGeneration(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID:  "account-1",
				turnID:     "turn-root",
				generation: 8,
			},
		},
		threadParents: make(
			map[string]string,
		),
		commandPIDs: make(
			map[string]map[int]string,
		),
		now: time.Now,
	}

	if m.quotaRebalanceBoundarySafe(
		"root-1",
		"account-1",
		7,
	) {
		t.Fatal(
			"stale generation was allowed to migrate",
		)
	}
}

func TestQuotaRebalanceTargetFailureRetainsNormalPoolFallback(
	t *testing.T,
) {
	if !shouldRetryQuotaRebalanceWithNormalPool(
		errors.New(
			"target unavailable",
		),
	) {
		t.Fatal(
			"proactive target failure did not retain normal-pool fallback",
		)
	}

	if shouldRetryQuotaRebalanceWithNormalPool(
		nil,
	) {
		t.Fatal(
			"successful target incorrectly requested fallback",
		)
	}
}

func TestResetAwareThreeAccountScenario(
	t *testing.T,
) {
	now := time.Now()

	source :=
		pacingState(
			now,
			60,
			3*time.Hour,
			45,
			3*24*time.Hour,
		)

	candidates :=
		map[string]quotaPacingState{
			"A": pacingState(
				now,
				88,
				8*time.Minute,
				45,
				3*24*time.Hour,
			),
			"B": pacingState(
				now,
				35,
				2*time.Hour,
				30,
				4*24*time.Hour,
			),
			"C": pacingState(
				now,
				10,
				4*time.Hour,
				65,
				2*24*time.Hour,
			),
		}

	target, decision :=
		bestQuotaMigrationTarget(
			"source",
			source,
			candidates,
		)

	if target == "" ||
		!decision.migrate {
		t.Fatalf(
			"three-account scenario produced no useful migration: target=%q decision=%#v",
			target,
			decision,
		)
	}

	if target == "C" {
		t.Fatal(
			"weekly-constrained account C should not win merely because its short window is empty",
		)
	}
}

func TestQuotaRebalanceBoundaryAllowsIdleRoot(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID:  "account-1",
				turnID:     "turn-root",
				generation: 4,
			},
		},
		threadParents: make(
			map[string]string,
		),
		commandPIDs: make(
			map[string]map[int]string,
		),
		now: time.Now,
	}

	if !m.quotaRebalanceBoundarySafe(
		"root-1",
		"account-1",
		4,
	) {
		t.Fatal(
			"idle root was not a safe rebalance boundary",
		)
	}
}

func TestQuotaRebalanceEventBoundaryAcquiresRecoveryLease(
	t *testing.T,
) {
	m :=
		&Multiplexer{
			activeTurns: map[string]activeTurn{
				"root-1": {
					accountID:                "account-1",
					turnID:                   "turn-root",
					generation:               4,
					rebalanceTarget:          "account-2",
					rebalanceBoundaryPending: true,
				},
			},
			threadParents: make(
				map[string]string,
			),
			commandPIDs: make(
				map[string]map[int]string,
			),
			now: time.Now,
		}

	plan, active, ok :=
		m.claimQuotaRebalanceBoundary(
			"root-1",
			"account-1",
		)

	if !ok {
		t.Fatal(
			"safe event boundary was not claimed",
		)
	}

	if plan.target != "account-2" {
		t.Fatalf(
			"target=%q want account-2",
			plan.target,
		)
	}

	if plan.generation != 5 ||
		active.generation != 5 {
		t.Fatalf(
			"generation plan=%d active=%d want 5",
			plan.generation,
			active.generation,
		)
	}

	current :=
		m.activeTurns["root-1"]

	if !current.recovering {
		t.Fatal(
			"boundary claim did not acquire recovery lease",
		)
	}

	if current.rebalanceBoundaryPending {
		t.Fatal(
			"claimed boundary remained pending",
		)
	}

	if current.rebalanceTarget != "" {
		t.Fatal(
			"claimed target remained pending",
		)
	}
}

func TestQuotaRebalanceEventBoundaryRejectsClosedBoundary(
	t *testing.T,
) {
	m :=
		&Multiplexer{
			activeTurns: map[string]activeTurn{
				"root-1": {
					accountID:                "account-1",
					turnID:                   "turn-root",
					generation:               4,
					rebalanceTarget:          "account-2",
					rebalanceBoundaryPending: false,
				},
			},
			threadParents: make(
				map[string]string,
			),
			commandPIDs: make(
				map[string]map[int]string,
			),
			now: time.Now,
		}

	_, _, ok :=
		m.claimQuotaRebalanceBoundary(
			"root-1",
			"account-1",
		)

	if ok {
		t.Fatal(
			"closed lifecycle boundary was claimed",
		)
	}
}

func TestDescendantActivityClosesRootRebalanceBoundary(
	t *testing.T,
) {
	m :=
		&Multiplexer{
			activeTurns: map[string]activeTurn{
				"root-1": {
					accountID:                "account-1",
					turnID:                   "turn-root",
					rebalanceBoundaryPending: true,
				},
			},
			threadParents: map[string]string{
				"child-1": "root-1",
			},
		}

	m.clearQuotaRebalanceBoundary(
		"child-1",
		"account-1",
	)

	if m.activeTurns["root-1"].
		rebalanceBoundaryPending {
		t.Fatal(
			"descendant activity did not close root boundary",
		)
	}
}
