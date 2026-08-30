package mux

import (
	"testing"
	"time"
)

func quotaBalancerTestLimits(
	shortUsed float64,
	weeklyUsed float64,
) *RateLimits {
	shortMinutes := int64(300)
	weeklyMinutes := int64(10_080)

	return &RateLimits{
		Primary: &RateLimitWindow{
			UsedPercent:        shortUsed,
			WindowDurationMins: &shortMinutes,
		},
		Secondary: &RateLimitWindow{
			UsedPercent:        weeklyUsed,
			WindowDurationMins: &weeklyMinutes,
		},
	}
}

func TestQuotaPressureUsesMostConstrainedWindow(
	t *testing.T,
) {
	pressure, usable, known :=
		quotaPressureForSnapshot(
			AccountSnapshot{
				ID:        "account-1",
				Enabled:   true,
				Connected: true,
				AuthType:  "chatgpt",
				RateLimits: quotaBalancerTestLimits(
					32,
					71,
				),
			},
		)

	if !known || !usable {
		t.Fatal(
			"healthy quota snapshot was not eligible",
		)
	}

	if pressure.short != 32 ||
		pressure.weekly != 71 ||
		pressure.combined != 71 {
		t.Fatalf(
			"unexpected pressure: %#v",
			pressure,
		)
	}
}

func TestQuotaRebalanceUsesFifteenPointHysteresis(
	t *testing.T,
) {
	if shouldQuotaRebalance(
		quotaPressure{
			combined: 54,
			short:    54,
			weekly:   40,
		},
		quotaPressure{
			combined: 42,
			short:    42,
			weekly:   35,
		},
	) {
		t.Fatal(
			"12 point difference should remain sticky",
		)
	}

	if !shouldQuotaRebalance(
		quotaPressure{
			combined: 58,
			short:    58,
			weekly:   40,
		},
		quotaPressure{
			combined: 42,
			short:    42,
			weekly:   35,
		},
	) {
		t.Fatal(
			"16 point difference should rebalance",
		)
	}
}

func TestQuotaRebalanceProtectsNearlyExhaustedShortWindow(
	t *testing.T,
) {
	if !shouldQuotaRebalance(
		quotaPressure{
			combined: 86,
			short:    86,
			weekly:   70,
		},
		quotaPressure{
			combined: 78,
			short:    70,
			weekly:   78,
		},
	) {
		t.Fatal(
			"near-exhausted short window was not protected",
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
			"active child agent was treated as a safe rebalance boundary",
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
			"running command was treated as a safe rebalance boundary",
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
			"idle root was not treated as a safe rebalance boundary",
		)
	}
}
