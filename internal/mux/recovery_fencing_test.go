package mux

import (
	"testing"
	"time"
)

func newRecoveryFencingMux() *Multiplexer {
	return &Multiplexer{
		activeTurns:   make(map[string]activeTurn),
		threadParents: make(map[string]string),
		now:           time.Now,
	}
}

func TestBeginRecoveryRequiresTrackedRoot(
	t *testing.T,
) {
	m := newRecoveryFencingMux()

	if _, ok := m.beginRecovery(
		"root-1",
		"account-1",
	); ok {
		t.Fatal(
			"recovery manufactured an untracked root turn",
		)
	}
}

func TestRecoveryGenerationLease(
	t *testing.T,
) {
	m := newRecoveryFencingMux()

	m.activeTurns["root-1"] =
		activeTurn{
			accountID:  "account-1",
			generation: 10,
		}

	active, ok := m.beginRecovery(
		"root-1",
		"account-1",
	)

	if !ok {
		t.Fatal(
			"tracked root recovery was not claimed",
		)
	}

	if !m.recoveryLeaseCurrent(
		"root-1",
		"account-1",
		active.generation,
	) {
		t.Fatal(
			"fresh recovery generation was not current",
		)
	}

	m.supersedeRecoveryForUserTurn(
		"root-1",
	)

	if m.recoveryLeaseCurrent(
		"root-1",
		"account-1",
		active.generation,
	) {
		t.Fatal(
			"superseded recovery generation remained valid",
		)
	}
}

func TestSupersedeRecoveryPreservesTrackedTask(
	t *testing.T,
) {
	m := newRecoveryFencingMux()

	m.activeTurns["root-1"] =
		activeTurn{
			accountID:     "account-1",
			generation:    4,
			recovering:    true,
			recoveryCause: "quota",
			failureRaw:    []byte("failure"),
		}

	m.supersedeRecoveryForUserTurn(
		"root-1",
	)

	active :=
		m.activeTurns["root-1"]

	if active.generation != 5 {
		t.Fatalf(
			"generation=%d want 5",
			active.generation,
		)
	}

	if active.recovering ||
		active.parked {
		t.Fatal(
			"user turn did not cancel automatic recovery state",
		)
	}

	if active.accountID != "account-1" {
		t.Fatalf(
			"tracked account changed unexpectedly: %q",
			active.accountID,
		)
	}
}

func TestSetRecoverySucceededRejectsStaleLease(
	t *testing.T,
) {
	m := newRecoveryFencingMux()

	m.activeTurns["root-1"] =
		activeTurn{
			accountID:  "account-1",
			generation: 8,
			recovering: true,
		}

	ok, err := m.setRecoverySucceeded(
		"root-1",
		"account-1",
		"account-2",
		"turn-2",
		nil,
		nil,
		7,
	)

	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatal(
			"stale recovery generation committed success",
		)
	}

	if got :=
		m.activeTurns["root-1"].accountID; got != "account-1" {
		t.Fatalf(
			"stale recovery changed account to %q",
			got,
		)
	}
}

func TestStaleRecoveryCannotParkNewUserGeneration(
	t *testing.T,
) {
	m := newRecoveryFencingMux()

	m.activeTurns["root-1"] =
		activeTurn{
			accountID:  "account-1",
			generation: 10,
			recovering: true,
		}

	if m.setRecoveryParked(
		"root-1",
		"account-1",
		"old recovery",
		nil,
		9,
	) {
		t.Fatal(
			"stale recovery parked a newer generation",
		)
	}

	active :=
		m.activeTurns["root-1"]

	if active.parked {
		t.Fatal(
			"newer generation was incorrectly parked",
		)
	}
}

func TestStaleRecoveryCannotFailNewUserGeneration(
	t *testing.T,
) {
	m := newRecoveryFencingMux()

	m.activeTurns["root-1"] =
		activeTurn{
			accountID:  "account-1",
			generation: 10,
			recovering: true,
		}

	if m.setRecoveryFailed(
		"root-1",
		"account-1",
		9,
	) {
		t.Fatal(
			"stale recovery mutated newer generation",
		)
	}
}

func TestClaimParkedRecoveryAdvancesGeneration(
	t *testing.T,
) {
	m := newRecoveryFencingMux()

	m.activeTurns["root-1"] =
		activeTurn{
			accountID:  "account-1",
			generation: 4,
			parked:     true,
		}

	active, ok :=
		m.claimParkedRecovery(
			"root-1",
		)

	if !ok {
		t.Fatal(
			"parked recovery was not claimed",
		)
	}

	if active.generation != 5 {
		t.Fatalf(
			"generation=%d want 5",
			active.generation,
		)
	}

	if !active.recovering ||
		active.parked {
		t.Fatal(
			"parked retry did not become an active recovery lease",
		)
	}
}
