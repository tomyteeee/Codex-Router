package mux

import "testing"

func TestActiveExecutionAccountUsesRuntimeOwner(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID: "runtime-account",
			},
		},
		threadParents: make(map[string]string),
	}

	accountID, ok :=
		m.activeExecutionAccount(
			"root-1",
		)

	if !ok {
		t.Fatal(
			"active execution account was not found",
		)
	}

	if accountID != "runtime-account" {
		t.Fatalf(
			"account=%q want runtime-account",
			accountID,
		)
	}
}

func TestActiveExecutionAccountResolvesChildToRoot(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID: "runtime-account",
			},
		},
		threadParents: map[string]string{
			"child-1": "root-1",
		},
	}

	accountID, ok :=
		m.activeExecutionAccount(
			"child-1",
		)

	if !ok {
		t.Fatal(
			"child did not resolve to active root execution",
		)
	}

	if accountID != "runtime-account" {
		t.Fatalf(
			"account=%q want runtime-account",
			accountID,
		)
	}
}

func TestActiveExecutionAccountRejectsRecoveringTurn(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID:  "runtime-account",
				recovering: true,
			},
		},
		threadParents: make(map[string]string),
	}

	if accountID, ok :=
		m.activeExecutionAccount(
			"root-1",
		); ok {
		t.Fatalf(
			"recovering turn exposed account %q as steerable",
			accountID,
		)
	}
}

func TestActiveExecutionAccountRejectsParkedTurn(
	t *testing.T,
) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"root-1": {
				accountID: "runtime-account",
				parked:    true,
			},
		},
		threadParents: make(map[string]string),
	}

	if accountID, ok :=
		m.activeExecutionAccount(
			"root-1",
		); ok {
		t.Fatalf(
			"parked turn exposed account %q as steerable",
			accountID,
		)
	}
}
