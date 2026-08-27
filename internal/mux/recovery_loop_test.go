package mux

import "testing"

func TestUsageLimitTextRequiresStrongEvidence(t *testing.T) {
	positive := []string{
		"usageLimitExceeded",
		"usage_limit_exceeded",
		"usage limit exceeded",
		"usage limit reached",
		"You've hit your usage limit",
		"rate_limit_exceeded",
		"rate limit reached",
		"quota exceeded",
		"insufficient quota",
		`{"type":"usage_limit"}`,
	}

	for _, text := range positive {
		if !usageLimitText(text) {
			t.Fatalf(
				"expected usage-limit classification for %q",
				text,
			)
		}
	}

	negative := []string{
		"quota routing logic failed to compile",
		"show quota data in the menu",
		"rate limit metadata changed",
		"usage limit handling code needs work",
		"the router should preserve quota state",
		"this task discusses rate limits",
	}

	for _, text := range negative {
		if usageLimitText(text) {
			t.Fatalf(
				"generic quota language must not trigger failover: %q",
				text,
			)
		}
	}
}

func TestItemNotificationHasAgentMessage(t *testing.T) {
	if !itemNotificationHasAgentMessage([]byte(`{
		"threadId":"thread-1",
		"item":{
			"type":"agentMessage",
			"text":"Owner action is required before I can continue."
		}
	}`)) {
		t.Fatal(
			"expected completed agentMessage to be detected",
		)
	}

	if itemNotificationHasAgentMessage([]byte(`{
		"threadId":"thread-1",
		"item":{
			"type":"commandExecution",
			"text":"quota"
		}
	}`)) {
		t.Fatal(
			"non-agent item must not count as terminal model output",
		)
	}
}

func TestSilentCompletionSuppressedAfterAgentMessage(t *testing.T) {
	completed := []byte(`{
		"threadId":"thread-1",
		"turn":{
			"status":"completed",
			"items":[]
		}
	}`)

	if !silentCompletionNeedsQuotaVerification(
		activeTurn{},
		completed,
	) {
		t.Fatal(
			"empty completion without agent output should remain suspicious",
		)
	}

	if silentCompletionNeedsQuotaVerification(
		activeTurn{
			agentMessageComplete: true,
		},
		completed,
	) {
		t.Fatal(
			"real agent output must suppress silent quota recovery",
		)
	}
}

func TestAgentMessageCompletionState(t *testing.T) {
	m := &Multiplexer{
		activeTurns: map[string]activeTurn{
			"thread-1": {
				accountID: "account-a",
			},
		},
	}

	m.setAgentMessageComplete(
		"thread-1",
		"account-a",
		true,
	)

	active, ok := m.activeTurnFor(
		"thread-1",
		"account-a",
	)

	if !ok {
		t.Fatal("active turn disappeared")
	}

	if !active.agentMessageComplete {
		t.Fatal(
			"completed agent output was not recorded",
		)
	}

	// Starting another item means work continued, so the terminal candidate
	// must be clearable again.
	m.setAgentMessageComplete(
		"thread-1",
		"account-a",
		false,
	)

	active, _ = m.activeTurnFor(
		"thread-1",
		"account-a",
	)

	if active.agentMessageComplete {
		t.Fatal(
			"subsequent activity did not clear terminal candidate",
		)
	}
}
