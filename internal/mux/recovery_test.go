package mux

import "testing"

func TestParseTurnStarted(t *testing.T) {
	got := parseTurnStarted([]byte(`{
		"threadId":"child-1",
		"turn":{"id":"turn-9","status":"inProgress"}
	}`))
	if got.ThreadID != "child-1" || got.TurnID != "turn-9" {
		t.Fatalf("unexpected turn metadata: %#v", got)
	}
}

func TestParseTerminalUsageError(t *testing.T) {
	threadID, willRetry, limited := parseErrorNotification([]byte(`{
		"threadId":"thread-1",
		"turnId":"turn-1",
		"willRetry":false,
		"error":{
			"message":"platform usage limit reached",
			"codexErrorInfo":{"usageLimitExceeded":{}}
		}
	}`))
	if threadID != "thread-1" || willRetry || !limited {
		t.Fatalf(
			"unexpected error classification: thread=%q retry=%v limited=%v",
			threadID,
			willRetry,
			limited,
		)
	}
}

func TestParseRetryingUsageError(t *testing.T) {
	_, willRetry, limited := parseErrorNotification([]byte(`{
		"threadId":"thread-1",
		"turnId":"turn-1",
		"willRetry":true,
		"error":{"message":"rate limit"}
	}`))
	if !willRetry || !limited {
		t.Fatalf("expected retrying quota error: retry=%v limited=%v", willRetry, limited)
	}
}

func TestParseCollabQuotaFailure(t *testing.T) {
	sender, receivers, quotaChild := parseCollabItem([]byte(`{
		"threadId":"parent-1",
		"turnId":"turn-parent",
		"item":{
			"type":"collabAgentToolCall",
			"tool":"wait",
			"status":"completed",
			"senderThreadId":"parent-1",
			"receiverThreadIds":["child-1","child-2"],
			"agentsStates":{
				"child-1":{"status":"completed","message":"done"},
				"child-2":{"status":"errored","message":"platform usage limit reached"}
			}
		}
	}`))
	if sender != "parent-1" {
		t.Fatalf("unexpected sender %q", sender)
	}
	if len(receivers) != 2 {
		t.Fatalf("unexpected receivers %#v", receivers)
	}
	if quotaChild != "child-2" {
		t.Fatalf("expected child-2 quota failure, got %q", quotaChild)
	}
}

func TestParseUsageLimitedGoal(t *testing.T) {
	threadID := parseUsageLimitedGoal([]byte(`{
		"threadId":"thread-7",
		"turnId":"turn-7",
		"goal":{"status":"usageLimited"}
	}`))
	if threadID != "thread-7" {
		t.Fatalf("unexpected usage-limited thread %q", threadID)
	}
}

func TestTurnIDFromTurnStartResult(t *testing.T) {
	turnID := turnIDFromTurnStartResult([]byte(`{
		"turn":{"id":"turn-123","status":"inProgress"}
	}`))
	if turnID != "turn-123" {
		t.Fatalf("unexpected turn id %q", turnID)
	}
}
