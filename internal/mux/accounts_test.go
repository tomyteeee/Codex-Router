package mux

import (
	"testing"
	"time"
)

func TestPlanLabel(t *testing.T) {
	tests := map[string]string{
		"free":       "Free",
		"go":         "Go",
		"plus":       "Plus",
		"prolite":    "Pro 5x",
		"pro":        "Pro 20x",
		"business":   "Business",
		"enterprise": "Enterprise",
		"edu":        "Edu",
		"unknown":    "",
	}
	for planType, want := range tests {
		if got := planLabel(planType); got != want {
			t.Errorf("planLabel(%q) = %q, want %q", planType, got, want)
		}
	}
}

func TestLongestAndShortestWindowUsesQuotaDuration(t *testing.T) {
	shortMinutes := int64(300)
	weeklyMinutes := int64(10_080)
	short := &RateLimitWindow{UsedPercent: 72, WindowDurationMins: &shortMinutes}
	weekly := &RateLimitWindow{UsedPercent: 31, WindowDurationMins: &weeklyMinutes}

	longest, shortest := longestAndShortestWindow(&RateLimits{
		Primary: short, Secondary: weekly,
	})
	if longest != weekly || shortest != short {
		t.Fatalf("windows were not ordered by duration: longest=%#v shortest=%#v", longest, shortest)
	}
}

func TestLongestAndShortestWindowHandlesSingleWindow(t *testing.T) {
	t.Run("five hour window is short only", func(t *testing.T) {
		duration := int64(300)

		window := &RateLimitWindow{
			UsedPercent:        12,
			WindowDurationMins: &duration,
		}

		weekly, short := longestAndShortestWindow(&RateLimits{
			Primary: window,
		})

		if weekly != nil {
			t.Fatalf(
				"single 5h window must not be treated as weekly: %#v",
				weekly,
			)
		}

		if short != window {
			t.Fatalf(
				"expected single 5h window as short window: %#v",
				short,
			)
		}
	})

	t.Run("weekly window is weekly only", func(t *testing.T) {
		duration := int64(10_080)

		window := &RateLimitWindow{
			UsedPercent:        12,
			WindowDurationMins: &duration,
		}

		weekly, short := longestAndShortestWindow(&RateLimits{
			Primary: window,
		})

		if weekly != window {
			t.Fatalf(
				"expected single weekly window as weekly window: %#v",
				weekly,
			)
		}

		if short != nil {
			t.Fatalf(
				"single weekly window must not be treated as 5h: %#v",
				short,
			)
		}
	})

	t.Run("empty valid limits have no active windows", func(t *testing.T) {
		weekly, short := longestAndShortestWindow(&RateLimits{})

		if weekly != nil || short != nil {
			t.Fatalf(
				"empty valid rate limits should have no active windows: weekly=%#v short=%#v",
				weekly,
				short,
			)
		}
	})
}

func TestAggregateRateLimitsKeepsPoolAvailable(t *testing.T) {
	weeklyMinutes := int64(10_080)
	limits, err := aggregateRateLimits([]AccountSnapshot{
		{
			ID: "one", Enabled: true, Connected: true, AuthType: "chatgpt",
			RateLimits: &RateLimits{Primary: &RateLimitWindow{
				UsedPercent: 100, WindowDurationMins: &weeklyMinutes,
			}},
		},
		{
			ID: "two", Enabled: true, Connected: true, AuthType: "chatgpt",
			RateLimits: &RateLimits{Primary: &RateLimitWindow{
				UsedPercent: 20, WindowDurationMins: &weeklyMinutes,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if limits.Primary == nil || limits.Primary.UsedPercent != 60 {
		t.Fatalf("expected pooled usage to average to 60%%, got %#v", limits.Primary)
	}
	if limits.RateLimitReachedType != nil {
		t.Fatalf("pool should remain available while one account has capacity: %#v", limits)
	}
}

func TestAggregateRateLimitsReportsAllDepleted(t *testing.T) {
	limits, err := aggregateRateLimits([]AccountSnapshot{
		{
			ID: "one", Enabled: true, Connected: true, AuthType: "chatgpt",
			RateLimits: &RateLimits{Primary: &RateLimitWindow{UsedPercent: 100}},
		},
		{
			ID: "two", Enabled: true, Connected: true, AuthType: "chatgpt",
			RateLimits: &RateLimits{Primary: &RateLimitWindow{UsedPercent: 100}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if limits.RateLimitReachedType != "rate_limit_reached" {
		t.Fatalf("expected the pool to report depletion, got %#v", limits)
	}
}

func TestRouteUrgencyPrefersQuotaExpiringSooner(t *testing.T) {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	weeklyMinutes := int64(10_080)
	soon := now.Add(24 * time.Hour).Unix()
	later := now.Add(6 * 24 * time.Hour).Unix()
	soonScore := routeUrgencyScore(now, &RateLimitWindow{
		UsedPercent: 40, WindowDurationMins: &weeklyMinutes, ResetsAt: &soon,
	}, resetCreditMetadata{})
	laterScore := routeUrgencyScore(now, &RateLimitWindow{
		UsedPercent: 40, WindowDurationMins: &weeklyMinutes, ResetsAt: &later,
	}, resetCreditMetadata{})
	if soonScore <= laterScore {
		t.Fatalf("sooner reset should be more urgent: soon=%f later=%f", soonScore, laterScore)
	}
}

func TestRouteUrgencyWeightsBankedResetsWithoutDominating(t *testing.T) {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	weeklyMinutes := int64(10_080)
	reset := now.Add(4 * 24 * time.Hour).Unix()
	window := &RateLimitWindow{
		UsedPercent: 50, WindowDurationMins: &weeklyMinutes, ResetsAt: &reset,
	}
	plain := routeUrgencyScore(now, window, resetCreditMetadata{Known: true})
	banked := routeUrgencyScore(now, window, resetCreditMetadata{Known: true, AvailableCount: 2})
	if banked <= plain {
		t.Fatalf("banked resets should increase urgency: plain=%f banked=%f", plain, banked)
	}
	if banked > plain*1.31 {
		t.Fatalf("banked reset bonus should remain bounded: plain=%f banked=%f", plain, banked)
	}
}

func TestRouteUrgencyCapsResetBonus(t *testing.T) {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	reset := now.Add(7 * 24 * time.Hour).Unix()
	window := &RateLimitWindow{UsedPercent: 20, ResetsAt: &reset}
	three := routeUrgencyScore(now, window, resetCreditMetadata{Known: true, AvailableCount: 3})
	ten := routeUrgencyScore(now, window, resetCreditMetadata{Known: true, AvailableCount: 10})
	if three != ten {
		t.Fatalf("reset bonus cap was not applied: three=%f ten=%f", three, ten)
	}
}

func TestRouteUrgencyFallsBackToWeeklyUtilization(t *testing.T) {
	now := time.Date(2026, time.August, 16, 12, 0, 0, 0, time.UTC)
	weeklyMinutes := int64(10_080)
	lessUsed := routeUrgencyScore(now, &RateLimitWindow{
		UsedPercent: 20, WindowDurationMins: &weeklyMinutes,
	}, resetCreditMetadata{})
	moreUsed := routeUrgencyScore(now, &RateLimitWindow{
		UsedPercent: 80, WindowDurationMins: &weeklyMinutes,
	}, resetCreditMetadata{})
	if lessUsed <= moreUsed {
		t.Fatalf("fallback should prefer the less-used account: less=%f more=%f", lessUsed, moreUsed)
	}
}

func TestRateLimitsHaveCapacityRejectsExhaustedShortWindow(t *testing.T) {
	shortMinutes := int64(300)
	weeklyMinutes := int64(10_080)

	limits := &RateLimits{
		Primary: &RateLimitWindow{
			UsedPercent:        100,
			WindowDurationMins: &shortMinutes,
		},
		Secondary: &RateLimitWindow{
			UsedPercent:        16,
			WindowDurationMins: &weeklyMinutes,
		},
		RateLimitReachedType: "rate_limit_reached",
	}

	if rateLimitsHaveCapacity(limits) {
		t.Fatal("account with exhausted 5-hour window must not have capacity")
	}
}

func TestRateLimitsHaveCapacityAllowsSingleWeeklyWindow(t *testing.T) {
	weeklyMinutes := int64(10_080)

	limits := &RateLimits{
		Primary: &RateLimitWindow{
			UsedPercent:        0,
			WindowDurationMins: &weeklyMinutes,
		},
		Secondary:            nil,
		RateLimitReachedType: nil,
	}

	if !rateLimitsHaveCapacity(limits) {
		t.Fatal("unused account with only a weekly window should have capacity")
	}
}

func TestAggregateRateLimitsTreatsShortWindowAsCapacityLimit(t *testing.T) {
	shortMinutes := int64(300)
	weeklyMinutes := int64(10_080)

	makeExhausted := func(id string) AccountSnapshot {
		return AccountSnapshot{
			ID:        id,
			Enabled:   true,
			Connected: true,
			AuthType:  "chatgpt",
			RateLimits: &RateLimits{
				Primary: &RateLimitWindow{
					UsedPercent:        100,
					WindowDurationMins: &shortMinutes,
				},
				Secondary: &RateLimitWindow{
					UsedPercent:        16,
					WindowDurationMins: &weeklyMinutes,
				},
				RateLimitReachedType: "rate_limit_reached",
			},
		}
	}

	limits, err := aggregateRateLimits([]AccountSnapshot{
		makeExhausted("one"),
		makeExhausted("two"),
	})

	if err != nil {
		t.Fatal(err)
	}

	if limits.RateLimitReachedType != "rate_limit_reached" {
		t.Fatalf(
			"pool with every short window exhausted must report depletion: %#v",
			limits,
		)
	}
}

func TestAggregateRateLimitsNormalizesMixedWindowPositions(t *testing.T) {
	short := int64(300)
	weekly := int64(10_080)

	snapshots := []AccountSnapshot{
		{
			ID:        "primary",
			Enabled:   true,
			Connected: true,
			AuthType:  "chatgpt",
			RateLimits: &RateLimits{
				Primary: &RateLimitWindow{
					UsedPercent:        100,
					WindowDurationMins: &short,
				},
				Secondary: &RateLimitWindow{
					UsedPercent:        16,
					WindowDurationMins: &weekly,
				},
				RateLimitReachedType: "rate_limit_reached",
			},
		},
		{
			ID:        "subscription-2",
			Enabled:   true,
			Connected: true,
			AuthType:  "chatgpt",
			RateLimits: &RateLimits{
				Primary: &RateLimitWindow{
					UsedPercent:        100,
					WindowDurationMins: &short,
				},
				Secondary: &RateLimitWindow{
					UsedPercent:        16,
					WindowDurationMins: &weekly,
				},
				RateLimitReachedType: "rate_limit_reached",
			},
		},
		{
			ID:        "subscription-3",
			Enabled:   true,
			Connected: true,
			AuthType:  "chatgpt",
			RateLimits: &RateLimits{
				// Untouched account currently exposes only its weekly
				// window, and exposes it as primary.
				Primary: &RateLimitWindow{
					UsedPercent:        0,
					WindowDurationMins: &weekly,
				},
				Secondary:            nil,
				RateLimitReachedType: nil,
			},
		},
	}

	got, err := aggregateRateLimits(snapshots)
	if err != nil {
		t.Fatal(err)
	}

	if got.Primary == nil ||
		got.Primary.WindowDurationMins == nil ||
		*got.Primary.WindowDurationMins != short {
		t.Fatalf("expected normalized 5h primary window, got %#v", got.Primary)
	}

	if diff := got.Primary.UsedPercent - (200.0 / 3.0); diff < -0.001 || diff > 0.001 {
		t.Fatalf(
			"expected pooled 5h usage ~= 66.67%%, got %.4f",
			got.Primary.UsedPercent,
		)
	}

	if got.Secondary == nil ||
		got.Secondary.WindowDurationMins == nil ||
		*got.Secondary.WindowDurationMins != weekly {
		t.Fatalf(
			"expected normalized weekly secondary window, got %#v",
			got.Secondary,
		)
	}

	if diff := got.Secondary.UsedPercent - (32.0 / 3.0); diff < -0.001 || diff > 0.001 {
		t.Fatalf(
			"expected pooled weekly usage ~= 10.67%%, got %.4f",
			got.Secondary.UsedPercent,
		)
	}

	if got.RateLimitReachedType != nil {
		t.Fatalf(
			"pool must not be depleted while subscription 3 has capacity: %#v",
			got.RateLimitReachedType,
		)
	}
}

func TestTurnCompletedUsageLimit(t *testing.T) {
	t.Run("structured usage limit", func(t *testing.T) {
		threadID, limited := turnCompletedUsageLimit([]byte(`{
			"threadId":"thread-1",
			"turn":{
				"status":"failed",
				"error":{
					"message":"usage exhausted",
					"codexErrorInfo":"usageLimitExceeded"
				}
			}
		}`))

		if threadID != "thread-1" {
			t.Fatalf("unexpected thread id: %q", threadID)
		}

		if !limited {
			t.Fatal("expected usage-limit failure")
		}
	})

	t.Run("ordinary completed turn", func(t *testing.T) {
		threadID, limited := turnCompletedUsageLimit([]byte(`{
			"threadId":"thread-2",
			"turn":{
				"status":"completed",
				"error":null
			}
		}`))

		if threadID != "thread-2" {
			t.Fatalf("unexpected thread id: %q", threadID)
		}

		if limited {
			t.Fatal("completed turn must not be classified as usage limited")
		}
	})

	t.Run("non quota failure", func(t *testing.T) {
		_, limited := turnCompletedUsageLimit([]byte(`{
			"threadId":"thread-3",
			"turn":{
				"status":"failed",
				"error":{
					"message":"sandbox failed",
					"codexErrorInfo":"sandboxError"
				}
			}
		}`))

		if limited {
			t.Fatal("sandbox failure must not trigger subscription failover")
		}
	})
}

func TestSilentCompletedTurn(t *testing.T) {
	t.Run("empty completed turn is suspicious", func(t *testing.T) {
		threadID, silent := silentCompletedTurn([]byte(`{
			"threadId":"thread-1",
			"turn":{
				"id":"turn-1",
				"status":"completed",
				"items":[]
			}
		}`))

		if threadID != "thread-1" {
			t.Fatalf("unexpected thread id: %q", threadID)
		}

		if !silent {
			t.Fatal("expected empty completed turn to be suspicious")
		}
	})

	t.Run("agent message means normal completion", func(t *testing.T) {
		_, silent := silentCompletedTurn([]byte(`{
			"threadId":"thread-2",
			"turn":{
				"id":"turn-2",
				"status":"completed",
				"items":[
					{
						"type":"agentMessage",
						"text":"Finished successfully."
					}
				]
			}
		}`))

		if silent {
			t.Fatal("normal agent completion must not be suspicious")
		}
	})
}
