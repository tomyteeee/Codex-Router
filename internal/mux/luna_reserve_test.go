package mux

import "testing"

func TestAggregateRateLimitsByLimitIDPreservesLunaReserve(t *testing.T) {
	weekly := int64(10_080)
	limitID := "codex_luna_reserve"
	limitName := "gpt-reserve"

	makeSnapshot := func(id string, reserveUsage float64) AccountSnapshot {
		return AccountSnapshot{
			ID:        id,
			Enabled:   true,
			Connected: true,
			AuthType:  "chatgpt",
			RateLimits: &RateLimits{
				Primary: &RateLimitWindow{
					UsedPercent:        100,
					WindowDurationMins: &weekly,
				},
				RateLimitReachedType: "rate_limit_reached",
			},
			RateLimitsByLimitID: map[string]*RateLimits{
				limitID: {
					LimitID:   &limitID,
					LimitName: &limitName,
					Primary: &RateLimitWindow{
						UsedPercent:        reserveUsage,
						WindowDurationMins: &weekly,
					},
				},
			},
		}
	}

	got, err := aggregateRateLimitsByLimitID([]AccountSnapshot{
		makeSnapshot("one", 20),
		makeSnapshot("two", 60),
	})
	if err != nil {
		t.Fatal(err)
	}

	reserve := got[limitID]
	if reserve == nil {
		t.Fatalf("missing pooled Luna Reserve bucket: %#v", got)
	}

	if reserve.LimitID == nil || *reserve.LimitID != limitID {
		t.Fatalf("unexpected Reserve limit ID: %#v", reserve.LimitID)
	}

	if reserve.LimitName == nil || *reserve.LimitName != "gpt-reserve" {
		t.Fatalf("unexpected Reserve limit name: %#v", reserve.LimitName)
	}

	if reserve.Primary == nil || reserve.Primary.UsedPercent != 40 {
		t.Fatalf("expected pooled Reserve usage of 40%%, got %#v", reserve.Primary)
	}

	if reserve.RateLimitReachedType != nil {
		t.Fatalf("Reserve should still have capacity: %#v", reserve)
	}
}

func TestAggregateRateLimitsByLimitIDDoesNotTreatMissingReserveAsUnused(t *testing.T) {
	weekly := int64(10_080)
	limitID := "reserve_bucket"
	limitName := "gpt-reserve"

	snapshots := []AccountSnapshot{
		{
			ID:        "eligible",
			Enabled:   true,
			Connected: true,
			AuthType:  "chatgpt",
			RateLimitsByLimitID: map[string]*RateLimits{
				limitID: {
					LimitID:   &limitID,
					LimitName: &limitName,
					Primary: &RateLimitWindow{
						UsedPercent:        80,
						WindowDurationMins: &weekly,
					},
				},
			},
		},
		{
			ID:                  "not-eligible",
			Enabled:             true,
			Connected:           true,
			AuthType:            "chatgpt",
			RateLimitsByLimitID: nil,
		},
	}

	got, err := aggregateRateLimitsByLimitID(snapshots)
	if err != nil {
		t.Fatal(err)
	}

	reserve := got[limitID]
	if reserve == nil || reserve.Primary == nil {
		t.Fatalf("missing Reserve bucket: %#v", got)
	}

	if reserve.Primary.UsedPercent != 80 {
		t.Fatalf(
			"account without Reserve eligibility must not dilute usage; got %.2f%%",
			reserve.Primary.UsedPercent,
		)
	}
}

func TestLunaReserveBucketRoutingPhase2(t *testing.T) {
	weekly := int64(10_080)
	reserveID := "base_model_inference"
	reserveName := "gpt-reserve"

	snapshot := AccountSnapshot{
		Enabled:   true,
		Connected: true,
		AuthType:  "chatgpt",
		RateLimits: &RateLimits{
			Primary: &RateLimitWindow{
				UsedPercent:        100,
				WindowDurationMins: &weekly,
			},
			RateLimitReachedType: "rate_limit_reached",
		},
		RateLimitsByLimitID: map[string]*RateLimits{
			reserveID: {
				LimitID:   &reserveID,
				LimitName: &reserveName,
				Primary: &RateLimitWindow{
					UsedPercent:        0,
					WindowDurationMins: &weekly,
				},
			},
		},
	}

	if accountHasCapacityForQuotaBucket(
		snapshot,
		quotaBucketNormal,
	) {
		t.Fatal("normal quota should be exhausted")
	}

	if !accountHasCapacityForQuotaBucket(
		snapshot,
		quotaBucketReserve,
	) {
		t.Fatal(
			"Reserve must remain usable when normal quota is exhausted",
		)
	}
}

func TestLunaReserveMissingBucketMeansIneligible(t *testing.T) {
	snapshot := AccountSnapshot{
		Enabled:             true,
		Connected:           true,
		AuthType:            "chatgpt",
		RateLimitsByLimitID: nil,
	}

	if accountHasCapacityForQuotaBucket(
		snapshot,
		quotaBucketReserve,
	) {
		t.Fatal(
			"account without a gpt-reserve bucket must not be Reserve eligible",
		)
	}
}

func TestLunaReserveModelSelection(t *testing.T) {
	if got := quotaBucketFromParams(
		[]byte(`{"model":"gpt-reserve"}`),
	); got != quotaBucketReserve {
		t.Fatalf("expected Reserve bucket, got %q", got)
	}

	if got := quotaBucketFromParams(
		[]byte(`{"model":"gpt-5.6"}`),
	); got != quotaBucketNormal {
		t.Fatalf("expected normal bucket, got %q", got)
	}
}

func TestLunaReserveThreadBucketIsSticky(t *testing.T) {
	m := &Multiplexer{
		threadQuotaBuckets: make(map[string]quotaBucket),
		threadParents:      make(map[string]string),
	}

	threadID := "thread-luna-test"

	first := m.threadQuotaBucket(
		threadID,
		[]byte(`{"model":"gpt-reserve"}`),
	)

	if first != quotaBucketReserve {
		t.Fatalf("expected Reserve, got %q", first)
	}

	second := m.threadQuotaBucket(
		threadID,
		[]byte(`{"threadId":"thread-luna-test"}`),
	)

	if second != quotaBucketReserve {
		t.Fatalf(
			"omitted model must preserve sticky Reserve bucket; got %q",
			second,
		)
	}

	m.rememberExplicitThreadQuotaBucket(
		threadID,
		[]byte(`{"model":"gpt-5.6"}`),
	)

	restored := m.threadQuotaBucket(
		threadID,
		[]byte(`{"threadId":"thread-luna-test"}`),
	)

	if restored != quotaBucketNormal {
		t.Fatalf(
			"restoring a normal model must restore normal quota routing; got %q",
			restored,
		)
	}
}

func TestQuotaBlockKeysSeparateNormalAndReserve(t *testing.T) {
	normal := quotaBlockKey(
		"account",
		quotaBucketNormal,
	)

	reserve := quotaBlockKey(
		"account",
		quotaBucketReserve,
	)

	if normal == reserve {
		t.Fatal(
			"normal and Reserve quota blocks must be independent",
		)
	}
}

func TestNormalCapacityRequiresEveryWindowAvailable(t *testing.T) {
	short := int64(300)
	weekly := int64(10080)

	tests := []struct {
		name   string
		short  float64
		weekly float64
		want   bool
	}{
		{
			name:   "short empty weekly exhausted",
			short:  0,
			weekly: 100,
			want:   false,
		},
		{
			name:   "short exhausted weekly empty",
			short:  100,
			weekly: 0,
			want:   false,
		},
		{
			name:   "both exhausted",
			short:  100,
			weekly: 100,
			want:   false,
		},
		{
			name:   "both still usable",
			short:  99,
			weekly: 99,
			want:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := &RateLimits{
				Primary: &RateLimitWindow{
					UsedPercent:        tc.short,
					WindowDurationMins: &short,
				},
				Secondary: &RateLimitWindow{
					UsedPercent:        tc.weekly,
					WindowDurationMins: &weekly,
				},
			}

			if got := rateLimitsHaveCapacity(limits); got != tc.want {
				t.Fatalf(
					"rateLimitsHaveCapacity() = %v, want %v for short=%v weekly=%v",
					got,
					tc.want,
					tc.short,
					tc.weekly,
				)
			}
		})
	}
}

func TestParamsForQuotaBucketForcesReserveModel(t *testing.T) {
	params, err := paramsForQuotaBucket(
		[]byte(`{
			"threadId":"thread-1",
			"model":"gpt-5.4",
			"effort":"high"
		}`),
		quotaBucketReserve,
	)
	if err != nil {
		t.Fatal(err)
	}

	if got := quotaBucketFromParams(params); got != quotaBucketReserve {
		t.Fatalf(
			"Reserve fallback params selected %q, want %q: %s",
			got,
			quotaBucketReserve,
			string(params),
		)
	}
}
