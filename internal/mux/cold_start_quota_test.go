package mux

import (
	"testing"
	"time"
)

func TestInitialQuotaKnownState(
	t *testing.T,
) {
	m := &Multiplexer{
		initialQuotaKnown: make(map[string]struct{}),
	}

	if m.initialQuotaStateKnown(
		"account-1",
	) {
		t.Fatal(
			"unseen account started as known",
		)
	}

	m.markInitialQuotaKnown(
		"account-1",
	)

	if !m.initialQuotaStateKnown(
		"account-1",
	) {
		t.Fatal(
			"known account was not remembered",
		)
	}
}

func TestInitialQuotaWarmupAlreadyDoneDoesNotWait(
	t *testing.T,
) {
	done := make(chan struct{})
	close(done)

	m := &Multiplexer{
		initialQuotaWarmupDone: done,
	}

	start := time.Now()

	m.waitForInitialQuotaWarmup()

	if elapsed :=
		time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf(
			"completed warmup waited %v",
			elapsed,
		)
	}
}

func TestNormalQuotaBlockMarksInitialStateKnown(
	t *testing.T,
) {
	m := &Multiplexer{
		quotaBlocked:      make(map[string]time.Time),
		initialQuotaKnown: make(map[string]struct{}),
		now:               time.Now,
	}

	m.markAccountQuotaBlockedFor(
		"account-1",
		quotaBucketNormal,
	)

	if !m.initialQuotaStateKnown(
		"account-1",
	) {
		t.Fatal(
			"normal quota failure did not mark account state known",
		)
	}
}
