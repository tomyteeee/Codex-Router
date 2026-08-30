package mux

import (
	"fmt"
	"math"
	"time"
)

const (
	quotaPacingShortFallback      = 5 * time.Hour
	quotaPacingWeeklyFallback     = 7 * 24 * time.Hour
	quotaPacingShortRiskHorizon   = 30 * time.Minute
	quotaPacingWeeklyRiskHorizon  = 24 * time.Hour
	quotaPacingShortReserve       = 10.0
	quotaPacingWeeklyReserve      = 12.0
	quotaPacingWeeklyScarcity     = 25.0
	quotaPacingMigrationAdvantage = 0.08
	quotaPacingFallbackGap        = 15.0
	quotaPacingRiskTrigger        = 0.35
	quotaPacingRiskImprovement    = 0.20
)

const (
	quotaDecisionExpiry    = "EXPIRY_URGENCY"
	quotaDecisionDepletion = "DEPLETION_PROTECTION"
	quotaDecisionLoad      = "LOAD_BALANCE"
	quotaDecisionHard      = "HARD_QUOTA_RECOVERY"
)

type quotaWindowPacing struct {
	known         bool
	resetKnown    bool
	staleReset    bool
	used          float64
	remaining     float64
	duration      time.Duration
	untilReset    time.Duration
	opportunity   float64
	depletionRisk float64
}

type quotaPacingState struct {
	usable           bool
	resetAware       bool
	short            quotaWindowPacing
	weekly           quotaWindowPacing
	desirability     float64
	depletionRisk    float64
	fallbackPressure float64
}

type quotaMigrationDecision struct {
	migrate   bool
	reason    string
	advantage float64
}

func clampQuotaValue(
	value float64,
	low float64,
	high float64,
) float64 {
	return math.Max(
		low,
		math.Min(
			high,
			value,
		),
	)
}

func quotaWindowPacingFor(
	now time.Time,
	window *RateLimitWindow,
	fallbackDuration time.Duration,
	reserveFloor float64,
	riskHorizon time.Duration,
) quotaWindowPacing {
	if window == nil {
		return quotaWindowPacing{}
	}

	used :=
		clampQuotaValue(
			window.UsedPercent,
			0,
			100,
		)

	result := quotaWindowPacing{
		known:     true,
		used:      used,
		remaining: 100 - used,
		duration:  fallbackDuration,
	}

	if window.WindowDurationMins != nil &&
		*window.WindowDurationMins > 0 {
		result.duration =
			time.Duration(
				*window.WindowDurationMins,
			) * time.Minute
	}

	if result.duration <= 0 {
		result.duration =
			fallbackDuration
	}

	if window.ResetsAt != nil {
		reset :=
			time.Unix(
				*window.ResetsAt,
				0,
			)

		until :=
			reset.Sub(now)

		if until > 0 &&
			until <=
				2*result.duration {
			result.resetKnown = true
			result.untilReset =
				until
		} else {
			result.staleReset = true
		}
	}

	if result.resetKnown {
		fractionLeft :=
			clampQuotaValue(
				float64(
					result.untilReset,
				)/
					float64(
						result.duration,
					),
				0,
				1,
			)

		expectedRemaining :=
			100 * fractionLeft

		surplus :=
			math.Max(
				0,
				result.remaining-
					expectedRemaining,
			)

		result.opportunity =
			surplus / 100
	}

	if result.remaining <= 0 {
		result.depletionRisk = 1
		return result
	}

	// Without reliable burn-rate telemetry, preserve a deterministic
	// continuity reserve. Expiry opportunity may encourage using quota
	// aggressively, but it must never make a critically depleted window
	// attractive merely because its reset is imminent.
	criticalReserve :=
		reserveFloor * 0.5

	if result.remaining <=
		criticalReserve {
		result.depletionRisk = 1
		return result
	}

	rawRisk :=
		math.Max(
			0,
			(reserveFloor-
				result.remaining)/
				reserveFloor,
		)

	riskMultiplier := 1.0

	if result.resetKnown {
		riskMultiplier =
			clampQuotaValue(
				float64(
					result.untilReset,
				)/
					float64(
						riskHorizon,
					),
				0.25,
				1,
			)
	}

	result.depletionRisk =
		clampQuotaValue(
			rawRisk*riskMultiplier,
			0,
			1,
		)

	return result
}

func quotaPacingStateForLimits(
	now time.Time,
	limits *RateLimits,
	credits resetCreditMetadata,
) quotaPacingState {
	state := quotaPacingState{}

	if limits == nil {
		return state
	}

	state.usable =
		rateLimitsHaveCapacity(
			limits,
		)

	weekly, short :=
		longestAndShortestWindow(
			limits,
		)

	state.short =
		quotaWindowPacingFor(
			now,
			short,
			quotaPacingShortFallback,
			quotaPacingShortReserve,
			quotaPacingShortRiskHorizon,
		)

	state.weekly =
		quotaWindowPacingFor(
			now,
			weekly,
			quotaPacingWeeklyFallback,
			quotaPacingWeeklyReserve,
			quotaPacingWeeklyRiskHorizon,
		)

	state.resetAware =
		state.short.resetKnown ||
			state.weekly.resetKnown

	state.depletionRisk =
		math.Max(
			state.short.depletionRisk,
			state.weekly.depletionRisk,
		)

	state.fallbackPressure =
		math.Max(
			state.short.used,
			state.weekly.used,
		)

	headroom := 100.0
	hasWindow := false

	for _, window := range []quotaWindowPacing{
		state.short,
		state.weekly,
	} {
		if !window.known {
			continue
		}

		hasWindow = true

		headroom =
			math.Min(
				headroom,
				window.remaining,
			)
	}

	if !hasWindow {
		headroom = 100
	}

	weeklyScarcity := 0.0

	if state.weekly.known {
		weeklyScarcity =
			math.Max(
				0,
				(quotaPacingWeeklyScarcity-
					state.weekly.remaining)/
					quotaPacingWeeklyScarcity,
			)
	}

	state.desirability =
		0.35*(headroom/100) +
			2.20*state.short.opportunity +
			1.20*state.weekly.opportunity -
			2.80*state.short.depletionRisk -
			4.00*state.weekly.depletionRisk -
			1.40*weeklyScarcity

	if credits.Known &&
		credits.AvailableCount > 0 {
		count :=
			min(
				credits.AvailableCount,
				routingResetBonusCreditCap,
			)

		state.desirability +=
			float64(count) * 0.03
	}

	return state
}

func compareQuotaPacingSelection(
	left quotaPacingState,
	right quotaPacingState,
) int {
	const epsilon = 0.000001

	leftCritical :=
		left.depletionRisk >=
			1-epsilon

	rightCritical :=
		right.depletionRisk >=
			1-epsilon

	if leftCritical !=
		rightCritical {
		if leftCritical {
			return -1
		}

		return 1
	}

	if math.Abs(
		left.desirability-
			right.desirability,
	) > epsilon {
		if left.desirability >
			right.desirability {
			return 1
		}

		return -1
	}

	return 0
}

func quotaRoutingDriver(
	state quotaPacingState,
) string {
	if state.depletionRisk >=
		quotaPacingRiskTrigger {
		return quotaDecisionDepletion
	}

	if state.short.opportunity+
		state.weekly.opportunity >=
		0.05 {
		return quotaDecisionExpiry
	}

	return quotaDecisionLoad
}

func quotaRebalanceDecision(
	source quotaPacingState,
	target quotaPacingState,
) quotaMigrationDecision {
	if !target.usable {
		return quotaMigrationDecision{}
	}

	riskImprovement :=
		source.depletionRisk -
			target.depletionRisk

	if source.depletionRisk >=
		quotaPacingRiskTrigger &&
		riskImprovement >=
			quotaPacingRiskImprovement {
		return quotaMigrationDecision{
			migrate:   true,
			reason:    quotaDecisionDepletion,
			advantage: riskImprovement,
		}
	}

	if source.resetAware &&
		target.resetAware {
		advantage :=
			target.desirability -
				source.desirability

		if advantage <
			quotaPacingMigrationAdvantage {
			return quotaMigrationDecision{
				advantage: advantage,
			}
		}

		reason :=
			quotaDecisionLoad

		targetOpportunity :=
			target.short.opportunity +
				target.weekly.opportunity

		sourceOpportunity :=
			source.short.opportunity +
				source.weekly.opportunity

		if targetOpportunity-
			sourceOpportunity >=
			0.05 {
			reason =
				quotaDecisionExpiry
		}

		return quotaMigrationDecision{
			migrate:   true,
			reason:    reason,
			advantage: advantage,
		}
	}

	if source.resetAware &&
		!target.resetAware &&
		source.short.opportunity+
			source.weekly.opportunity >=
			0.05 {
		return quotaMigrationDecision{}
	}

	pressureImprovement :=
		source.fallbackPressure -
			target.fallbackPressure

	if pressureImprovement >=
		quotaPacingFallbackGap {
		return quotaMigrationDecision{
			migrate:   true,
			reason:    quotaDecisionLoad,
			advantage: pressureImprovement / 100,
		}
	}

	return quotaMigrationDecision{
		advantage: pressureImprovement /
			100,
	}
}

func bestQuotaMigrationTarget(
	sourceID string,
	source quotaPacingState,
	candidates map[string]quotaPacingState,
) (
	string,
	quotaMigrationDecision,
) {
	bestID := ""
	best :=
		quotaMigrationDecision{}

	for accountID, candidate := range candidates {
		if accountID ==
			sourceID {
			continue
		}

		decision :=
			quotaRebalanceDecision(
				source,
				candidate,
			)

		if !decision.migrate {
			continue
		}

		if bestID == "" ||
			decision.advantage >
				best.advantage+
					0.000001 ||
			(math.Abs(
				decision.advantage-
					best.advantage,
			) <= 0.000001 &&
				accountID < bestID) {
			bestID =
				accountID
			best =
				decision
		}
	}

	return bestID, best
}

func quotaWindowPacingSummary(
	window quotaWindowPacing,
) string {
	if !window.known {
		return "missing"
	}

	reset := "unknown"

	if window.resetKnown {
		reset =
			window.untilReset.Round(
				time.Second,
			).String()
	} else if window.staleReset {
		reset = "stale"
	}

	return fmt.Sprintf(
		"used=%.1f rem=%.1f reset=%s opp=%.3f risk=%.3f",
		window.used,
		window.remaining,
		reset,
		window.opportunity,
		window.depletionRisk,
	)
}

func quotaPacingSummary(
	state quotaPacingState,
) string {
	return fmt.Sprintf(
		"score=%.3f short[%s] weekly[%s]",
		state.desirability,
		quotaWindowPacingSummary(
			state.short,
		),
		quotaWindowPacingSummary(
			state.weekly,
		),
	)
}
