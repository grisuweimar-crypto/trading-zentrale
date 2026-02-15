def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def weighted_score(factors: dict, weights: dict, neutral: float = 0.5) -> float:
    num = 0.0
    den = 0.0
    for k, w in weights.items():
        v = float(factors.get(k, neutral))
        if v < 0:
            v = 0.0
        if v > 1:
            v = 1.0
        num += v * float(w)
        den += float(w)
    return 0.0 if den == 0 else (num / den) * 100.0


def compute_scores(
    opportunity_factors_0_1: dict,
    risk_factors_0_1: dict,
    opp_weights: dict,
    risk_weights: dict,
    risk_multiplier: float,
    opp_weight: float = 0.65,
    risk_weight: float = 0.35,
) -> dict:
    opp_score = weighted_score(opportunity_factors_0_1, opp_weights)
    risk_score = weighted_score(risk_factors_0_1, risk_weights)

    # Weighted blend (clean + explainable)
    final = clamp((opp_weight * opp_score) - (risk_multiplier * risk_weight * risk_score), 0.0, 100.0)

    return {
        "final_score": round(final, 2),
        "opportunity_score": round(opp_score, 2),
        "risk_score": round(risk_score, 2),
        "meta": {
            "opp_weight": opp_weight,
            "risk_weight": risk_weight,
            "risk_multiplier": risk_multiplier,
        },
        "factor_breakdown": {
            "opportunity_factors_0_1": opportunity_factors_0_1,
            "risk_factors_0_1": risk_factors_0_1,
        },
    }
