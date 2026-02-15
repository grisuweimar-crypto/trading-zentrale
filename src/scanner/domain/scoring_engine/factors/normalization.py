from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence
import math


@dataclass(frozen=True)
class NormConfig:
    method: str = "percentile"   # "percentile" or "zsigmoid"
    winsor_p: float = 0.02       # winsorize tails (2% each side)
    neutral: float = 0.5


def _clean_floats(values: Iterable[float]) -> list[float]:
    out: list[float] = []
    for v in values:
        try:
            v = float(v)
            if math.isfinite(v):
                out.append(v)
        except Exception:
            continue
    return out


def winsorize(values: Sequence[float], p: float) -> list[float]:
    """Clip extremes to reduce outlier impact."""
    vals = sorted(_clean_floats(values))
    if not vals:
        return []
    n = len(vals)
    lo_i = max(0, min(n - 1, int(round(p * (n - 1)))))
    hi_i = max(0, min(n - 1, int(round((1 - p) * (n - 1)))))
    lo = vals[lo_i]
    hi = vals[hi_i]
    return [min(max(v, lo), hi) for v in vals]


def percentile_rank(x: float, distribution: Sequence[float], neutral: float = 0.5) -> float:
    dist = _clean_floats(distribution)
    if not dist or not math.isfinite(x):
        return neutral
    # percent strictly lower -> stable and simple
    cnt = sum(1 for v in dist if v < x)
    return cnt / len(dist)


def zsigmoid(x: float, mean: float, std: float, neutral: float = 0.5) -> float:
    if not (math.isfinite(x) and math.isfinite(mean) and math.isfinite(std)) or std <= 0:
        return neutral
    z = (x - mean) / std
    # sigmoid
    return 1.0 / (1.0 + math.exp(-z))


def scale_value(
    x: float,
    distribution: Sequence[float],
    cfg: NormConfig,
) -> float:
    """Return 0..1 scaled value from raw x and distribution."""
    dist = _clean_floats(distribution)
    if not dist:
        return cfg.neutral

    # outlier robust
    dist_w = winsorize(dist, cfg.winsor_p)

    if cfg.method == "percentile":
        v = percentile_rank(x, dist_w, cfg.neutral)
    else:  # "zsigmoid"
        mean = sum(dist_w) / len(dist_w)
        var = sum((d - mean) ** 2 for d in dist_w) / max(1, (len(dist_w) - 1))
        std = math.sqrt(var)
        v = zsigmoid(x, mean, std, cfg.neutral)

    # clamp
    if v < 0:
        return 0.0
    if v > 1:
        return 1.0
    return float(v)
