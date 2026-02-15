"""Minimal pipeline health test (Golden Test style).

This does NOT validate financial correctness. It only ensures:
- score_health.csv exists
- no NA scores
- no ScoreError rows
- scoring coverage is above a minimum threshold
- zero scores are allowed (common for crypto in bear trends) but non-crypto zero scores can be limited
- UI contract matches expected schema (prevents the UI from silently breaking)

Usage:
  python scripts/test_pipeline.py
  python scripts/test_pipeline.py --min-coverage 0.90
  python scripts/test_pipeline.py --max-noncrypto-zero 0
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _is_crypto(df: pd.DataFrame) -> pd.Series:
    if "IsCrypto" in df.columns:
        return df["IsCrypto"].fillna(False).astype(bool)
    for col in ("ScoreAssetClass", "asset_class"):
        if col in df.columns:
            return df[col].fillna("").astype(str).str.lower().eq("crypto")
    ys = df["YahooSymbol"] if "YahooSymbol" in df.columns else pd.Series("", index=df.index)
    tk = df["Ticker"] if "Ticker" in df.columns else pd.Series("", index=df.index)
    ys = ys.fillna("").astype(str)
    tk = tk.fillna("").astype(str)
    return ys.str.upper().str.endswith("-USD") | tk.str.upper().str.endswith("-USD")


def _validate_contract(csv_path: Path, contract_path: Path) -> tuple[bool, list[str]]:
    """Return (ok, messages). Messages are human-readable."""
    try:
        from scanner.data.schema.contract import validate_csv
    except Exception as e:
        return False, [f"could not import contract validator: {e}"]

    res = validate_csv(csv_path, contract_path)
    if res.ok:
        return True, res.warnings
    return False, res.errors + res.warnings


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--health", default=r"artifacts/reports/score_health.csv")
    ap.add_argument("--watchlist", default=r"artifacts/watchlist/watchlist_CORE.csv")
    ap.add_argument("--contract", default=r"configs/watchlist_contract.json")
    ap.add_argument("--skip-contract", action="store_true")

    ap.add_argument("--min-coverage", type=float, default=0.90)
    ap.add_argument("--max-noncrypto-zero", type=int, default=0)
    args = ap.parse_args()

    health_path = Path(args.health)
    if not health_path.exists():
        print(f"❌ Missing: {health_path}")
        return 2

    df = pd.read_csv(health_path)
    if "score" not in df.columns:
        print("❌ score_health.csv has no 'score' column")
        return 2

    score = pd.to_numeric(df["score"], errors="coerce")
    total = len(df)
    scored = int((score > 0).sum())
    na = int(score.isna().sum())

    # errors
    if "ScoreError" in df.columns:
        err = df["ScoreError"].fillna("").astype(str)
        err_rows = int(err.str.len().gt(0).sum())
    else:
        err_rows = 0

    crypto = _is_crypto(df)
    zero = score.fillna(0).eq(0)
    noncrypto_zero = int((zero & ~crypto).sum())

    coverage = (scored / total) if total else 0.0

    print(f"rows={total} scored={scored} (coverage={coverage:.1%}) NA={na} error_rows={err_rows} zero_noncrypto={noncrypto_zero}")

    problems = []
    if na > 0:
        problems.append(f"{na} NA scores")
    if err_rows > 0:
        problems.append(f"{err_rows} ScoreError rows")
    if coverage < args.min_coverage:
        problems.append(f"coverage {coverage:.1%} < {args.min_coverage:.1%}")
    if noncrypto_zero > args.max_noncrypto_zero:
        problems.append(f"non-crypto zero scores {noncrypto_zero} > {args.max_noncrypto_zero}")

    # contract gate
    if not args.skip_contract:
        csv_path = Path(args.watchlist)
        contract_path = Path(args.contract)
        if not csv_path.exists() or not contract_path.exists():
            problems.append(f"contract gate not configured (missing {csv_path} or {contract_path})")
        else:
            ok, msgs = _validate_contract(csv_path, contract_path)
            if not ok:
                problems.append("contract validation failed")
                for m in msgs[:8]:
                    problems.append(f"  - {m}")

    # briefing gate (deterministic, offline)
    try:
        from scanner.reports.briefing import build_briefing_from_csv, validate_briefing_json, write_briefing_outputs
        from scanner.data.io.paths import artifacts_dir, project_root
        from scanner.ui.generator import build_ui
    except Exception as e:
        problems.append(f"could not import briefing/ui modules: {e}")
    else:
        root = project_root()
        # choose a watchlist csv that exists
        candidates = [
            Path(args.watchlist),
            Path(r"artifacts/watchlist/watchlist_ALL.csv"),
            Path(r"artifacts/watchlist/watchlist_CORE.csv"),
            Path(r"artifacts/watchlist/watchlist_full.csv"),
        ]
        csv_in = None
        for c in candidates:
            p = (root / c) if not c.is_absolute() else c
            if p.exists():
                csv_in = p
                break
        if csv_in is None:
            problems.append("briefing gate: no watchlist CSV found")
        else:
            try:
                briefing = build_briefing_from_csv(csv_in, top_n=3, language="de")
                ok, errs = validate_briefing_json(briefing)
                if not ok:
                    problems.append("briefing.json validation failed")
                    for e in errs[:8]:
                        problems.append(f"  - {e}")
                else:
                    out = write_briefing_outputs(briefing=briefing, output_dir=artifacts_dir() / "reports")
                    # UI generator should not crash if briefing files are missing
                    reports_dir = artifacts_dir() / "reports"
                    bak = []
                    for fn in ("briefing_ai.txt", "briefing.txt"):
                        fp = reports_dir / fn
                        if fp.exists():
                            bp = reports_dir / (fn + ".bak")
                            try:
                                fp.rename(bp)
                                bak.append((bp, fp))
                            except Exception:
                                pass
                    try:
                        build_ui(csv_path=str(csv_in), out_html=r"artifacts/ui/index.html", contract_path=args.contract)
                    finally:
                        for bp, fp in bak:
                            try:
                                bp.rename(fp)
                            except Exception:
                                pass
            except Exception as e:
                problems.append(f"briefing gate failed: {e}")

    if problems:
        print("❌ FAIL:")
        for p in problems:
            print("-", p)
        return 1

    print("✅ OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
