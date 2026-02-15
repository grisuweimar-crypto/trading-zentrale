from __future__ import annotations
from pathlib import Path
import os
import shutil
import pandas as pd

from scanner.data.io.paths import artifacts_dir, project_root
from scanner.data.io.safe_csv import to_csv_safely
from scanner.presets.load import load_presets
from scanner.presets.apply import apply_preset
from scanner.data.schema.canonical import canonicalize_df
from scanner.data.enrich.yahoo_taxonomy import load_mapping as load_yahoo_taxonomy, apply_mapping as apply_yahoo_taxonomy, derive_cluster_official
from scanner.data.enrich.pillars import load_mapping as load_pillars, apply_mapping as apply_pillars, derive_from_official_taxonomy, derive_from_legacy_categories
from scanner.app.score_step import apply_scoring
from scanner._version import __version__, __build__


def _dedupe_universe(df: pd.DataFrame) -> tuple[pd.DataFrame, list[int], pd.DataFrame | None]:
    """Deduplicate rows for preset/UI output.

    Why here (app-layer) and not in canonicalize_df?
    - canonicalize_df is a pure transform (no IO)
    - dedup should emit a report into artifacts/reports

    We dedupe on `asset_id` when present (preferred). Otherwise we fall back to `ticker`.
    If a score is available, we keep the highest-score row.
    """

    if df.empty:
        return df, list(df.index), None

    key = "asset_id" if "asset_id" in df.columns else ("ticker" if "ticker" in df.columns else None)
    if key is None:
        return df, list(df.index), None

    work = df.copy()
    work["_orig_index"] = work.index

    # Sort so that the first occurrence is the one we want to keep.
    # If we have crypto base-dedup (asset_id like CRYPTO:ADA), prefer quote currency in order:
    # USD -> USDT -> EUR -> others.
    quote_pref = {"USD": 0, "USDT": 1, "EUR": 2}
    if "quote_currency" in work.columns:
        qc = work["quote_currency"]
        if isinstance(qc, pd.DataFrame):
            qc = qc.iloc[:, 0] if qc.shape[1] else pd.Series("", index=work.index)
        qc = qc.fillna("").astype(str).str.upper()
        work["_quote_rank"] = qc.map(quote_pref).fillna(9).astype(int)
    else:
        work["_quote_rank"] = 9

    if "score" in work.columns:
        work["score"] = pd.to_numeric(work["score"], errors="coerce")
        work = work.sort_values(by=[key, "score", "_quote_rank"], ascending=[True, False, True], na_position="last")
    else:
        work = work.sort_values(by=[key, "_quote_rank"], ascending=[True, True])

    dup_mask = work.duplicated(subset=[key], keep="first")
    dupes_idx = work.loc[dup_mask, "_orig_index"].tolist()
    keep_idx = work.loc[~dup_mask, "_orig_index"].tolist()

    # Preserve original order for downstream outputs
    keep_idx_sorted = sorted(keep_idx)

    dupes_df: pd.DataFrame | None = None
    if dupes_idx:
        cols = [
            c
            for c in [
                "asset_id",
                "ticker",
                "ticker_display",
                "name",
                "isin",
                "symbol",
                "yahoo_symbol",
                "quote_currency",
                "score",
                "score_status",
                "is_crypto",
            ]
            if c in df.columns
        ]
        dupes_df = df.loc[dupes_idx, cols].copy() if cols else df.loc[dupes_idx].copy()

    return df.loc[keep_idx_sorted].copy(), keep_idx_sorted, dupes_df

def _find_input_watchlist() -> Path:
    p1 = artifacts_dir() / "watchlist" / "watchlist.csv"
    if p1.exists():
        return p1

    p2 = project_root() / "data" / "inputs" / "watchlist.csv"
    if p2.exists():
        # Bootstrap: if the user has not created an artifacts/watchlist/watchlist.csv yet,
        # copy the template/seed watchlist into artifacts so the "watchlist as DB" semantics hold.
        p1.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(p2, p1)
            print(f"Bootstrapped watchlist.csv -> {p1}")
            return p1
        except Exception:
            # fallback to template path
            return p2

    raise FileNotFoundError(
        "Keine watchlist.csv gefunden. Lege eine ab unter:\n"
        f"- {p1}\n- {p2}"
    )

def build_watchlist_outputs() -> None:
    print(f"Scanner_vNext {__version__} (build {__build__})")
    src = _find_input_watchlist()

    out_dir = artifacts_dir() / "watchlist"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) RAW laden (watchlist.csv acts like a small DB snapshot)
    df_raw = pd.read_csv(src)

    # 1b) Optional: refresh market data via Yahoo Finance
    # IMPORTANT: This only updates input columns (price/perf/risk/regime). It never touches scores.
    # By default, this is auto-enabled on GitHub Actions, but stays OFF locally.
    try:
        from scanner.data.enrich.yahoo_prices import enrich_watchlist_with_yahoo, should_fetch_yahoo

        if should_fetch_yahoo():
            print("🔄 Yahoo Finance: fetch enabled (refreshing market data)")
            df_y, rep = enrich_watchlist_with_yahoo(df_raw, enabled=True)
            # Persist back to watchlist.csv ONLY if the source is the artifacts DB snapshot.
            # (Never overwrite templates under data/inputs)
            try:
                db_path = artifacts_dir() / "watchlist" / "watchlist.csv"
                if src.resolve() == db_path.resolve():
                    to_csv_safely(df_y, db_path, index=False)
                    print(f"Updated DB snapshot: {db_path}")
            except Exception:
                pass

            # Write a small enrichment report (for auditing/debug)
            try:
                reports_dir = artifacts_dir() / "reports"
                reports_dir.mkdir(parents=True, exist_ok=True)
                (reports_dir / "yahoo_enrichment_report.txt").write_text(rep.to_text(), encoding="utf-8")
            except Exception:
                pass

            df_raw = df_y
        else:
            print("ℹ️ Yahoo Finance: fetch disabled (using existing values from watchlist.csv)")
    except Exception as e:
        # Never break the pipeline on market fetch. Keep existing values.
        print(f"⚠️ Yahoo Finance enrichment skipped: {e}")

    # 2) RAW exportieren (ungeändert)
    raw_path = out_dir / "watchlist_full_raw.csv"
    to_csv_safely(df_raw, raw_path, index=False)
    print("Wrote:", raw_path)

    # 3) Scoring auf RAW (schreibt Score/Confidence/... in legacy-Spalten)
    df_scored_raw = apply_scoring(df_raw, universe_csv_path=str(src))

    # 4) Canonical + Derived erzeugen (für Presets + UI)
    df = canonicalize_df(df_scored_raw)

    # 4b) Deduplicate universe (common when watchlist.csv was maintained with mixed IDs)
    # We dedupe on canonical asset_id (prefer rows with higher score) and keep indices aligned
    # with df_scored_raw for health reports.
    df, kept_idx, dupes = _dedupe_universe(df)
    df_scored_raw = df_scored_raw.loc[kept_idx].copy()


    # 4c) Optional enrichments (cache-driven; never calls the network)
    df = apply_yahoo_taxonomy(df, load_yahoo_taxonomy())
    df = derive_cluster_official(df)
    df = apply_pillars(df, load_pillars())

    # Fallbacks (metadata only; never affects scoring):
    # 1) official taxonomy heuristic (sector/industry) → 5-säulen
    # 2) legacy categories (if present) → 5-säulen / concept2 / playground
    df = derive_from_official_taxonomy(df)
    df = derive_from_legacy_categories(df)

    # Ensure pillar columns exist (avoid contract warnings; empty values are fine)
    for _c in ("pillar_primary", "bucket_type", "pillar_confidence", "pillar_reason", "pillar_tags"):
        if _c not in df.columns:
            df[_c] = pd.NA
    if "bucket_type" in df.columns:
        df["bucket_type"] = df["bucket_type"].fillna("none")

    reports_dir = artifacts_dir() / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    if dupes is not None and not dupes.empty:
        dup_path = reports_dir / "watchlist_duplicates_dropped.csv"
        to_csv_safely(dupes, dup_path, index=False)
        print(f"⚠️ Dedup: dropped {len(dupes)} duplicate rows (see {dup_path})")

    # 5) Canonical exportieren
    full_path = out_dir / "watchlist_full.csv"
    to_csv_safely(df, full_path, index=False)
    print("Wrote:", full_path)

    # --- QUALITY / HEALTH REPORTS ---

    total = len(df)
    scored = int((df["score"] > 0).sum()) if "score" in df.columns else 0
    zero = int((df["score"] == 0).sum()) if "score" in df.columns else 0
    na = int(df["score"].isna().sum()) if "score" in df.columns else 0

    # Errors are tracked on the RAW-scored frame (ScoreError lives there)
    if "ScoreError" in df_scored_raw.columns:
        err_rows = int(df_scored_raw["ScoreError"].fillna("").astype(str).str.len().gt(0).sum())
    else:
        err_rows = 0

    # --- Zero-score semantics ---
    # A score of 0 is allowed and meaningful (typically "avoid"), especially for crypto in strong downtrends.
    # IMPORTANT: Do NOT override canonical `is_crypto` based on ScoreAssetClass (it can be wrong for pairs like ADA-EUR).
    # We use canonical flags when present and fall back to a robust OR-heuristic otherwise.

    def _first_series(frame: pd.DataFrame, col: str) -> pd.Series | None:
        if col not in frame.columns:
            return None
        obj = frame[col]
        if isinstance(obj, pd.DataFrame):
            return obj.iloc[:, 0] if obj.shape[1] else None
        return obj

    def _crypto_or_heuristic(frame: pd.DataFrame) -> pd.Series:
        idx = frame.index
        def _col(name: str) -> pd.Series:
            s = _first_series(frame, name)
            if s is None:
                return pd.Series("", index=idx)
            return s.fillna("").astype(str).str.strip()

        mask = pd.Series(False, index=idx)

        # explicit class (OR, never early-return)
        for col in ("ScoreAssetClass", "asset_class", "AssetClass"):
            s = _first_series(frame, col)
            if s is not None:
                cls = s.fillna("").astype(str).str.lower()
                mask = mask | cls.eq("crypto")
                break

        # suffix / pair heuristics
        quotes = ("USD", "EUR", "USDT", "BTC", "ETH")
        ys = _col("YahooSymbol").str.upper()
        tk = _col("Ticker").str.upper()
        sym = _col("Symbol").str.upper()
        for q in quotes:
            suf = f"-{q}"
            mask = mask | ys.str.endswith(suf) | tk.str.endswith(suf) | sym.str.endswith(suf)

        # name / sector/category markers
        nm = _col("Name").str.lower()
        mask = mask | nm.str.contains(r"\b(?:crypto|krypto|kryptow)\w*\b", regex=True, na=False)
        sec = _col("Sector").str.lower()
        mask = mask | sec.str.contains(r"\b(?:crypto|krypto)\b", regex=True, na=False)
        cat = _col("Sektor").str.lower()
        mask = mask | cat.str.contains(r"\b(?:crypto|krypto)\b", regex=True, na=False)
        return mask

    # Prefer canonical `is_crypto` if already present on df
    if "is_crypto" in df.columns:
        crypto_mask = df["is_crypto"].fillna(False).infer_objects(copy=False).astype(bool)
    else:
        crypto_mask = _crypto_or_heuristic(df_scored_raw).reindex(df.index, fill_value=False).astype(bool)

    zero_non_crypto = 0
    if "score" in df.columns:
        zero_mask = df["score"].fillna(0).astype(float).eq(0)
        zero_non_crypto = int((zero_mask & ~crypto_mask).sum())

        # Ensure stable UI fields exist (do not overwrite if already present)
        if "is_crypto" not in df.columns:
            df["is_crypto"] = crypto_mask.astype(bool)

        # score_status is part of the UI contract. We always make sure it's consistent with
        # score==0 semantics AND that ScoreError is reflected.
        err = df_scored_raw["ScoreError"].fillna("") if "ScoreError" in df_scored_raw.columns else pd.Series("", index=df.index)
        err = err.reindex(df.index, fill_value="")

        if "score_status" in df.columns:
            status = df["score_status"].fillna("OK").astype(str)
        else:
            status = pd.Series("OK", index=df.index, dtype="string")

        status[df["score"].isna()] = "NA"
        status[err.astype(str).str.len().gt(0)] = "ERROR"
        status[(zero_mask) & crypto_mask] = "AVOID_CRYPTO_BEAR"
        status[(zero_mask) & ~crypto_mask] = "AVOID"
        df["score_status"] = status

    print(
        f"Score coverage: {scored}/{total} score>0, {zero} score==0 "
        f"({zero_non_crypto} non-crypto), {na} NA, {err_rows} error-rows"
    )

    # Write a compact health CSV (always) so you can inspect quickly
    health_cols = [
        # identifiers
        "Ticker",
        "Name",
        "YahooSymbol",
        "ISIN",
        # canonical
        "score",
        "opportunity_score",
        "risk_score",
        "confidence",
        "cycle",
        "trend200",
        "trend_ok",
        "dollar_volume",
        "liquidity_ok",
        # scoring meta (from score_step)
        "ScoreMarketRegime",
        "ScoreAssetClass",
        "ScoreRiskMult",
        "ScoreMarketTrend200",
        # raw error
        "ScoreError",
    ]
    present = [c for c in health_cols if c in df_scored_raw.columns] + [c for c in health_cols if c in df.columns and c not in df_scored_raw.columns]
    health_df = pd.concat([df_scored_raw, df], axis=1)

    # add status flag (quickly tells you if 0 is deliberate or if there was an error)
    if "score" in health_df.columns:
        s = pd.to_numeric(health_df["score"], errors="coerce")
        err = health_df["ScoreError"].fillna("") if "ScoreError" in health_df.columns else ""
        # Prefer canonical flags if present on the merged frame
        if "is_crypto" in health_df.columns:
            obj = health_df["is_crypto"]
            if isinstance(obj, pd.DataFrame):
                obj = obj.iloc[:, 0] if obj.shape[1] else pd.Series(False, index=health_df.index)
            s_bool = obj.fillna(False)
            if s_bool.dtype == bool:
                crypto_mask_h = s_bool.astype(bool)
            else:
                # accept common truthy strings/numbers
                st = s_bool.astype(str).str.strip().str.lower()
                crypto_mask_h = st.isin({"1", "true", "t", "yes", "y"})
        else:
            crypto_mask_h = _crypto_or_heuristic(health_df).astype(bool)

        status = pd.Series("OK", index=health_df.index)
        status[s.isna()] = "NA"
        if isinstance(err, pd.Series):
            status[err.astype(str).str.len().gt(0)] = "ERROR"
        status[(s.fillna(0).eq(0)) & crypto_mask_h] = "AVOID_CRYPTO_BEAR"
        status[(s.fillna(0).eq(0)) & ~crypto_mask_h] = "AVOID"
        health_df["ScoreStatus"] = status
        health_df["IsCrypto"] = crypto_mask_h

    health_df = health_df.loc[:, [c for c in (present + ["ScoreStatus", "IsCrypto"]) if c in health_df.columns]]
    health_path = reports_dir / "score_health.csv"
    to_csv_safely(health_df, health_path, index=False)

    # Optional debug exports
    write_debug = os.getenv("SCANNER_WRITE_SCORE_DEBUG", "0").strip() in {"1", "true", "yes"}
    if write_debug or na > 0 or err_rows > 0 or zero > 0:
        # zero-score rows (canonical view)
        if "score" in df.columns:
            zero_path = reports_dir / "score_zero_rows.csv"
            to_csv_safely(df[df["score"] == 0], zero_path, index=False)
        if "score" in df.columns:
            na_path = reports_dir / "score_na_rows.csv"
            to_csv_safely(df[df["score"].isna()], na_path, index=False)
        if "ScoreError" in df_scored_raw.columns:
            err_path = reports_dir / "score_error_rows.csv"
            to_csv_safely(
                df_scored_raw[df_scored_raw["ScoreError"].fillna("").astype(str).str.len().gt(0)],
                err_path,
                index=False,
            )

    # Strict modes (opt-in)
    strict = os.getenv("SCANNER_STRICT_SCORING", "0").strip() in {"1", "true", "yes"}
    strict_zero = os.getenv("SCANNER_STRICT_ZERO_SCORE", "0").strip() in {"1", "true", "yes"}
    min_cov = os.getenv("SCANNER_EXPECT_SCORING_MIN_COVERAGE", "").strip()
    if min_cov:
        try:
            min_cov_f = float(min_cov)
        except Exception:
            min_cov_f = None
    else:
        min_cov_f = None

    cov = (scored / total) if total > 0 else 0.0
    if strict:
        problems = []
        if err_rows > 0:
            problems.append(f"{err_rows} error-rows")
        if na > 0:
            problems.append(f"{na} NA scores")
        # Zero-scores are allowed and meaningful. If you *really* want to fail on zero-scores,
        # we only enforce it for NON-CRYPTO rows (crypto in bear trend often clamps to 0).
        if strict_zero and zero_non_crypto > 0:
            problems.append(f"{zero_non_crypto} zero scores (non-crypto)")
        if min_cov_f is not None and cov < min_cov_f:
            problems.append(f"coverage {cov:.2%} < {min_cov_f:.2%}")

        if problems:
            raise RuntimeError(
                "Strict scoring failed: " + ", ".join(problems) + "\n"
                f"See reports in: {reports_dir}\n"
                f"- {health_path}"
            )

    # 6) Presets anwenden
    presets = load_presets()
    for pid, preset in presets.items():
        filtered = apply_preset(df, preset)
        if filtered is None:
            raise RuntimeError(f"apply_preset returned None for preset '{pid}'")

        p = out_dir / f"watchlist_{pid}.csv"
        to_csv_safely(filtered, p, index=False)
        print(f"Wrote: {p} ({len(filtered)} rows)")

    # 7) Briefing (Stage A deterministic + optional Stage B AI)
    # IMPORTANT: This must never influence scoring. It's an explainability/report output only.
    # UI reads artifacts/reports/briefing_ai.txt (preferred) or briefing.txt.
    try:
        from scanner.reports.briefing import (
            load_briefing_config,
            resolve_source_csv,
            build_briefing_from_csv,
            validate_briefing_json,
            write_briefing_outputs,
            generate_ai_briefing_text,
        )

        cfg = load_briefing_config(project_root() / "configs" / "briefing.yaml")
        src_csv = resolve_source_csv(cfg.source_csv)

        briefing = build_briefing_from_csv(src_csv, top_n=cfg.top_n, language=cfg.language)
        ok, errs = validate_briefing_json(briefing)
        if not ok:
            raise RuntimeError("briefing.json validation failed: " + "; ".join(errs[:6]))

        # Resolve output dir (must live under artifacts/)
        out_dir_cfg = Path(str(cfg.output_dir))
        brief_out_dir = out_dir_cfg if out_dir_cfg.is_absolute() else (project_root() / out_dir_cfg)

        ai_text = None
        if bool(cfg.enable_ai) and str(cfg.ai_provider).strip().lower() == "openai":
            try:
                model = os.getenv("OPENAI_MODEL") or str(cfg.ai_model)
                ai_text = generate_ai_briefing_text(briefing, model=model)
            except Exception as e:
                print(f"⚠️ AI briefing skipped (enable_ai=true, but API failed): {e}")

        out = write_briefing_outputs(
            briefing=briefing,
            output_dir=brief_out_dir,
            write_ai=bool(ai_text),
            ai_text=ai_text,
        )
        print(f"Wrote: {out.get('json')}")
        print(f"Wrote: {out.get('txt')}")
        if out.get("ai_txt"):
            print(f"Wrote: {out.get('ai_txt')}")

        # Keep a legacy daily summary for debugging/backward reference.
        try:
            legacy_path = reports_dir / "briefing_legacy.txt"
            _write_briefing(df=df, out_path=legacy_path)
            print(f"Wrote: {legacy_path}")
        except Exception:
            pass

    except Exception as e:
        # Fallback: legacy deterministic summary (never fail the pipeline on briefing)
        try:
            fallback_path = reports_dir / "briefing.txt"
            _write_briefing(df=df, out_path=fallback_path)
            print(f"⚠️ Briefing (new) failed: {e} — wrote legacy fallback: {fallback_path}")
        except Exception as e2:
            print(f"⚠️ Briefing write failed: {e2}")


def _write_briefing(df: pd.DataFrame, out_path: Path) -> None:
    """Write a small, deterministic daily briefing (no external API)."""

    lines: list[str] = []

    total = len(df)
    scored = int((df.get("score", 0) > 0).sum()) if "score" in df.columns else 0
    zeros = int((df.get("score", 0) == 0).sum()) if "score" in df.columns else 0

    lines.append("Scanner_vNext – Daily Briefing")
    lines.append("=" * 32)
    lines.append(f"Universe: {total} | Scored>0: {scored} | Score==0: {zeros}")

    # Top ideas (purely score-based)
    if "score" in df.columns:
        top = df.copy()
        top["score"] = pd.to_numeric(top["score"], errors="coerce")
        top = top[top["score"].notna()]
        top = top[top["score"] > 0].sort_values("score", ascending=False).head(3)
        if not top.empty:
            lines.append("")
            lines.append("Top 3 (by score)")
            for _, r in top.iterrows():
                tk = str(r.get("ticker") or r.get("Ticker") or "?")
                name = str(r.get("name") or r.get("Name") or "").strip()
                sc = float(r.get("score"))
                status = str(r.get("score_status") or r.get("ScoreStatus") or "OK")
                lines.append(f"- {tk}: {sc:.1f} | {status} | {name}")

    # Quick risk/health signals
    def _vc(col: str) -> str:
        if col not in df.columns:
            return "n/a"
        try:
            return str(df[col].value_counts(dropna=False).to_dict())
        except Exception:
            return "n/a"

    lines.append("")
    lines.append("Signals")
    for col in ("score_status", "trend_ok", "liquidity_ok", "is_crypto"):
        lines.append(f"- {col}: {_vc(col)}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
