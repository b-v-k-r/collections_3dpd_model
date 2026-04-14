"""
predict.py
==========
End-to-end 2-DPD risk prediction pipeline.

Automatically scores all users who were at 2 DPD yesterday (CURRENT_DATE - 1).
No input table is required — the prediction base is built directly from the
live source tables filtered to yesterday's 2-DPD cohort.

Steps:
  1. Creates a Snowflake prediction base table for yesterday's 2-DPD users.
  2. Runs the SMS / in-app SQL feature pipeline against the prediction base.
  3. Runs the FTS SQL feature pipeline (activity, bureau, ledger, transactional,
     renewal, aa, ai_calling, legal) against the prediction base.
  4. Combines all feature tables into prediction-specific combined tables.
  5. Loads the trained XGBoost model, applies the same preprocessing as
     model_trainer.py, and outputs a risk probability for each loan.

Usage
-----
  python predict.py

  # Optional: save results back to Snowflake
  python predict.py --output-table analytics.data_science.my_predictions \\
                    --output-csv scores.csv

  # Skip individual steps when tables already exist
  python predict.py --skip-features

Output columns:
  USER_ID, LOAN_ID, CUTOFF_DATE, RISK_PROBABILITY, RISK_LABEL, THRESHOLD_USED
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import pickle
import re
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)

# ---------------------------------------------------------------------------
# Paths & project-root bootstrap
# ---------------------------------------------------------------------------

PREDICTION_DIR = Path(__file__).resolve().parent
MODEL_2DPD_DIR = PREDICTION_DIR.parent
FEATURES_DIR = MODEL_2DPD_DIR / "features"
FTS_DIR = FEATURES_DIR / "create_new_folder_fts"
SMS_DIR = FEATURES_DIR / "pipeline_and_sms_features"
MODEL_OUTPUTS_DIR = MODEL_2DPD_DIR / "model_outputs"

_PROJECT_ROOT = MODEL_2DPD_DIR
for _p in [str(_PROJECT_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db_service import execute_query, fetch_data, upload_to_snowflake  # noqa: E402

# ---------------------------------------------------------------------------
# Default table names
# ---------------------------------------------------------------------------

DEFAULT_PRED_BASE_TABLE = "analytics.data_science.early_dpd2_pred_base"
# Single-day working table: always CREATE OR REPLACE with just yesterday's rows.
# Feature pipelines read from this so they never see more than one day at a time.
DEFAULT_PRED_DAILY_TABLE = "analytics.data_science.early_dpd2_pred_base_daily"
PRED_COMBINED_PREFIX = "analytics.data_science.early_dpd2_pred_combined_features"
# Daily features history: ID cols + model-selected features, appended every run.
DEFAULT_PRED_FEATURES_TABLE = "analytics.data_science.early_dpd2_pred_daily_features"

# Feature tables written by the SQL pipelines.
# The prediction entrypoint can suffix these at runtime via --parser so shared
# base/training tables remain untouched.
FEATURE_TABLES = {
    "collect_dbt": "analytics.data_science.data_early_dpd2_sms_features_collect_dpd",
    "sms_final": "analytics.data_science.data_early_dpd2_sms_final_features",
    "inapp": "analytics.data_science.data_early_dpd2_final_app_features",
    "activity": "analytics.data_science.all_activity_features_for_early_dpd2",
    "ledger": "analytics.data_science.all_ledger_features_for_early_dpd2",
    "bureau": "analytics.data_science.all_bureau_features_for_early_dpd2",
    "transactional": "analytics.data_science.transactional_features_for_early_dpd2",
    "renewal": "analytics.data_science.renewal_features_for_early_dpd2",
    "aa": "analytics.data_science.aa_features_for_early_dpd2",
    "ai_calling": "analytics.data_science.ai_calling_features_for_early_dpd2",
    "legal_automation": "analytics.data_science.legal_automation_features_for_early_dpd2",
}

MERGE_KEYS = ["USER_ID", "CUTOFF_DATE", "LOAN_ID"]

# ---------------------------------------------------------------------------
# Constants shared with model_trainer.py — MUST stay in sync
# ---------------------------------------------------------------------------

CATEGORICAL_COLS = {
    "LOAN_STATUS",
    "TYPE",
    "IS_UPI_AUTOPAY_PRIMARY",
    "IS_LENDER_PAYMENT_SPLIT_ENABLED",
    "IS_MANUAL_REPAYMENT_ON_LENDER_MID",
    "MODE",
    "LENDER",
    "BANK_NAME",
    "PROVIDER",
    "LATEST_STATUS",
    "ACCOUNT_TYPE",
    "REASON_BUCKET",
    "META_AUTH_SUB_MODE",
    "UPI_FLOW",
    "AUTH_SUB_MODE",
}

ID_AND_LEAKAGE_COLS = {
    "LOAN_ID",
    "USER_ID",
    "CUTOFF_DATE",
    "REPAYMENTS_TRANSACTIONS_SPLIT",
    "FUTURE_MAX_DPD",
    "ACTUAL_DPD",
    "TEAM_ALLOCATION",
    "IS_ALLOCATION_EXPERIMENT",
    "SETTLEMENT_MIS_FLAG",
    "LENDER_LEAD_FLAG",
}

_NULL_SENTINELS = {"nan", "none", "nat", "<na>", ""}

# Ratio / delta window-suffix pairs (same as model_trainer._WINDOW_SUFFIX_PAIRS)
_WINDOW_SUFFIX_PAIRS = [
    ("_1_TO_7_D", "_8_TO_15_D"),
    ("_8_TO_15_D", "_16_TO_30_D"),
    ("_1_TO_7_D", "_16_TO_30_D"),
    ("_1_WEEK", "_2_WEEK"),
    ("_1_WEEK", "_4_WEEK"),
    ("_2_WEEK", "_4_WEEK"),
    ("_1_2_DAY", "_1_WEEK"),
    ("_1_WEEK", "_3_WEEK"),
]

# ---------------------------------------------------------------------------
# SQL execution helpers  (same logic as run_pipeline.py)
# ---------------------------------------------------------------------------

_COMMENT_ONLY = re.compile(r"^\s*--")
_CREATE_TABLE_RE = re.compile(
    r"(?i)\bcreate\s+or\s+replace\s+(?:transient\s+)?table\s+([A-Za-z0-9_.]+)"
)


def _normalize_parser_suffix(parser_name: str | None) -> str | None:
    if parser_name is None:
        return None
    suffix = re.sub(r"[^A-Za-z0-9_]+", "_", parser_name.strip())
    suffix = re.sub(r"_+", "_", suffix).strip("_").lower()
    return suffix or None


def _append_parser_suffix(table_name: str, parser_suffix: str | None) -> str:
    if not parser_suffix:
        return table_name
    parts = table_name.split(".")
    leaf = parts[-1]
    suffix = f"_{parser_suffix}"
    if leaf.lower().endswith(suffix.lower()):
        return table_name
    parts[-1] = f"{leaf}{suffix}"
    return ".".join(parts)


def _extract_created_tables(sql: str) -> list[str]:
    return [
        table_name
        for match in _CREATE_TABLE_RE.finditer(sql)
        if re.search(r"[A-Za-z0-9]", (table_name := match.group(1)))
    ]


def _replace_table_references(sql: str, table_name_map: dict[str, str] | None) -> str:
    if not table_name_map:
        return sql
    updated = sql
    for original, replacement in sorted(
        table_name_map.items(), key=lambda kv: len(kv[0]), reverse=True
    ):
        updated = re.sub(rf"(?i)\b{re.escape(original)}\b", replacement, updated)
    return updated


def _build_sql_table_name_map(
    sql_paths: list[Path], parser_suffix: str | None
) -> dict[str, str]:
    if not parser_suffix:
        return {}
    created_tables: dict[str, str] = {}
    for path in sql_paths:
        for table_name in _extract_created_tables(path.read_text()):
            created_tables.setdefault(
                table_name, _append_parser_suffix(table_name, parser_suffix)
            )
    return created_tables


def _build_feature_tables(parser_suffix: str | None) -> dict[str, str]:
    return {
        feature_name: _append_parser_suffix(table_name, parser_suffix)
        for feature_name, table_name in FEATURE_TABLES.items()
    }


def _is_runnable(stmt: str) -> bool:
    """True if the statement is non-empty, has non-comment content, and is not SET."""
    lines = [l for l in stmt.splitlines() if l.strip()]
    if not lines:
        return False
    non_comment = [l for l in lines if not _COMMENT_ONLY.match(l)]
    if not non_comment:
        return False
    return non_comment[0].strip().split()[0].lower() != "set"


def _split_statements(sql: str) -> list[str]:
    return [s.strip() for s in sql.split(";") if _is_runnable(s.strip())]


def _inject_base_tbl(sql: str, base_tbl: str) -> str:
    """Replace SET declaration and identifier($base_tbl) references."""
    sql = re.sub(
        r"(?i)(set\s+base_tbl\s*=\s*')[^']*(')",
        rf"\g<1>{base_tbl}\g<2>",
        sql,
    )
    sql = re.sub(r"(?i)identifier\(\s*\$base_tbl\s*\)", base_tbl, sql)
    return sql


def run_sql_file(
    path: Path,
    base_tbl: str,
    table_name_map: dict[str, str] | None = None,
) -> None:
    """Execute all runnable statements in a SQL file, substituting base_tbl."""
    sql = _inject_base_tbl(path.read_text(), base_tbl)
    sql = _replace_table_references(sql, table_name_map)
    statements = _split_statements(sql)
    total = len(statements)

    print(f"\n{'=' * 60}")
    print(f"  File : {path.name}  ({total} statement{'s' if total != 1 else ''})")
    print(f"{'=' * 60}")

    for i, stmt in enumerate(statements, 1):
        preview_lines = [
            l.strip()
            for l in stmt.splitlines()
            if l.strip() and not _COMMENT_ONLY.match(l.strip())
        ]
        preview = (preview_lines[0][:100] + "...") if preview_lines else stmt[:100]
        print(f"\n  [{i}/{total}] {preview}")
        t0 = time.time()
        try:
            execute_query(stmt)
            print(f"         ✓  Done in {time.time() - t0:.1f}s")
        except Exception as exc:
            print(f"         ✗  FAILED after {time.time() - t0:.1f}s: {exc}")
            raise


# ---------------------------------------------------------------------------
# features_combinator helpers  (mirrors data_creator.py — kept in sync)
# ---------------------------------------------------------------------------

def _get_columns(table_name: str) -> list[str]:
    df = fetch_data(f"SELECT * FROM {table_name} LIMIT 1")
    cols = list(df.columns)
    if cols:
        return cols
    for query in [f"SHOW COLUMNS IN TABLE {table_name}", f"DESC TABLE {table_name}"]:
        meta_df = fetch_data(query)
        if meta_df.empty:
            continue
        name_col = next(
            (c for c in ("name", "NAME", "column_name", "COLUMN_NAME") if c in meta_df.columns),
            None,
        )
        if not name_col:
            continue
        meta_cols = [c for c in meta_df[name_col].tolist() if isinstance(c, str) and c]
        if meta_cols:
            return meta_cols
    raise ValueError(f"Could not determine columns for '{table_name}'.")


def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _normalize_col(col: str) -> str:
    return "".join(ch for ch in col.upper() if ch.isalnum())


def _resolve_join_col(requested: str, available: list[str]) -> str | None:
    avail_upper = {c.upper(): c for c in available}
    req_upper = requested.upper()
    if req_upper in avail_upper:
        return avail_upper[req_upper]

    norm_map = {}
    for c in available:
        n = _normalize_col(c)
        if n not in norm_map:
            norm_map[n] = c
    req_norm = _normalize_col(requested)
    if req_norm in norm_map:
        return norm_map[req_norm]

    alias_map = {
        "USER_ID": ["USERID", "CUSTOMER_ID", "CUSTOMERID", "KB_ID", "PROFILE_ID"],
        "CUTOFF_DATE": ["CUTOFFDATE", "CUTOFF_DT", "AS_OF_DATE", "SNAPSHOT_DATE"],
    }
    for alias in alias_map.get(req_upper, []):
        if alias in avail_upper:
            return avail_upper[alias]
        if _normalize_col(alias) in norm_map:
            return norm_map[_normalize_col(alias)]

    if req_upper == "USER_ID":
        user_like = [c for c in available if "USER" in c.upper() and "ID" in c.upper()]
        if len(user_like) == 1:
            return user_like[0]
    elif req_upper == "CUTOFF_DATE":
        cutoff_like = [
            c for c in available
            if "CUTOFF" in c.upper() and ("DATE" in c.upper() or c.upper().endswith("_DT"))
        ]
        if len(cutoff_like) == 1:
            return cutoff_like[0]
    return None


def _features_combinator(
    base_table: str,
    tables: dict[str, str],
    target_table: str,
    join_columns: list[str] | None = None,
) -> None:
    """
    LEFT JOIN each table in `tables` onto `base_table` and write the result
    to `target_table` via a Snowflake CTAS.  Duplicate columns are deduplicated.
    """
    if join_columns is None:
        join_columns = ["cutoff_date", "user_id"]

    print(f"  Fetching base columns for {base_table} ...", flush=True)
    base_cols = _get_columns(base_table)
    seen_cols: set[str] = {c.upper() for c in base_cols}

    select_parts: list[str] = []
    join_parts: list[str] = []

    for alias, table_name in tables.items():
        print(f"  Fetching columns for {alias} ({table_name}) ...", flush=True)
        table_cols = _get_columns(table_name)

        selected_count = 0
        skipped_count = 0
        local_seen: set[str] = set()
        for col in table_cols:
            cu = col.upper()
            if cu in seen_cols or cu in local_seen:
                skipped_count += 1
                continue
            local_seen.add(cu)
            seen_cols.add(cu)
            select_parts.append(f"{alias}.{_quote_ident(col)}")
            selected_count += 1

        if skipped_count:
            print(
                f"  Skipping {skipped_count} duplicate cols from {alias}; "
                f"selecting {selected_count}.",
                flush=True,
            )

        resolved_pairs: list[tuple[str, str]] = []
        missing_join_cols: list[str] = []
        for req_col in join_columns:
            base_col = _resolve_join_col(req_col, base_cols)
            tbl_col = _resolve_join_col(req_col, table_cols)
            if not base_col or not tbl_col:
                missing_join_cols.append(req_col)
                continue
            resolved_pairs.append((base_col, tbl_col))

        if missing_join_cols:
            key_like = [
                c for c in table_cols
                if any(k in c.upper() for k in ("USER", "CUTOFF", "DATE", "DT", "ID"))
            ][:30]
            raise ValueError(
                f"Missing join columns {missing_join_cols} for '{table_name}'. "
                f"Available key-like cols: {key_like}"
            )

        on_clause = " AND ".join(
            f"base.{_quote_ident(bc)} = {alias}.{_quote_ident(tc)}"
            for bc, tc in resolved_pairs
        )
        join_parts.append(f"LEFT JOIN {table_name} AS {alias} ON {on_clause}")

    select_cols = ",\n    ".join(["base.*"] + select_parts)
    join_clauses = "\n    ".join(join_parts)
    query = (
        f"CREATE OR REPLACE TABLE {target_table} AS\n"
        f"SELECT\n    {select_cols}\n"
        f"FROM {base_table} AS base\n    {join_clauses}\n"
    )

    print(f"  Executing CTAS → {target_table} ...", flush=True)
    execute_query(query)
    print(f"  ✓ {target_table} created.", flush=True)


# ---------------------------------------------------------------------------
# Step 1 — Create prediction base table
# ---------------------------------------------------------------------------

def create_prediction_base(pred_base_table: str, pred_daily_table: str) -> None:
    """
    Append yesterday's 2-DPD cohort to the history base table, then build the
    daily-slice working table used by all downstream feature pipelines.

    pred_base_table  — permanent history table (CREATE IF NOT EXISTS + INSERT INTO)
    pred_daily_table — yesterday-only working table (CREATE OR REPLACE)
                       passed as $base_tbl to all SMS / FTS SQL files so they
                       always process exactly one day and produce no duplicates.
    """
    print(f"\n{'#' * 60}")
    print(f"  STEP 1 — Create prediction base table")
    print(f"  Cohort : users at 2 DPD as of yesterday (CURRENT_DATE - 1)")
    print(f"  History: {pred_base_table}")
    print(f"  Daily  : {pred_daily_table}")
    print(f"{'#' * 60}")

    select_body = (PREDICTION_DIR / "create_prediction_base.sql").read_text().strip().rstrip(";")

    # Step 1a — ensure history table exists (schema only, zero rows on first run)
    print(f"\n  Ensuring history table exists ...")
    execute_query(
        f"CREATE TABLE IF NOT EXISTS {pred_base_table} AS\n"
        f"SELECT * FROM (\n{select_body}\n) _schema_init WHERE 1=0"
    )

    # Step 1b — append yesterday's rows to the history table
    print(f"  Inserting yesterday's rows → {pred_base_table} ...")
    t0 = time.time()
    execute_query(f"INSERT INTO {pred_base_table}\n{select_body}")
    n_rows = fetch_data(f"SELECT COUNT(*) AS cnt FROM {pred_base_table}").iloc[0, 0]
    print(f"  ✓ History table: {int(n_rows):,} total rows  ({time.time() - t0:.1f}s)")

    # Step 1c — build daily-slice table (CREATE OR REPLACE, just yesterday)
    print(f"\n  Building daily-slice → {pred_daily_table} ...")
    t0 = time.time()
    execute_query(
        f"CREATE OR REPLACE TABLE {pred_daily_table} AS\n"
        f"SELECT * FROM {pred_base_table}\n"
        f"WHERE DATE(cutoff_date) = DATEADD('day', -1, CURRENT_DATE())"
    )
    n_daily = fetch_data(f"SELECT COUNT(*) AS cnt FROM {pred_daily_table}").iloc[0, 0]
    print(f"  ✓ Daily-slice: {int(n_daily):,} rows  ({time.time() - t0:.1f}s)")


# ---------------------------------------------------------------------------
# Step 2 — SMS / in-app feature pipelines
# ---------------------------------------------------------------------------

def run_sms_pipeline(pred_base_table: str, parser_suffix: str | None = None) -> None:
    print(f"\n{'#' * 60}")
    print(f"  STEP 2 — SMS / in-app feature pipelines")
    print(f"{'#' * 60}")
    t0 = time.time()
    sql_paths = [SMS_DIR / fname for fname in ["pipeline_sms1.sql", "pipeline_sms2.sql", "pipeline_inapp.sql"]]
    table_name_map = _build_sql_table_name_map(sql_paths, parser_suffix)
    for sql_path in sql_paths:
        run_sql_file(sql_path, pred_base_table, table_name_map=table_name_map)
    print(f"\n  SMS pipeline done in {time.time() - t0:.1f}s")


# ---------------------------------------------------------------------------
# Step 3 — FTS (create_new_folder_fts) feature pipelines
# ---------------------------------------------------------------------------

def run_fts_pipeline(pred_base_table: str, parser_suffix: str | None = None) -> None:
    print(f"\n{'#' * 60}")
    print(f"  STEP 3 — FTS feature pipelines")
    print(f"{'#' * 60}")
    t0 = time.time()
    fts_files = [
        "activity_features.sql",
        "bureau_features.sql",
        "ledger_features.sql",
        "transactional_features.sql",
        "renewal_features.sql",
        "aa_features.sql",
        "ai_calling_features.sql",
        "legal_automations.sql",
    ]
    sql_paths = [FTS_DIR / fname for fname in fts_files]
    table_name_map = _build_sql_table_name_map(sql_paths, parser_suffix)
    for sql_path in sql_paths:
        run_sql_file(sql_path, pred_base_table, table_name_map=table_name_map)
    print(f"\n  FTS pipeline done in {time.time() - t0:.1f}s")


# ---------------------------------------------------------------------------
# Step 4 — Combine all feature tables into prediction part-tables
# ---------------------------------------------------------------------------

def combine_features(
    pred_base_table: str,
    feature_tables: dict[str, str],
    pred_combined_prefix: str,
) -> list[str]:
    print(f"\n{'#' * 60}")
    print(f"  STEP 4 — Combine feature tables")
    print(f"{'#' * 60}")
    t0 = time.time()

    # Same chunking structure as data_creator.py
    parts = [
        ("Part 1", {"collect_dbt": feature_tables["collect_dbt"],
                    "sms_final":   feature_tables["sms_final"]},           "_part1"),
        ("Part 2", {"inapp":    feature_tables["inapp"],
                    "activity": feature_tables["activity"]},               "_part2"),
        # ledger before bureau (bureau is slow; ledger must not be blocked)
        ("Part 4", {"ledger":        feature_tables["ledger"]},            "_part4"),
        ("Part 3", {"bureau":        feature_tables["bureau"]},            "_part3"),
        ("Part 5", {"transactional": feature_tables["transactional"]},     "_part5"),
        ("Part 6", {"renewal":       feature_tables["renewal"]},           "_part6"),
        ("Part 7", {"aa":            feature_tables["aa"]},                "_part7"),
        ("Part 8", {"ai_calling":    feature_tables["ai_calling"]},        "_part8"),
        ("Part 9", {"legal_automation": feature_tables["legal_automation"]},"_part9"),
    ]

    output_tables: list[str] = []
    for part_name, part_tables, suffix in parts:
        target = pred_combined_prefix + suffix
        print(f"\n--- {part_name} → {target} ---")
        _features_combinator(pred_base_table, part_tables, target)
        output_tables.append(target)

    print(f"\n  Feature combination done in {time.time() - t0:.1f}s")
    return output_tables


# ---------------------------------------------------------------------------
# Step 5 — Load features, preprocess, and predict
# ---------------------------------------------------------------------------

def _engineer_window_features(X: pd.DataFrame) -> pd.DataFrame:
    """Create RATIO__ and DELTA__ features from paired time-window columns."""
    new_cols: dict = {}
    numeric_cols = set(X.select_dtypes(include=[np.number]).columns)
    for suf_a, suf_b in _WINDOW_SUFFIX_PAIRS:
        for col_a in X.columns:
            if not col_a.endswith(suf_a) or col_a not in numeric_cols:
                continue
            prefix = col_a[: -len(suf_a)]
            col_b = prefix + suf_b
            if col_b not in X.columns or col_b not in numeric_cols:
                continue
            ratio_name = f"RATIO__{prefix}{suf_a}_vs{suf_b}"
            delta_name = f"DELTA__{prefix}{suf_a}_vs{suf_b}"
            if ratio_name not in new_cols:
                new_cols[ratio_name] = (
                    X[col_a].astype(np.float32)
                    / (X[col_b].abs().astype(np.float32) + 1.0)
                ).astype(np.float32)
            if delta_name not in new_cols:
                new_cols[delta_name] = (
                    X[col_a].astype(np.float32) - X[col_b].astype(np.float32)
                ).astype(np.float32)
    if new_cols:
        X = pd.concat([X, pd.DataFrame(new_cols, index=X.index)], axis=1)
        print(f"  Engineered {len(new_cols)} ratio/delta features → {X.shape[1]} total")
    return X


def _downcast_floats(df: pd.DataFrame) -> pd.DataFrame:
    """Cast float64 → float32 and downcast integers to halve memory usage."""
    for col in df.columns:
        if col in MERGE_KEYS:
            continue
        col_dtype = df[col].dtype
        if col_dtype == np.float64:
            df[col] = df[col].astype(np.float32)
        elif col_dtype in (np.int64, np.int32):
            df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def _rss_gb() -> float:
    try:
        import psutil, os as _os
        return psutil.Process(_os.getpid()).memory_info().rss / 1e9
    except Exception:
        return 0.0


def load_and_predict(
    combined_tables: list[str],
    model_dir: Path,
    output_csv: Path | None,
    output_table: str | None = None,
    features_table: str | None = None,
) -> pd.DataFrame:
    print(f"\n{'#' * 60}")
    print(f"  STEP 5 — Load features and predict")
    print(f"{'#' * 60}")
    t0 = time.time()

    import xgboost as xgb

    # Load model artefacts
    model = xgb.XGBClassifier()
    model.load_model(str(model_dir / "xgb_model.ubj"))
    print(f"  Model loaded from {model_dir / 'xgb_model.ubj'}")

    with open(model_dir / "metadata.json") as f:
        meta = json.load(f)
    selected_features: list[str] = meta.get("selected_features", [])
    threshold: float = meta["metrics"].get("threshold", 0.5)
    print(f"  Selected features: {len(selected_features)} | Threshold: {threshold:.4f}")

    with open(model_dir / "label_encoders.pkl", "rb") as f:
        label_encoders: dict = pickle.load(f)

    # ── Load and merge combined part tables ───────────────────────────────────
    df_combined: pd.DataFrame | None = None
    # Sort tables to match training merge order (part1,2,3,4,5,6,7,8,9)
    sorted_tables = sorted(combined_tables, key=lambda t: int(re.search(r"_part(\d+)$", t).group(1)))

    for idx, table in enumerate(sorted_tables, start=1):
        print(f"  Fetching: {table}", flush=True)
        df = fetch_data(f"SELECT * FROM {table}")
        df.columns = [c.upper() for c in df.columns]

        missing_keys = [k for k in MERGE_KEYS if k not in df.columns]
        if missing_keys:
            print(f"  WARNING: {table} missing merge keys {missing_keys} — skipping")
            continue

        df = df.drop_duplicates(subset=MERGE_KEYS, keep="last")
        df = _downcast_floats(df)
        print(
            f"  → Fetched {len(df):,} rows × {df.shape[1]} cols"
            f"  [{df.memory_usage(deep=False).sum() / 1e9:.2f} GB]",
            flush=True,
        )

        if df_combined is None:
            df_combined = df
            del df
        else:
            common = set(df_combined.columns) & set(df.columns)
            df = df.drop(columns=[c for c in common if c not in MERGE_KEYS])
            df_combined = pd.merge(df_combined, df, on=MERGE_KEYS, how="left")
            del df

        gc.collect()
        print(
            f"  → {len(df_combined):,} rows × {df_combined.shape[1]} cols"
            f"  [RSS {_rss_gb():.2f} GB after part {idx}]",
            flush=True,
        )

    if df_combined is None or df_combined.empty:
        raise RuntimeError("No data loaded from combined feature tables.")

    print(f"\n  Combined: {len(df_combined):,} rows × {df_combined.shape[1]} cols")

    # ── Preserve ID columns for output ────────────────────────────────────────
    id_cols = [c for c in MERGE_KEYS if c in df_combined.columns]
    id_df = df_combined[id_cols].copy()

    # ── Preprocess (mirrors model_trainer.preprocess_partition) ───────────────
    df_combined.columns = [c.upper() for c in df_combined.columns]
    if "CUTOFF_DATE" in df_combined.columns:
        df_combined["CUTOFF_DATE"] = pd.to_datetime(
            df_combined["CUTOFF_DATE"], errors="coerce"
        )

    drop_cols = [c for c in df_combined.columns if c in ID_AND_LEAKAGE_COLS]
    X = df_combined.drop(columns=drop_cols)

    # Drop the target column if it leaked into the feature tables (it's -1 anyway)
    for tgt_col in ("TARGET_RISK_BUCKET_2D", "FUTURE_MAX_DPD"):
        if tgt_col in X.columns:
            X = X.drop(columns=[tgt_col])

    # Handle categorical columns using training-time label encoder levels
    for col in list(X.columns):
        if X[col].dtype != object:
            continue
        if col not in CATEGORICAL_COLS:
            X = X.drop(columns=[col])
            continue
        normalized = X[col].astype(str).apply(
            lambda v: "__MISSING__" if v.lower() in _NULL_SENTINELS else v
        )
        levels = label_encoders.get(col)
        if isinstance(levels, list):
            X[col] = pd.Categorical(normalized, categories=levels)
        else:
            # label_encoders stores "category" as a dummy value for unknown levels
            X[col] = pd.Categorical(normalized)

    # Drop any remaining object columns (safety net)
    for col in list(X.columns):
        if X[col].dtype == object:
            print(f"  Dropping non-categorical object col: {col}")
            X = X.drop(columns=[col])

    # ── Engineer ratio / delta window features (mirrors model_trainer) ────────
    X = _engineer_window_features(X)

    # ── Align to the exact feature set used at training time ──────────────────
    if selected_features:
        missing_feats = [f for f in selected_features if f not in X.columns]
        if missing_feats:
            print(
                f"  WARNING: {len(missing_feats)} selected features missing "
                f"— filling with NaN: {missing_feats[:10]}{'...' if len(missing_feats) > 10 else ''}"
            )
            for f in missing_feats:
                X[f] = np.nan
        X_pred = X[selected_features]
    else:
        X_pred = X

    # ── Store daily model features (ID cols + selected features) ──────────────
    if features_table:
        print(f"\n  Storing daily features → {features_table} ...")
        t_feat = time.time()
        features_df = pd.concat(
            [id_df.reset_index(drop=True), X_pred.reset_index(drop=True)],
            axis=1,
        )
        upload_to_snowflake(features_df, features_table, if_exists="append")
        print(f"  ✓ {len(features_df):,} rows × {len(selected_features)} features "
              f"appended → {features_table}  ({time.time() - t_feat:.1f}s)")

    # ── Score ─────────────────────────────────────────────────────────────────
    proba = model.predict_proba(X_pred)[:, 1]
    labels = (proba >= threshold).astype(int)

    result = id_df.copy().reset_index(drop=True)
    result["RISK_PROBABILITY"] = proba      # raw model probability (0–1)
    result["RISK_LABEL"] = labels           # 1 = risky, 0 = safe
    result["THRESHOLD_USED"] = threshold

    risky_n = int(labels.sum())
    print(f"\n  Scored {len(result):,} loans")
    print(f"  Risky (label=1): {risky_n:,}  ({labels.mean():.1%})")
    print(f"  Probability range: [{proba.min():.4f}, {proba.max():.4f}]  "
          f"median={np.median(proba):.4f}")

    if output_csv is not None:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_csv, index=False)
        print(f"  ✓ Predictions saved → {output_csv.resolve()}")

    if output_table:
        upload_to_snowflake(result, output_table, if_exists="append")
        print(f"  ✓ Predictions appended → {output_table}")

    print(f"  Scoring done in {time.time() - t0:.1f}s")
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="2-DPD risk prediction pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--output-csv",
        default=None,
        metavar="PATH",
        help="Local CSV path for predictions (optional; omit to skip local file write).",
    )
    p.add_argument(
        "--output-table",
        default=None,
        metavar="DB.SCHEMA.TABLE",
        help="Snowflake table to write predictions to (optional).",
    )
    p.add_argument(
        "--features-table",
        default=DEFAULT_PRED_FEATURES_TABLE,
        metavar="DB.SCHEMA.TABLE",
        help=(
            f"Snowflake table to append daily model features to "
            f"(ID cols + selected features). "
            f"Default: {DEFAULT_PRED_FEATURES_TABLE}. Pass empty string to skip."
        ),
    )
    p.add_argument(
        "--pred-base-table",
        default=DEFAULT_PRED_BASE_TABLE,
        metavar="DB.SCHEMA.TABLE",
        help=f"Snowflake table for full prediction history (default: {DEFAULT_PRED_BASE_TABLE}).",
    )
    p.add_argument(
        "--pred-daily-table",
        default=DEFAULT_PRED_DAILY_TABLE,
        metavar="DB.SCHEMA.TABLE",
        help=(
            f"Snowflake table for yesterday's-only daily slice used by feature pipelines "
            f"(default: {DEFAULT_PRED_DAILY_TABLE}).  Always CREATE OR REPLACE."
        ),
    )
    p.add_argument(
        "--parser",
        default=None,
        metavar="SUFFIX",
        help=(
            "Suffix appended to every prediction pipeline CREATE OR REPLACE TABLE target "
            "so base/shared tables are not overwritten."
        ),
    )
    p.add_argument(
        "--model-dir",
        default=str(MODEL_OUTPUTS_DIR),
        metavar="PATH",
        help=f"Directory containing model artefacts (default: {MODEL_OUTPUTS_DIR}).",
    )
    p.add_argument(
        "--skip-features",
        action="store_true",
        help="Skip all feature creation steps (assumes feature tables already exist).",
    )
    p.add_argument(
        "--skip-base",
        action="store_true",
        help="Skip prediction base table creation.",
    )
    p.add_argument(
        "--skip-sms",
        action="store_true",
        help="Skip the SMS / in-app pipeline step.",
    )
    p.add_argument(
        "--skip-fts",
        action="store_true",
        help="Skip the FTS pipeline step.",
    )
    p.add_argument(
        "--skip-combine",
        action="store_true",
        help=(
            "Skip the feature-combination step "
            "(assumes prediction combined tables already exist)."
        ),
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    overall_start = time.time()

    model_dir = Path(args.model_dir)
    output_csv = Path(args.output_csv) if args.output_csv else None
    parser_suffix = _normalize_parser_suffix(args.parser)
    pred_base_table = _append_parser_suffix(args.pred_base_table, parser_suffix)
    # Daily-slice table: always just yesterday's rows; used by feature pipelines
    pred_daily_table = _append_parser_suffix(args.pred_daily_table, parser_suffix)
    feature_tables = _build_feature_tables(parser_suffix)
    pred_combined_prefix = _append_parser_suffix(PRED_COMBINED_PREFIX, parser_suffix)

    if parser_suffix:
        print(f"Using parser suffix : {parser_suffix}")
        print(f"History base table  : {pred_base_table}")
        print(f"Daily-slice table   : {pred_daily_table}")

    if output_csv is None and args.output_table is None:
        # Default: write a local CSV next to predict.py
        output_csv = PREDICTION_DIR / "predictions.csv"
        print(f"No output destination specified — defaulting to {output_csv}")

    skip_all_features = args.skip_features or args.skip_base

    if not skip_all_features:
        create_prediction_base(pred_base_table, pred_daily_table)

    # Feature pipelines always use the daily-slice table so they only ever
    # process yesterday's rows — not the full accumulated history.
    if not skip_all_features and not args.skip_sms:
        run_sms_pipeline(pred_daily_table, parser_suffix=parser_suffix)

    if not skip_all_features and not args.skip_fts:
        run_fts_pipeline(pred_daily_table, parser_suffix=parser_suffix)

    if not args.skip_combine and not skip_all_features:
        combined_tables = combine_features(pred_daily_table, feature_tables, pred_combined_prefix)
    else:
        combined_tables = [f"{pred_combined_prefix}_part{i}" for i in [1, 2, 3, 4, 5, 6, 7, 8, 9]]
        print(
            f"\nSkipping feature creation — using existing combined tables "
            f"({pred_combined_prefix}_part1..9)"
        )

    result = load_and_predict(
        combined_tables,
        model_dir,
        output_csv=output_csv,
        output_table=args.output_table,
        features_table=args.features_table or None,
    )

    total_elapsed = time.time() - overall_start
    print(f"\n{'=' * 60}")
    print(f"  Pipeline complete in {total_elapsed / 60:.1f} min ({total_elapsed:.0f}s)")
    if output_csv:
        print(f"  CSV output      : {output_csv.resolve()}")
    if args.output_table:
        print(f"  Predictions     : {args.output_table}")
    if args.features_table:
        print(f"  Features table  : {args.features_table}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
