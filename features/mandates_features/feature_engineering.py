"""
Mandate behaviour feature engineering — Polars-accelerated.
Architecture:
  1. Load raw history rows from Snowflake (or CSV).
  2. classify_reason()  — vectorised Polars when-then chain → REASON_BUCKET per row.
  3. ref_agg()          — single group_by(LOAN_ID, CUTOFF_DATE, REFERENCE_ID).agg()
                          for ~95% of features.
  4. ref_python_pass()  — small per-reference Python pass for the three things Polars
                          can't express purely (deduped path string, max_overlap sweep,
                          active-duration-log JSON parse). Runs on the much smaller
                          reference frame, not on 2M rows.
  5. loan_agg()         — single group_by(LOAN_ID, CUTOFF_DATE).agg() over the
                          reference frame.
  6. Upload to analytics.data_science.collection_mandate_features.

Chunking and Checkpointing:
  - Processes loan_id/cutoff_date combinations in configurable chunks
  - Saves checkpoints after each chunk to enable resume capability
  - Supports resuming from the last checkpoint if interrupted
  - Appends results incrementally to Snowflake table
"""

import argparse
import json
import pickle
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db_service
from config import CHECKPOINT_DIR, CHECKPOINT_ENABLED, CHUNK_SIZE
from db_service import execute_query, fetch_data

BASE_SQL_PATH = Path(__file__).with_name("base_with_cutoff_date.sql")
OUTPUT_TABLE = "analytics.data_science.early_dpd2_mandate_features"
LOAN_KEY_COLS = ["LOAN_ID", "CUTOFF_DATE"]
REF_KEY_COLS = [*LOAN_KEY_COLS, "REFERENCE_ID"]

MODE_KEYS = ["UPI", "ENACH", "PNACH", "PDC"]
REASON_BUCKETS = [
    "SETTLEMENT_CLOSURE",
    "BULK_OPERATION",
    "CUSTOMER_ACTION",
    "MANUAL_OPERATION",
    "BANK_OR_NPCI",
    "AUTH_OR_REGISTRATION",
    "TECHNICAL_ERROR",
    "UNKNOWN",
]
PUBLIC_SECTOR_BANKS = {
    "State Bank of India",
    "Bank of Baroda",
    "Bank of India",
    "Punjab National Bank",
    "Union Bank of India",
    "Central Bank of India",
    "Indian Bank",
    "UCO Bank",
    "Indian Overseas Bank",
    "Punjab & Sind Bank",
    "Bank of Maharashtra",
    "IDBI",
}
DATETIME_COLS = [
    "CUTOFF_DATE",
    "LOAN_DISBURSED_DATE",
    "ACTUAL_LOAN_END_DATE",
    "MANDATE_CREATED_AT",
    "MANDATE_END_AT",
    "MANDATE_LAST_UPDATED_AT",
    "HISTORY_UPDATED_AT",
    "LAST_STATUS_SYNC_AT",
]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate user-behavioural loan-cutoff-level mandate features (Polars)."
    )
    parser.add_argument("--csv", help="CSV exported from base_with_cutoff_date.sql")
    parser.add_argument(
        "--snowflake-table",
        default=OUTPUT_TABLE,
        metavar="DB.SCHEMA.TABLE",
        help=f"Snowflake output table (default: {OUTPUT_TABLE})",
    )
    parser.add_argument(
        "--no-upload", action="store_true", help="Skip Snowflake upload"
    )
    parser.add_argument(
        "--sample-report", action="store_true", help="Print sample report"
    )
    parser.add_argument("--sample-size", type=int, default=20)

    # Chunking and checkpointing arguments
    parser.add_argument(
        "--chunked",
        action="store_true",
        default=True,
        help="Use chunked processing with checkpointing (default: True)",
    )
    parser.add_argument(
        "--no-chunked",
        dest="chunked",
        action="store_false",
        help="Disable chunked processing (process all at once)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help=f"Number of loan/cutoff combinations per chunk (default: {CHUNK_SIZE} from config)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Run ID for checkpointing. If not provided, generates a new one.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore any existing checkpoints",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint for the given run-id and exit",
    )
    parser.add_argument(
        "--warehouse",
        default=None,
        help="Override the Snowflake warehouse for this run.",
    )

    return parser.parse_args()


def apply_warehouse_override(warehouse: str | None) -> None:
    if not warehouse:
        return
    db_service.SNOWFLAKE_CONFIG["warehouse"] = warehouse
    db_service._connection_pool.close_all()
    print(f"Warehouse override: {warehouse}", flush=True)


# ---------------------------------------------------------------------------
# Data loading — returns a Polars DataFrame
# ---------------------------------------------------------------------------


def load_data(csv_path=None) -> pl.DataFrame:
    if csv_path:
        df = pl.read_csv(csv_path, infer_schema_length=10000, try_parse_dates=False)
    else:
        import decimal

        print(
            "Connecting to Snowflake and executing base_with_cutoff_date.sql ...",
            flush=True,
        )
        # Split on ";" so SET variables are executed separately — the Snowflake
        # connector's cursor.execute() only accepts a single statement at a time.
        sql_statements = [
            s.strip() for s in BASE_SQL_PATH.read_text().split(";") if s.strip()
        ]
        # Run every statement except the last one (SET / preamble) via execute_query
        for stmt in sql_statements[:-1]:
            print(f"  → Executing preamble: {stmt[:80]}...", flush=True)
            execute_query(stmt)
        # The final (non-empty) statement is the main SELECT
        pdf = fetch_data(sql_statements[-1])
        print(f"Query returned {len(pdf):,} rows.", flush=True)
        # Snowflake returns Decimal types — normalise to float before handing to Polars
        for col in pdf.columns:
            if pdf[col].dtype == object:
                sample = pdf[col].dropna().iloc[:1]
                if len(sample) > 0 and isinstance(sample.iloc[0], decimal.Decimal):
                    pdf[col] = (
                        pdf[col]
                        .apply(
                            lambda x: float(x) if isinstance(x, decimal.Decimal) else x
                        )
                        .astype(float)
                    )
        df = pl.from_pandas(pdf)

    # Normalise column names to upper
    df = df.rename({c: c.upper() for c in df.columns})

    # Cast datetime columns — Snowflake can return them as str, Date, int (epoch ms),
    # or Datetime depending on the fetch path (Arrow vs fetchall).
    for col in DATETIME_COLS:
        if col not in df.columns:
            continue
        dtype = df[col].dtype
        if dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.to_datetime(format=None, strict=False).alias(col)
            )
        elif dtype in (pl.Int32, pl.Int64, pl.UInt32, pl.UInt64):
            # Epoch milliseconds (fallback fetch path)
            df = df.with_columns(pl.from_epoch(pl.col(col), time_unit="ms").alias(col))
        elif dtype not in (pl.Datetime, pl.Date):
            df = df.with_columns(pl.col(col).cast(pl.Datetime, strict=False))

    # CUTOFF_DATE, LOAN_DISBURSED_DATE, ACTUAL_LOAN_END_DATE are date-only:
    # keep them as pl.Date (not Datetime) so downstream formatting is unambiguous.
    DATE_ONLY_COLS = ["CUTOFF_DATE", "LOAN_DISBURSED_DATE", "ACTUAL_LOAN_END_DATE"]
    for col in DATE_ONLY_COLS:
        if col in df.columns and df[col].dtype == pl.Datetime:
            df = df.with_columns(pl.col(col).cast(pl.Date).alias(col))

    # Ensure MANDATE_AMOUNT is numeric
    if "MANDATE_AMOUNT" in df.columns and df["MANDATE_AMOUNT"].dtype == pl.Utf8:
        df = df.with_columns(pl.col("MANDATE_AMOUNT").cast(pl.Float64, strict=False))

    return df


# ---------------------------------------------------------------------------
# Vectorised reason classification (Polars when/then chain)
# ---------------------------------------------------------------------------


def add_reason_bucket(df: pl.DataFrame) -> pl.DataFrame:
    """Add REASON_BUCKET column using a vectorised Polars expression."""
    # Build the reason text: prefer MANDATE_STATUS_REASON, else concat raw blobs
    reason_expr = (
        pl.when(
            pl.col("MANDATE_STATUS_REASON").is_not_null()
            & (pl.col("MANDATE_STATUS_REASON") != "")
        )
        .then(pl.col("MANDATE_STATUS_REASON").str.to_lowercase())
        .otherwise(
            pl.concat_str(
                [
                    pl.col(c).fill_null("")
                    for c in [
                        "RAW_RESPONSE",
                        "RAW_DATA",
                        "META",
                        "RAW_DATA_PUBLIC_MANDATE",
                    ]
                ],
                separator=" || ",
            ).str.to_lowercase()
        )
        .alias("_REASON_TEXT")
    )

    bucket_expr = (
        pl.when(
            pl.col("_REASON_TEXT").str.contains("settled loan")
            | pl.col("_REASON_TEXT").str.contains("settled loans")
            | pl.col("_REASON_TEXT").str.contains("backfill mandate cancellation")
        )
        .then(pl.lit("SETTLEMENT_CLOSURE"))
        .when(
            pl.col("_REASON_TEXT").str.contains("bulk cancellation")
            | pl.col("_REASON_TEXT").str.contains("threaded script")
            | pl.col("_REASON_TEXT").str.contains("backfill")
        )
        .then(pl.lit("BULK_OPERATION"))
        .when(
            pl.col("_REASON_TEXT").str.contains("customer_cancelled")
            | pl.col("_REASON_TEXT").str.contains("customer cancelled")
            | pl.col("_REASON_TEXT").str.contains("user cancel")
        )
        .then(pl.lit("CUSTOMER_ACTION"))
        .when(
            pl.col("_REASON_TEXT").str.contains("manual cancellation")
            | pl.col("_REASON_TEXT").str.contains("foreclosed")
        )
        .then(pl.lit("MANUAL_OPERATION"))
        .when(
            pl.col("_REASON_TEXT").str.contains("bank")
            | pl.col("_REASON_TEXT").str.contains("reject")
            | pl.col("_REASON_TEXT").str.contains("npci")
        )
        .then(pl.lit("BANK_OR_NPCI"))
        .when(
            pl.col("_REASON_TEXT").str.contains("authentication")
            | pl.col("_REASON_TEXT").str.contains("create_success")
            | pl.col("_REASON_TEXT").str.contains("registration")
        )
        .then(pl.lit("AUTH_OR_REGISTRATION"))
        .when(
            pl.col("_REASON_TEXT").str.contains("error")
            | pl.col("_REASON_TEXT").str.contains("failed")
            | pl.col("_REASON_TEXT").str.contains("failure")
            | pl.col("_REASON_TEXT").str.contains("timeout")
        )
        .then(pl.lit("TECHNICAL_ERROR"))
        .otherwise(pl.lit("UNKNOWN"))
        .alias("REASON_BUCKET")
    )

    return df.with_columns(reason_expr).with_columns(bucket_expr).drop("_REASON_TEXT")


# ---------------------------------------------------------------------------
# Vectorised META signal extraction
# ---------------------------------------------------------------------------


def add_meta_signals(df: pl.DataFrame) -> pl.DataFrame:
    """Extract META signals — prefer pre-parsed SQL columns, fallback to JSON."""
    cols = df.columns

    # HAS_UMRN
    if "UMRN" in cols:
        df = df.with_columns(
            pl.col("UMRN").is_not_null().cast(pl.Int8).alias("META_HAS_UMRN")
        )
    else:
        df = df.with_columns(pl.lit(0).cast(pl.Int8).alias("META_HAS_UMRN"))

    # HAS_VPA
    if "UPI_VPA" in cols:
        df = df.with_columns(
            pl.col("UPI_VPA").is_not_null().cast(pl.Int8).alias("META_HAS_VPA")
        )
    else:
        df = df.with_columns(pl.lit(0).cast(pl.Int8).alias("META_HAS_VPA"))

    # UPI_FLOW
    if "UPI_FLOW" not in cols:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("UPI_FLOW"))

    # AUTH_SUB_MODE
    if "AUTH_SUB_MODE" not in cols:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("AUTH_SUB_MODE"))

    # HAS_ACTIVATION_DEBIT
    if "ACTIVATION_DEBIT_AMOUNT_SU" in cols:
        df = df.with_columns(
            (pl.col("ACTIVATION_DEBIT_AMOUNT_SU").fill_null(0) > 0)
            .cast(pl.Int8)
            .alias("META_HAS_ACTIVATION_DEBIT")
        )
    else:
        df = df.with_columns(pl.lit(0).cast(pl.Int8).alias("META_HAS_ACTIVATION_DEBIT"))

    # ACTIVATION_REFUND_REQUIRED
    if "ACTIVATION_REFUND_REQUIRED" in cols:
        df = df.with_columns(
            pl.col("ACTIVATION_REFUND_REQUIRED")
            .fill_null(False)
            .cast(pl.Int8)
            .alias("META_ACTIVATION_REFUND_REQUIRED")
        )
    else:
        df = df.with_columns(
            pl.lit(0).cast(pl.Int8).alias("META_ACTIVATION_REFUND_REQUIRED")
        )

    # IS_PUBLIC_SECTOR_BANK
    if "BANK_NAME" in cols:
        df = df.with_columns(
            pl.col("BANK_NAME")
            .is_in(list(PUBLIC_SECTOR_BANKS))
            .cast(pl.Int8)
            .alias("IS_PUBLIC_SECTOR_BANK")
        )
    else:
        df = df.with_columns(pl.lit(0).cast(pl.Int8).alias("IS_PUBLIC_SECTOR_BANK"))

    # IS_BUSINESS_HOURS / OFF_HOURS (if not pre-computed by SQL)
    if "IS_BUSINESS_HOURS_EVENT" not in cols and "HISTORY_UPDATED_AT" in cols:
        hour = pl.col("HISTORY_UPDATED_AT").dt.hour()
        df = df.with_columns(
            [
                ((hour >= 9) & (hour <= 18))
                .cast(pl.Int8)
                .alias("IS_BUSINESS_HOURS_EVENT"),
                ((hour < 9) | (hour > 18)).cast(pl.Int8).alias("IS_OFF_HOURS_EVENT"),
            ]
        )

    return df


# ---------------------------------------------------------------------------
# Vectorised per-event timing signals
# ---------------------------------------------------------------------------


def add_event_timing(df: pl.DataFrame) -> pl.DataFrame:
    """Flag rapid succession events (<60s) using per-group lag diff."""
    df = df.sort([*REF_KEY_COLS, "HISTORY_UPDATED_AT"])
    df = df.with_columns(
        pl.col("HISTORY_UPDATED_AT").shift(1).over(REF_KEY_COLS).alias("_PREV_EVENT_TS")
    )
    df = df.with_columns(
        (
            (pl.col("HISTORY_UPDATED_AT") - pl.col("_PREV_EVENT_TS")).dt.total_seconds()
        ).alias("_INTER_EVENT_SECS")
    )
    df = df.with_columns(
        [
            (pl.col("_INTER_EVENT_SECS") < 60).cast(pl.Int8).alias("_IS_RAPID"),
            (pl.col("_INTER_EVENT_SECS") > 86400).cast(pl.Int8).alias("_IS_SLOW"),
        ]
    ).drop(["_PREV_EVENT_TS", "_INTER_EVENT_SECS"])
    return df


# ---------------------------------------------------------------------------
# Reference-level aggregation — vectorised group_by
# ---------------------------------------------------------------------------


def ref_agg_polars(df: pl.DataFrame) -> pl.DataFrame:
    """
    Build ~90% of reference-level features using a single Polars group_by.agg().
    Returns one row per (LOAN_ID, CUTOFF_DATE, REFERENCE_ID).
    """
    # First ACTIVE timestamp per reference (needed to split pre/post)
    first_active = (
        df.filter(pl.col("HISTORY_STATUS") == "ACTIVE")
        .group_by(REF_KEY_COLS)
        .agg(pl.col("HISTORY_UPDATED_AT").min().alias("FIRST_ACTIVE_TS"))
    )

    df = df.join(first_active, on=REF_KEY_COLS, how="left")

    # Pre-ACTIVE mask
    df = df.with_columns(
        [
            (
                pl.col("FIRST_ACTIVE_TS").is_null()
                | (pl.col("HISTORY_UPDATED_AT") < pl.col("FIRST_ACTIVE_TS"))
            ).alias("_PRE_ACTIVE"),
            (
                pl.col("FIRST_ACTIVE_TS").is_not_null()
                & (pl.col("HISTORY_UPDATED_AT") > pl.col("FIRST_ACTIVE_TS"))
                & (pl.col("HISTORY_STATUS") == "CANCELLED")
            ).alias("_IS_POST_ACTIVE_CANCEL"),
        ]
    )

    # Per-event cancel reason bucket on cancelled events
    cancelled_df = df.filter(pl.col("HISTORY_STATUS") == "CANCELLED")

    # Post-active cancel reason counts per reference
    post_cancel = (
        cancelled_df.filter(pl.col("_IS_POST_ACTIVE_CANCEL"))
        .group_by([*REF_KEY_COLS, "REASON_BUCKET"])
        .agg(pl.len().alias("_CNT"))
        .pivot(
            on="REASON_BUCKET",
            index=REF_KEY_COLS,
            values="_CNT",
            aggregate_function="sum",
        )
    )
    # Rename pivot columns to POST_CANCEL_* prefix
    rename_map = {}
    for b in REASON_BUCKETS:
        if b in post_cancel.columns:
            rename_map[b] = f"POST_CANCEL_{b}"
    post_cancel = post_cancel.rename(rename_map)
    # Fill missing reason buckets with 0
    for b in REASON_BUCKETS:
        col_name = f"POST_CANCEL_{b}"
        if col_name not in post_cancel.columns:
            post_cancel = post_cancel.with_columns(
                pl.lit(0).cast(pl.Int32).alias(col_name)
            )

    ref = df.group_by(REF_KEY_COLS).agg(
        [
            # Pass-through identifier
            *(
                [pl.col("USER_ID").drop_nulls().first().alias("USER_ID")]
                if "USER_ID" in df.columns
                else []
            ),
            # Identity
            pl.col("MODE").drop_nulls().last().alias("MODE"),
            (
                pl.col("LENDER").drop_nulls().last().alias("LENDER")
                if "LENDER" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("LENDER")
            ),
            (
                pl.col("BANK_NAME").drop_nulls().last().alias("BANK_NAME")
                if "BANK_NAME" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("BANK_NAME")
            ),
            (
                pl.col("PROVIDER").drop_nulls().last().alias("PROVIDER")
                if "PROVIDER" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("PROVIDER")
            ),
            (
                pl.col("MANDATE_AMOUNT").drop_nulls().last().alias("MANDATE_AMOUNT")
                if "MANDATE_AMOUNT" in df.columns
                else pl.lit(None).cast(pl.Float64).alias("MANDATE_AMOUNT")
            ),
            pl.col("IS_PUBLIC_SECTOR_BANK").max().alias("IS_PUBLIC_SECTOR_BANK"),
            (
                pl.col("LATEST_STATUS").drop_nulls().last().alias("LATEST_STATUS")
                if "LATEST_STATUS" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("LATEST_STATUS")
            ),
            (
                pl.col("LOAN_STATUS").drop_nulls().last().alias("LOAN_STATUS")
                if "LOAN_STATUS" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("LOAN_STATUS")
            ),
            # Key timestamps
            pl.col("MANDATE_CREATED_AT").drop_nulls().min().alias("MANDATE_CREATED_AT"),
            pl.col("MANDATE_END_AT").drop_nulls().min().alias("MANDATE_END_AT"),
            pl.col("HISTORY_UPDATED_AT").drop_nulls().max().alias("LAST_EVENT_AT"),
            pl.col("FIRST_ACTIVE_TS").drop_nulls().first().alias("SUCCESS_AT"),
            pl.col("HISTORY_UPDATED_AT")
            .filter(pl.col("HISTORY_STATUS") == "CANCELLED")
            .min()
            .alias("CANCEL_AT"),
            (
                pl.col("LOAN_DISBURSED_DATE")
                .drop_nulls()
                .min()
                .alias("LOAN_DISBURSED_DATE")
                if "LOAN_DISBURSED_DATE" in df.columns
                else pl.lit(None).cast(pl.Datetime).alias("LOAN_DISBURSED_DATE")
            ),
            (
                pl.col("ACTUAL_LOAN_END_DATE")
                .drop_nulls()
                .min()
                .alias("ACTUAL_LOAN_END_DATE")
                if "ACTUAL_LOAN_END_DATE" in df.columns
                else pl.lit(None).cast(pl.Datetime).alias("ACTUAL_LOAN_END_DATE")
            ),
            # Event counts
            pl.len().alias("TOTAL_EVENT_COUNT"),
            pl.col("HISTORY_STATUS").n_unique().alias("UNIQUE_STATUS_COUNT"),
            # Pre-ACTIVE status counts
            pl.col("HISTORY_STATUS")
            .filter(pl.col("_PRE_ACTIVE") & (pl.col("HISTORY_STATUS") == "PENDING"))
            .len()
            .alias("PRE_SUCCESS_PENDING_EVENTS"),
            pl.col("HISTORY_STATUS")
            .filter(pl.col("_PRE_ACTIVE") & (pl.col("HISTORY_STATUS") == "IN_PROGRESS"))
            .len()
            .alias("PRE_SUCCESS_IN_PROGRESS_EVENTS"),
            pl.col("HISTORY_STATUS")
            .filter(
                pl.col("_PRE_ACTIVE") & (pl.col("HISTORY_STATUS") == "CREATE_SUCCESS")
            )
            .len()
            .alias("PRE_SUCCESS_CREATE_SUCCESS_EVENTS"),
            pl.col("HISTORY_STATUS")
            .filter(pl.col("_PRE_ACTIVE") & (pl.col("HISTORY_STATUS") == "CANCELLED"))
            .len()
            .alias("PRE_SUCCESS_CANCELLED_EVENTS"),
            # Total / post-active cancellations
            (pl.col("HISTORY_STATUS") == "CANCELLED")
            .sum()
            .alias("TOTAL_CANCELLED_EVENTS"),
            pl.col("_IS_POST_ACTIVE_CANCEL")
            .sum()
            .cast(pl.Int32)
            .alias("POST_SUCCESS_CANCELLED_EVENTS"),
            # Timing bursts
            pl.col("_IS_RAPID").sum().alias("RAPID_SUCCESSION_EVENTS"),
            pl.col("_IS_SLOW").sum().alias("SLOW_PROGRESSION_EVENTS"),
            (
                pl.col("IS_BUSINESS_HOURS_EVENT").sum().alias("BUSINESS_HOURS_EVENTS")
                if "IS_BUSINESS_HOURS_EVENT" in df.columns
                else pl.lit(0).alias("BUSINESS_HOURS_EVENTS")
            ),
            (
                pl.col("IS_OFF_HOURS_EVENT").sum().alias("OFF_HOURS_EVENTS")
                if "IS_OFF_HOURS_EVENT" in df.columns
                else pl.lit(0).alias("OFF_HOURS_EVENTS")
            ),
            pl.col("HISTORY_UPDATED_AT")
            .drop_nulls()
            .first()
            .dt.hour()
            .alias("FIRST_EVENT_HOUR"),
            # Mandate lifecycle flags (from SQL pre-computed cols)
            (
                pl.col("MANDATE_OUTLIVES_LOAN")
                .drop_nulls()
                .first()
                .alias("MANDATE_OUTLIVES_LOAN")
                if "MANDATE_OUTLIVES_LOAN" in df.columns
                else pl.lit(0).alias("MANDATE_OUTLIVES_LOAN")
            ),
            (
                pl.col("MANDATE_PRE_DISBURSAL")
                .drop_nulls()
                .first()
                .alias("MANDATE_PRE_DISBURSAL")
                if "MANDATE_PRE_DISBURSAL" in df.columns
                else pl.lit(0).alias("MANDATE_PRE_DISBURSAL")
            ),
            (
                pl.col("DISBURSAL_TO_MANDATE_DAYS")
                .drop_nulls()
                .first()
                .alias("DISBURSAL_TO_MANDATE_DAYS")
                if "DISBURSAL_TO_MANDATE_DAYS" in df.columns
                else pl.lit(None).cast(pl.Float64).alias("DISBURSAL_TO_MANDATE_DAYS")
            ),
            (
                pl.col("MANDATE_PLANNED_SPAN_DAYS")
                .drop_nulls()
                .first()
                .alias("MANDATE_PLANNED_SPAN_DAYS")
                if "MANDATE_PLANNED_SPAN_DAYS" in df.columns
                else pl.lit(None).cast(pl.Float64).alias("MANDATE_PLANNED_SPAN_DAYS")
            ),
            # META signals
            pl.col("META_HAS_UMRN").max().alias("META_HAS_UMRN"),
            pl.col("META_HAS_VPA").max().alias("META_HAS_VPA"),
            pl.col("META_HAS_ACTIVATION_DEBIT")
            .max()
            .alias("META_HAS_ACTIVATION_DEBIT"),
            pl.col("META_ACTIVATION_REFUND_REQUIRED")
            .max()
            .alias("META_ACTIVATION_REFUND_REQUIRED"),
            (
                pl.col("AUTH_SUB_MODE").drop_nulls().last().alias("META_AUTH_SUB_MODE")
                if "AUTH_SUB_MODE" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("META_AUTH_SUB_MODE")
            ),
            # Account / bank type
            (
                pl.col("CUSTOMER_ACCOUNT_TYPE")
                .drop_nulls()
                .last()
                .alias("ACCOUNT_TYPE")
                if "CUSTOMER_ACCOUNT_TYPE" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("ACCOUNT_TYPE")
            ),
            # Reason bucket of the reference (last event's bucket)
            pl.col("REASON_BUCKET").last().alias("REASON_BUCKET"),
            # Active duration log (take last non-null value; parsed in python pass)
            (
                pl.col("MANDATE_ACTIVE_DURATION_LOG")
                .drop_nulls()
                .last()
                .alias("MANDATE_ACTIVE_DURATION_LOG")
                if "MANDATE_ACTIVE_DURATION_LOG" in df.columns
                else pl.lit(None).cast(pl.Utf8).alias("MANDATE_ACTIVE_DURATION_LOG")
            ),
        ]
    )

    # Derived columns (vectorised on the smaller ref frame)
    ref = ref.with_columns(
        [
            # HAS_SUCCESS / FAILED_BEFORE_SUCCESS
            pl.col("SUCCESS_AT").is_not_null().cast(pl.Int8).alias("HAS_SUCCESS"),
            (
                pl.col("CANCEL_AT").is_not_null()
                & (
                    pl.col("SUCCESS_AT").is_null()
                    | (pl.col("CANCEL_AT") < pl.col("SUCCESS_AT"))
                )
            )
            .cast(pl.Int8)
            .alias("FAILED_BEFORE_SUCCESS"),
            # Derived friction counts
            (pl.col("PRE_SUCCESS_PENDING_EVENTS") - 1)
            .clip(lower_bound=0)
            .alias("DUPLICATE_PENDING_EVENTS"),
            (pl.col("PRE_SUCCESS_IN_PROGRESS_EVENTS") - 1)
            .clip(lower_bound=0)
            .alias("DUPLICATE_IN_PROGRESS_EVENTS"),
            (
                (pl.col("PRE_SUCCESS_PENDING_EVENTS") - 1).clip(lower_bound=0)
                + pl.col("PRE_SUCCESS_IN_PROGRESS_EVENTS")
                + pl.col("PRE_SUCCESS_CREATE_SUCCESS_EVENTS")
            ).alias("PRE_SUCCESS_RETRY_EVENTS"),
            # Timing days (using duration arithmetic)
            (
                (pl.col("SUCCESS_AT") - pl.col("MANDATE_CREATED_AT")).dt.total_seconds()
                / 86400.0
            )
            .clip(lower_bound=0)
            .alias("TIME_TO_SUCCESS_DAYS"),
            (
                (pl.col("CANCEL_AT") - pl.col("MANDATE_CREATED_AT")).dt.total_seconds()
                / 86400.0
            )
            .clip(lower_bound=0)
            .alias("TIME_TO_FIRST_CANCEL_DAYS"),
            # ACTIVE_LIFESPAN: from first ACTIVE to mandate end (or last event)
            (
                (
                    pl.coalesce(
                        [
                            pl.col("MANDATE_END_AT"),
                            pl.col("LAST_EVENT_AT"),
                            pl.col("CUTOFF_DATE"),
                        ]
                    )
                    - pl.col("SUCCESS_AT")
                ).dt.total_seconds()
                / 86400.0
            )
            .clip(lower_bound=0)
            .alias("ACTIVE_LIFESPAN_DAYS"),
            # Reason flags
            pl.col("REASON_BUCKET")
            .is_in(["SETTLEMENT_CLOSURE", "BULK_OPERATION"])
            .cast(pl.Int8)
            .alias("IS_CLOSURE_REASON"),
            pl.col("REASON_BUCKET")
            .is_in(["CUSTOMER_ACTION", "MANUAL_OPERATION"])
            .cast(pl.Int8)
            .alias("IS_USER_DRIVEN_REASON"),
            pl.col("REASON_BUCKET")
            .is_in(["BANK_OR_NPCI", "AUTH_OR_REGISTRATION", "TECHNICAL_ERROR"])
            .cast(pl.Int8)
            .alias("IS_FAILURE_REASON"),
        ]
    )

    # Join post-cancel reason breakdown
    ref = ref.join(post_cancel, on=REF_KEY_COLS, how="left")
    for b in REASON_BUCKETS:
        col_name = f"POST_CANCEL_{b}"
        if col_name in ref.columns:
            ref = ref.with_columns(pl.col(col_name).fill_null(0))

    # Derived loan-end vs problem cancels
    ref = ref.with_columns(
        [
            (
                pl.col("POST_CANCEL_SETTLEMENT_CLOSURE").fill_null(0)
                + pl.col("POST_CANCEL_BULK_OPERATION").fill_null(0)
            ).alias("POST_CANCEL_LOAN_END_EVENTS"),
            (
                pl.col("POST_SUCCESS_CANCELLED_EVENTS")
                - pl.col("POST_CANCEL_SETTLEMENT_CLOSURE").fill_null(0)
                - pl.col("POST_CANCEL_BULK_OPERATION").fill_null(0)
            ).alias("POST_CANCEL_PROBLEM_EVENTS"),
        ]
    )

    return ref


# ---------------------------------------------------------------------------
# Python pass — only for things Polars can't express (sweep-line, JSON, strings)
# ---------------------------------------------------------------------------


def _safe_json(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"{}", "[]", "nan", "None"}:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _parse_mode_split(value):
    parsed = _safe_json(value)
    if not isinstance(parsed, dict):
        return {}
    out = {}
    for k, v in parsed.items():
        try:
            out[str(k).upper()] = int(v)
        except Exception:
            pass
    return out


def _max_overlap(intervals):
    """Sweep-line max simultaneous intervals."""
    import datetime

    events = []
    for s, e in intervals:
        if s is None or e is None:
            continue
        # Normalise date → datetime so comparisons don't blow up
        if isinstance(s, datetime.date) and not isinstance(s, datetime.datetime):
            s = datetime.datetime(s.year, s.month, s.day)
        if isinstance(e, datetime.date) and not isinstance(e, datetime.datetime):
            e = datetime.datetime(e.year, e.month, e.day)
        if e < s:
            e = s
        events.append((s, 1))
        events.append((e, -1))
    if not events:
        return 0
    events.sort(key=lambda x: (x[0], x[1]))
    cur = best = 0
    for _, d in events:
        cur += d
        best = max(best, cur)
    return best


def _parse_active_duration_log(value):
    parsed = _safe_json(value)
    if not isinstance(parsed, list) or not parsed:
        return 0, float("nan"), float("nan")
    windows = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        try:
            import pandas as pd

            s = pd.to_datetime(item.get("START_DATE"), errors="coerce")
            e = pd.to_datetime(item.get("END_DATE"), errors="coerce")
            if s is not pd.NaT and e is not pd.NaT:
                windows.append((s, e))
        except Exception:
            continue
    if not windows:
        return 0, float("nan"), float("nan")
    windows.sort()
    total = sum((e - s).days for s, e in windows)
    gap = sum(
        max((windows[i][0] - windows[i - 1][1]).days, 0) for i in range(1, len(windows))
    )
    return len(windows), float(total), float(gap)


def ref_python_pass(ref_df: pl.DataFrame) -> pl.DataFrame:
    """
    Small Python pass over the reference-level frame for:
      - DEDUPED_PATH / PATH_LENGTH  (need original row ordering — done separately)
      - ACTIVE_LOG features         (JSON parse)
      - MAX_SIMULTANEOUS_*          (sweep-line — needs intervals, done at loan level)
    Here we just add ACTIVE_LOG columns from JSON.
    """
    log_windows, log_total, log_gap = [], [], []
    for val in ref_df["MANDATE_ACTIVE_DURATION_LOG"].to_list():
        w, t, g = _parse_active_duration_log(val)
        log_windows.append(w)
        log_total.append(t)
        log_gap.append(g)

    ref_df = ref_df.with_columns(
        [
            pl.Series("ACTIVE_LOG_WINDOW_COUNT", log_windows, dtype=pl.Int32),
            pl.Series("ACTIVE_LOG_TOTAL_PLANNED_DAYS", log_total, dtype=pl.Float64),
            pl.Series("ACTIVE_LOG_GAP_DAYS", log_gap, dtype=pl.Float64),
        ]
    )
    return ref_df


# ---------------------------------------------------------------------------
# Loan-level aggregation
# ---------------------------------------------------------------------------


def _mode_agg_exprs(prefix: str, post_cancel_cols: list) -> list:
    """
    Return the full list of Polars aggregation expressions for one mode slice.
    All output columns are prefixed with e.g. 'UPI__' so they sit side-by-side
    in the final loan-cutoff-level table without collapsing across modes.
    """
    p = prefix  # e.g. "UPI__"
    pc_aggs = [pl.col(c).sum().alias(f"{p}{c}_TOTAL") for c in post_cancel_cols]
    return [
        pl.len().alias(f"{p}REFERENCE_COUNT"),
        pl.col("HAS_SUCCESS").sum().alias(f"{p}SUCCESSFUL_ATTEMPT_COUNT"),
        pl.col("FAILED_BEFORE_SUCCESS").sum().alias(f"{p}FAILED_ATTEMPT_COUNT"),
        pl.col("HAS_SUCCESS").mean().alias(f"{p}SUCCESS_RATE"),
        # Friction
        pl.col("PRE_SUCCESS_RETRY_EVENTS")
        .sum()
        .alias(f"{p}PRE_SUCCESS_RETRY_EVENTS_TOTAL"),
        pl.col("PRE_SUCCESS_PENDING_EVENTS")
        .sum()
        .alias(f"{p}PRE_SUCCESS_PENDING_EVENTS_TOTAL"),
        pl.col("PRE_SUCCESS_IN_PROGRESS_EVENTS")
        .sum()
        .alias(f"{p}PRE_SUCCESS_IN_PROGRESS_EVENTS_TOTAL"),
        pl.col("PRE_SUCCESS_CREATE_SUCCESS_EVENTS")
        .sum()
        .alias(f"{p}PRE_SUCCESS_CREATE_SUCCESS_EVENTS_TOTAL"),
        pl.col("PRE_SUCCESS_CANCELLED_EVENTS")
        .sum()
        .alias(f"{p}PRE_SUCCESS_CANCELLED_EVENTS_TOTAL"),
        pl.col("DUPLICATE_PENDING_EVENTS").sum().alias(f"{p}DUPLICATE_PENDING_TOTAL"),
        pl.col("DUPLICATE_IN_PROGRESS_EVENTS")
        .sum()
        .alias(f"{p}DUPLICATE_IN_PROGRESS_TOTAL"),
        # Post-cancel
        pl.col("POST_SUCCESS_CANCELLED_EVENTS")
        .sum()
        .alias(f"{p}POST_SUCCESS_CANCELLED_TOTAL"),
        pl.col("POST_CANCEL_LOAN_END_EVENTS")
        .sum()
        .alias(f"{p}POST_CANCEL_LOAN_END_TOTAL"),
        pl.col("POST_CANCEL_PROBLEM_EVENTS")
        .sum()
        .alias(f"{p}POST_CANCEL_PROBLEM_TOTAL"),
        *pc_aggs,
        pl.col("POST_SUCCESS_CANCELLED_EVENTS")
        .filter(pl.col("IS_CLOSURE_REASON") == 0)
        .sum()
        .alias(f"{p}NON_CLOSURE_POST_CANCELLED_TOTAL"),
        pl.col("TOTAL_CANCELLED_EVENTS")
        .filter(pl.col("IS_USER_DRIVEN_REASON") == 1)
        .sum()
        .alias(f"{p}USER_DRIVEN_CANCELLATIONS"),
        # Timing
        pl.col("TIME_TO_SUCCESS_DAYS").mean().alias(f"{p}AVG_TIME_TO_SUCCESS_DAYS"),
        pl.col("TIME_TO_SUCCESS_DAYS").max().alias(f"{p}MAX_TIME_TO_SUCCESS_DAYS"),
        pl.col("ACTIVE_LIFESPAN_DAYS").mean().alias(f"{p}AVG_ACTIVE_LIFESPAN_DAYS"),
        pl.col("ACTIVE_LIFESPAN_DAYS").max().alias(f"{p}LONGEST_ACTIVE_LIFESPAN_DAYS"),
        pl.col("MANDATE_CREATED_AT").min().alias(f"{p}_FIRST_MANDATE_TS"),
        pl.col("MANDATE_CREATED_AT").max().alias(f"{p}_LAST_MANDATE_TS"),
        pl.col("LAST_EVENT_AT").max().alias(f"{p}_LAST_EVENT_TS"),
        # Reason flags
        pl.col("IS_USER_DRIVEN_REASON").max().alias(f"{p}HAS_ANY_USER_DRIVEN_REASON"),
        pl.col("IS_FAILURE_REASON").max().alias(f"{p}HAS_ANY_FAILURE_REASON"),
        pl.col("IS_CLOSURE_REASON").max().alias(f"{p}HAS_ANY_CLOSURE_REASON"),
        # Bank signals
        pl.col("BANK_NAME").drop_nulls().n_unique().alias(f"{p}UNIQUE_BANKS_TRIED"),
        pl.col("IS_PUBLIC_SECTOR_BANK").max().alias(f"{p}HAS_PUBLIC_SECTOR_BANK"),
        pl.col("PROVIDER").drop_nulls().n_unique().alias(f"{p}UNIQUE_PROVIDERS_USED"),
        # Event timing
        pl.col("RAPID_SUCCESSION_EVENTS")
        .sum()
        .alias(f"{p}TOTAL_RAPID_SUCCESSION_EVENTS"),
        pl.col("OFF_HOURS_EVENTS").sum().alias(f"{p}TOTAL_OFF_HOURS_EVENTS"),
        # Lifecycle
        pl.col("MANDATE_OUTLIVES_LOAN").max().alias(f"{p}ANY_MANDATE_OUTLIVES_LOAN"),
        pl.col("MANDATE_PRE_DISBURSAL").max().alias(f"{p}ANY_PRE_DISBURSAL_MANDATE"),
        pl.col("MANDATE_AMOUNT").max().alias(f"{p}MAX_MANDATE_AMOUNT"),
        # META
        pl.col("META_HAS_ACTIVATION_DEBIT").max().alias(f"{p}HAS_ACTIVATION_DEBIT"),
        pl.col("META_ACTIVATION_REFUND_REQUIRED")
        .max()
        .alias(f"{p}ACTIVATION_REFUND_NEEDED"),
        pl.col("META_HAS_VPA").max().alias(f"{p}HAS_VPA_REGISTERED"),
        pl.col("META_HAS_UMRN").max().alias(f"{p}HAS_UMRN_REGISTERED"),
        (pl.col("META_AUTH_SUB_MODE") == "aadhaar")
        .sum()
        .alias(f"{p}AUTH_COUNT_AADHAAR"),
        (pl.col("META_AUTH_SUB_MODE") == "debit").sum().alias(f"{p}AUTH_COUNT_DEBIT"),
        (pl.col("META_AUTH_SUB_MODE") == "net_banking")
        .sum()
        .alias(f"{p}AUTH_COUNT_NET_BANKING"),
        # Account type
        ((pl.col("ACCOUNT_TYPE") == "savings").sum() > 0)
        .cast(pl.Int8)
        .alias(f"{p}ACCOUNT_TYPE_SAVINGS"),
        ((pl.col("ACCOUNT_TYPE") == "current").sum() > 0)
        .cast(pl.Int8)
        .alias(f"{p}ACCOUNT_TYPE_CURRENT"),
        # Active log
        pl.col("ACTIVE_LOG_WINDOW_COUNT").sum().alias(f"{p}TOTAL_ACTIVE_LOG_WINDOWS"),
        pl.col("ACTIVE_LOG_TOTAL_PLANNED_DAYS")
        .max()
        .alias(f"{p}MAX_ACTIVE_LOG_PLANNED_DAYS"),
        pl.col("ACTIVE_LOG_GAP_DAYS").sum().alias(f"{p}TOTAL_ACTIVE_LOG_GAP_DAYS"),
        # Reason bucket counts
        *[
            (pl.col("REASON_BUCKET") == b).sum().alias(f"{p}REASON_CNT_{b}")
            for b in REASON_BUCKETS
        ],
    ]


def loan_agg_polars(
    df: pl.DataFrame,  # raw event frame
    ref: pl.DataFrame,  # reference-level features (one row per LOAN_ID + CUTOFF_DATE + REFERENCE_ID)
) -> pl.DataFrame:
    post_cancel_cols = [
        f"POST_CANCEL_{b}" for b in REASON_BUCKETS if f"POST_CANCEL_{b}" in ref.columns
    ]

    # ---- Part 1: sweep-line simultaneous overlap (Python, per loan) ----
    print("  Computing simultaneous mandate overlap ...", flush=True)
    overlap_rows = []
    for loan_key, group in ref.group_by(LOAN_KEY_COLS):
        if isinstance(loan_key, tuple):
            loan_id_val, cutoff_date_val = loan_key
        else:
            loan_id_val, cutoff_date_val = loan_key, None
        rows = group.to_dicts()
        intervals, mode_ivs = [], {m: [] for m in MODE_KEYS}
        active_ivs, active_mode_ivs = [], {m: [] for m in MODE_KEYS}
        for r in rows:
            s = r.get("MANDATE_CREATED_AT")
            e = (
                r.get("MANDATE_END_AT")
                or r.get("LAST_EVENT_AT")
                or r.get("CUTOFF_DATE")
            )
            intervals.append((s, e))
            m = r.get("MODE")
            if m in mode_ivs:
                mode_ivs[m].append((s, e))
            sa = r.get("SUCCESS_AT")
            if sa:
                active_ivs.append((sa, e))
                if m in active_mode_ivs:
                    active_mode_ivs[m].append((sa, e))
        overlap_rows.append(
            {
                "LOAN_ID": loan_id_val,
                "CUTOFF_DATE": cutoff_date_val,
                "MAX_SIMULTANEOUS_MANDATES": _max_overlap(intervals),
                **{
                    f"MAX_SIMULTANEOUS_{m}_MANDATES": _max_overlap(mode_ivs[m])
                    for m in MODE_KEYS
                },
                "MAX_SIMULTANEOUS_ACTIVE_MANDATES": _max_overlap(active_ivs),
                **{
                    f"MAX_SIMULTANEOUS_ACTIVE_{m}": _max_overlap(active_mode_ivs[m])
                    for m in MODE_KEYS
                },
            }
        )
    overlap_df = pl.DataFrame(overlap_rows)

    # ---- Part 2: reference ID lists per loan ----
    ref_id_df = (
        ref.sort(
            [*LOAN_KEY_COLS, "MANDATE_CREATED_AT"], descending=[False, False, False]
        )
        .group_by(LOAN_KEY_COLS)
        .agg(
            [
                pl.col("REFERENCE_ID")
                .cast(pl.Utf8)
                .sort()
                .str.join(",")
                .alias("REFERENCE_IDS"),
                pl.col("REFERENCE_ID")
                .sort_by("MANDATE_CREATED_AT")
                .last()
                .alias("LATEST_REFERENCE_ID"),
            ]
        )
    )

    # ---- Part 3: cross-mode loan-cutoff-level summary ----
    print("  Aggregating cross-mode loan-cutoff-level features ...", flush=True)

    loans = ref.group_by(LOAN_KEY_COLS).agg(
        [
            pl.len().alias("TOTAL_REFERENCE_COUNT"),
            # Pass-through identifier columns
            *(
                [pl.col("USER_ID").drop_nulls().first().alias("USER_ID")]
                if "USER_ID" in ref.columns
                else []
            ),
            pl.col("LOAN_STATUS").drop_nulls().last().alias("LATEST_LOAN_STATUS"),
            pl.col("HAS_SUCCESS").sum().alias("TOTAL_SUCCESSFUL_ATTEMPTS"),
            pl.col("FAILED_BEFORE_SUCCESS").sum().alias("TOTAL_FAILED_ATTEMPTS"),
            pl.col("HAS_SUCCESS").mean().alias("OVERALL_SUCCESS_RATE"),
            # Mode diversity
            pl.col("MODE").drop_nulls().n_unique().alias("UNIQUE_MODES_TRIED"),
            pl.col("MODE")
            .drop_nulls()
            .n_unique()
            .gt(1)
            .cast(pl.Int8)
            .alias("HAS_MULTI_MODE_HISTORY"),
            # Mandate count per mode (how many references per mode across loan lifetime)
            *[
                (pl.col("MODE") == m).sum().cast(pl.Int32).alias(f"{m}_REFERENCE_COUNT")
                for m in MODE_KEYS
            ],
            # Timing anchors (loan-cutoff-level)
            pl.col("MANDATE_CREATED_AT").min().alias("_FIRST_MANDATE_TS"),
            pl.col("MANDATE_CREATED_AT").max().alias("_LAST_MANDATE_TS"),
            pl.col("LAST_EVENT_AT").max().alias("_LAST_EVENT_TS"),
            pl.col("LOAN_DISBURSED_DATE")
            .drop_nulls()
            .min()
            .alias("LOAN_DISBURSED_DATE"),
            pl.col("ACTUAL_LOAN_END_DATE")
            .drop_nulls()
            .min()
            .alias("ACTUAL_LOAN_END_DATE"),
            pl.col("DISBURSAL_TO_MANDATE_DAYS")
            .drop_nulls()
            .first()
            .alias("DAYS_MANDATE_VS_DISBURSAL"),
        ]
    )

    loans = loans.with_columns(
        [
            pl.col("TOTAL_REFERENCE_COUNT").alias("MANDATE_ATTEMPT_COUNT"),
            (
                (
                    pl.col("_LAST_MANDATE_TS") - pl.col("_FIRST_MANDATE_TS")
                ).dt.total_seconds()
                / 86400.0
            )
            .clip(lower_bound=0)
            .alias("MANDATE_CREATION_SPAN_DAYS"),
            (
                (pl.col("CUTOFF_DATE") - pl.col("_LAST_EVENT_TS")).dt.total_seconds()
                / 86400.0
            )
            .clip(lower_bound=0)
            .alias("DAYS_SINCE_LAST_EVENT"),
            (
                (
                    pl.col("ACTUAL_LOAN_END_DATE") - pl.col("LOAN_DISBURSED_DATE")
                ).dt.total_seconds()
                / 86400.0
            )
            .clip(lower_bound=0)
            .alias("LOAN_TENURE_DAYS"),
        ]
    ).drop(["_FIRST_MANDATE_TS", "_LAST_MANDATE_TS", "_LAST_EVENT_TS"])

    # CANCELLED_ATTEMPTS_BEFORE_FIRST_SUCCESS (cross-mode — any mode)
    first_success_ts = (
        ref.filter(pl.col("HAS_SUCCESS") == 1)
        .group_by(LOAN_KEY_COLS)
        .agg(pl.col("SUCCESS_AT").min().alias("FIRST_SUCCESS_TS"))
    )
    cafs = (
        ref.join(first_success_ts, on=LOAN_KEY_COLS, how="left")
        .with_columns(
            [
                (
                    (pl.col("FAILED_BEFORE_SUCCESS") == 1)
                    & (
                        pl.col("FIRST_SUCCESS_TS").is_null()
                        | (pl.col("MANDATE_CREATED_AT") < pl.col("FIRST_SUCCESS_TS"))
                    )
                )
                .cast(pl.Int8)
                .alias("_cafs"),
                (
                    (pl.col("POST_CANCEL_PROBLEM_EVENTS") > 0)
                    & (
                        pl.col("FIRST_SUCCESS_TS").is_null()
                        | (pl.col("MANDATE_CREATED_AT") < pl.col("FIRST_SUCCESS_TS"))
                    )
                )
                .cast(pl.Int8)
                .alias("_prob_cafs"),
            ]
        )
        .group_by(LOAN_KEY_COLS)
        .agg(
            [
                pl.col("_cafs").sum().alias("CANCELLED_ATTEMPTS_BEFORE_FIRST_SUCCESS"),
                pl.col("_prob_cafs")
                .sum()
                .alias("PROBLEM_CANCELLED_BEFORE_FIRST_SUCCESS"),
            ]
        )
    )

    # ---- Part 4: per-mode stratified aggregation ----
    print("  Aggregating per-mode features ...", flush=True)
    mode_dfs = {}
    for mode in MODE_KEYS:
        prefix = f"{mode}__"
        mode_ref = ref.filter(pl.col("MODE") == mode)
        if mode_ref.is_empty():
            # produce a zero-filled skeleton so joins don't drop loans
            dummy_exprs = _mode_agg_exprs(prefix, post_cancel_cols)
            # build from full ref to get the LOAN_ID universe then fill nulls
            mode_df = ref.select(LOAN_KEY_COLS).unique()
            for expr in dummy_exprs:
                alias = expr.meta.output_name()
                dtype = pl.Float64 if "DAYS" in alias or "RATE" in alias else pl.Int32
                mode_df = mode_df.with_columns(pl.lit(None).cast(dtype).alias(alias))
        else:
            mode_df = mode_ref.group_by(LOAN_KEY_COLS).agg(
                _mode_agg_exprs(prefix, post_cancel_cols)
            )
            # Derived timing columns for this mode
            ts_cols_present = all(
                f"{prefix}{c}" in mode_df.columns
                for c in ["_FIRST_MANDATE_TS", "_LAST_MANDATE_TS"]
            )
            if ts_cols_present:
                mode_df = mode_df.with_columns(
                    [
                        (
                            (
                                pl.col(f"{prefix}_LAST_MANDATE_TS")
                                - pl.col(f"{prefix}_FIRST_MANDATE_TS")
                            ).dt.total_seconds()
                            / 86400.0
                        )
                        .clip(lower_bound=0)
                        .alias(f"{prefix}MANDATE_CREATION_SPAN_DAYS"),
                        (
                            (
                                pl.col("CUTOFF_DATE")
                                - pl.col(f"{prefix}_LAST_EVENT_TS")
                            ).dt.total_seconds()
                            / 86400.0
                        )
                        .clip(lower_bound=0)
                        .alias(f"{prefix}DAYS_SINCE_LAST_EVENT"),
                    ]
                ).drop(
                    [
                        f"{prefix}_FIRST_MANDATE_TS",
                        f"{prefix}_LAST_MANDATE_TS",
                        f"{prefix}_LAST_EVENT_TS",
                    ]
                )

            # MIN_ACTIVE_TO_CANCEL_DAYS for this mode
            min_atc_mode = (
                mode_ref.filter(
                    pl.col("SUCCESS_AT").is_not_null()
                    & pl.col("CANCEL_AT").is_not_null()
                    & (pl.col("CANCEL_AT") > pl.col("SUCCESS_AT"))
                )
                .with_columns(
                    (
                        (pl.col("CANCEL_AT") - pl.col("SUCCESS_AT")).dt.total_seconds()
                        / 86400.0
                    ).alias("_ATC")
                )
                .group_by(LOAN_KEY_COLS)
                .agg(pl.col("_ATC").min().alias(f"{prefix}MIN_ACTIVE_TO_CANCEL_DAYS"))
            )
            mode_df = mode_df.join(min_atc_mode, on=LOAN_KEY_COLS, how="left")

        mode_dfs[mode] = mode_df

    # Raw event count per loan
    raw_event_counts = df.group_by(LOAN_KEY_COLS).agg(pl.len().alias("RAW_EVENT_COUNT"))

    # ---- Part 5: assemble ----
    result = loans

    # Join mode-specific dataframes with unique suffixes to avoid duplicate column errors
    for i, mode in enumerate(MODE_KEYS):
        # Use unique temporary suffix for each join
        temp_suffix = f"_mode{i}_"
        result = result.join(
            mode_dfs[mode], on=LOAN_KEY_COLS, how="left", suffix=temp_suffix
        )
        # Remove the temporary suffix from column names
        rename_map = {
            col: col.replace(temp_suffix, "")
            for col in result.columns
            if temp_suffix in col
        }
        if rename_map:
            result = result.rename(rename_map)

    # Join other dataframes with unique suffixes
    for i, (df_to_join, name) in enumerate(
        [
            (cafs, "cafs"),
            (overlap_df, "overlap"),
            (raw_event_counts, "raw"),
            (ref_id_df, "ref"),
        ]
    ):
        temp_suffix = f"_{name}{i}_"
        result = result.join(
            df_to_join, on=LOAN_KEY_COLS, how="left", suffix=temp_suffix
        )
        # Remove the temporary suffix from column names
        rename_map = {
            col: col.replace(temp_suffix, "")
            for col in result.columns
            if temp_suffix in col
        }
        if rename_map:
            result = result.rename(rename_map)

    result = result.sort(LOAN_KEY_COLS)

    return result


# ---------------------------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------------------------


def get_checkpoint_path(run_id: str) -> Path:
    """Get the checkpoint file path for a given run ID."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"checkpoint_{run_id}.pkl"


def get_features_path(run_id: str) -> Path:
    """Get the final combined features output file path for a given run ID."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"features_{run_id}.parquet"


def get_chunk_path(run_id: str, chunk_num: int) -> Path:
    """Get the per-chunk parquet file path."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"chunk_{run_id}_{chunk_num:05d}.parquet"


def load_chunk_files(run_id: str) -> list[Path]:
    """Return sorted list of already-saved chunk parquet files for this run."""
    return sorted(CHECKPOINT_DIR.glob(f"chunk_{run_id}_*.parquet"))


def clear_chunk_files(run_id: str):
    """Remove all per-chunk parquet files after a successful combine."""
    for p in load_chunk_files(run_id):
        p.unlink()


def save_checkpoint(
    run_id: str,
    chunk_index: int,
    processed_keys: list,
    total_chunks: int,
    output_table: str,
    features: pl.DataFrame | None = None,
):
    """Save checkpoint state to disk. Optionally saves features when all chunks complete."""
    if not CHECKPOINT_ENABLED:
        return

    checkpoint_data = {
        "run_id": run_id,
        "chunk_index": chunk_index,
        "processed_keys": processed_keys,
        "total_chunks": total_chunks,
        "output_table": output_table,
        "timestamp": datetime.now().isoformat(),
    }
    checkpoint_path = get_checkpoint_path(run_id)
    with open(checkpoint_path, "wb") as f:
        pickle.dump(checkpoint_data, f)

    # Save features to parquet when all chunks are complete
    if features is not None and chunk_index >= total_chunks:
        features_path = get_features_path(run_id)
        features.write_parquet(features_path)
        print(
            f"  ✓ Features saved: {features_path.name} ({features.shape[0]:,} rows)",
            flush=True,
        )

    print(
        f"  ✓ Checkpoint saved: chunk {chunk_index}/{total_chunks} "
        f"({len(processed_keys)} combinations processed)",
        flush=True,
    )


def load_checkpoint(run_id: str, output_table: str | None = None) -> dict | None:
    """Load checkpoint state from disk. Returns None if no checkpoint exists."""
    if not CHECKPOINT_ENABLED:
        return None

    checkpoint_path = get_checkpoint_path(run_id)
    if not checkpoint_path.exists():
        return None

    try:
        with open(checkpoint_path, "rb") as f:
            checkpoint_data = pickle.load(f)
        if (
            output_table is not None
            and checkpoint_data.get("output_table") != output_table
        ):
            print(
                f"  ⚠ Ignoring checkpoint {checkpoint_path.name}: "
                f"table '{checkpoint_data.get('output_table')}' does not match "
                f"current table '{output_table}'.",
                flush=True,
            )
            return None
        print(
            f"  ✓ Checkpoint loaded: chunk {checkpoint_data['chunk_index']}/{checkpoint_data['total_chunks']} "
            f"({len(checkpoint_data['processed_keys'])} combinations already processed)",
            flush=True,
        )
        return checkpoint_data
    except Exception as e:
        print(f"  ⚠ Warning: Failed to load checkpoint: {e}", flush=True)
        return None


def clear_checkpoint(run_id: str):
    """Remove checkpoint and features files after successful completion."""
    if not CHECKPOINT_ENABLED:
        return

    checkpoint_path = get_checkpoint_path(run_id)
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        print(f"  ✓ Checkpoint cleared: {checkpoint_path.name}", flush=True)

    features_path = get_features_path(run_id)
    if features_path.exists():
        features_path.unlink()
        print(f"  ✓ Features file cleared: {features_path.name}", flush=True)


def generate_run_id() -> str:
    """Generate a unique run ID based on timestamp."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def get_latest_checkpoint(output_table: str | None = None) -> dict | None:
    """Find and load the most recent matching checkpoint file."""
    if not CHECKPOINT_ENABLED or not CHECKPOINT_DIR.exists():
        return None

    checkpoint_files = list(CHECKPOINT_DIR.glob("checkpoint_*.pkl"))
    if not checkpoint_files:
        return None

    # Sort by modification time (most recent first)
    checkpoint_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

    for checkpoint_file in checkpoint_files:
        try:
            with open(checkpoint_file, "rb") as f:
                checkpoint_data = pickle.load(f)
            if (
                output_table is not None
                and checkpoint_data.get("output_table") != output_table
            ):
                continue

            run_id = checkpoint_file.stem.replace("checkpoint_", "")
            checkpoint_data["run_id"] = run_id
            print(
                f"  ✓ Latest checkpoint found: {checkpoint_file.name} "
                f"(chunk {checkpoint_data['chunk_index']}/{checkpoint_data['total_chunks']})",
                flush=True,
            )
            return checkpoint_data
        except Exception as e:
            print(
                f"  ⚠ Warning: Failed to load checkpoint {checkpoint_file.name}: {e}",
                flush=True,
            )

    return None


# ---------------------------------------------------------------------------
# Chunked feature engineering
# ---------------------------------------------------------------------------


def get_loan_cutoff_keys(df: pl.DataFrame) -> pl.DataFrame:
    """Get unique loan_id/cutoff_date combinations from the dataframe."""
    return df.select(LOAN_KEY_COLS).unique().sort(LOAN_KEY_COLS)


def filter_df_by_keys(df: pl.DataFrame, keys: pl.DataFrame) -> pl.DataFrame:
    """Filter dataframe to only include rows matching the given loan_id/cutoff_date keys."""
    return df.join(keys, on=LOAN_KEY_COLS, how="inner")


def engineer_features_chunk(df_chunk: pl.DataFrame) -> pl.DataFrame:
    """
    Process a chunk of data through the feature engineering pipeline.
    This is the core processing logic extracted from the original engineer_features function.
    """
    required = {
        "LOAN_ID",
        "CUTOFF_DATE",
        "REFERENCE_ID",
        "HISTORY_STATUS",
        "HISTORY_UPDATED_AT",
    }
    missing = required.difference(df_chunk.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df_chunk = add_reason_bucket(df_chunk)
    df_chunk = add_meta_signals(df_chunk)
    df_chunk = add_event_timing(df_chunk)

    ref = ref_agg_polars(df_chunk)
    ref = ref_python_pass(ref)

    features = loan_agg_polars(df_chunk, ref)

    # Replace inf with null
    features = features.with_columns(
        [
            pl.when(pl.col(c).is_infinite()).then(None).otherwise(pl.col(c)).alias(c)
            for c in features.columns
            if features[c].dtype in (pl.Float32, pl.Float64)
        ]
    )

    return features


def engineer_features_chunked(
    df: pl.DataFrame,
    output_table: str,
    run_id: str | None = None,
    resume: bool = True,
) -> pl.DataFrame:
    """
    Process features in chunks with checkpointing support.

    Args:
        df: Full dataframe with all raw data
        output_table: Snowflake output table name
        run_id: Optional run ID for checkpointing. If None, generates a new one.
        resume: Whether to resume from checkpoint if available

    Returns:
        Combined feature dataframe with all chunks processed
    """
    if run_id is None:
        run_id = generate_run_id()

    # Get all unique loan_id/cutoff_date combinations
    all_keys = get_loan_cutoff_keys(df)
    total_combinations = all_keys.shape[0]
    total_chunks = (total_combinations + CHUNK_SIZE - 1) // CHUNK_SIZE

    print(f"\n{'='*60}", flush=True)
    print("Chunked Feature Engineering Pipeline", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Total loan/cutoff combinations: {total_combinations:,}", flush=True)
    print(f"  Chunk size: {CHUNK_SIZE:,}", flush=True)
    print(f"  Total chunks: {total_chunks}", flush=True)
    print(f"  Run ID: {run_id}", flush=True)
    print(f"  Checkpoint enabled: {CHECKPOINT_ENABLED}", flush=True)
    print(f"{'='*60}\n", flush=True)

    # Check for existing checkpoint
    start_chunk = 0
    processed_keys = []
    checkpoint = load_checkpoint(run_id, output_table=output_table) if resume else None

    if checkpoint:
        start_chunk = checkpoint["chunk_index"]
        processed_keys = checkpoint["processed_keys"]
        print(f"  Resuming from chunk {start_chunk + 1}/{total_chunks}\n", flush=True)

    # Filter out already processed keys
    if processed_keys:
        processed_df = pl.DataFrame(processed_keys)
        # Cast each key column to match all_keys schema — the saved checkpoint may
        # have been written with a different dtype (e.g. datetime[μs] vs Date).
        processed_df = processed_df.with_columns(
            [
                pl.col(col).cast(all_keys.schema[col])
                for col in LOAN_KEY_COLS
                if col in processed_df.columns
                and processed_df.schema[col] != all_keys.schema[col]
            ]
        )
        remaining_keys = all_keys.join(processed_df, on=LOAN_KEY_COLS, how="anti")
    else:
        remaining_keys = all_keys

    if remaining_keys.shape[0] == 0:
        print("  ✓ All combinations already processed!", flush=True)
        # Try final combined parquet first
        features_path = get_features_path(run_id)
        if features_path.exists():
            print(f"  Loading saved features from {features_path.name}...", flush=True)
            combined_features = pl.read_parquet(features_path)
            print(
                f"  ✓ Loaded: {combined_features.shape[0]:,} rows, "
                f"{combined_features.shape[1]} features",
                flush=True,
            )
            return combined_features
        # Fall back: combine from individual chunk parquet files
        chunk_files = load_chunk_files(run_id)
        if chunk_files:
            print(f"  Combining {len(chunk_files)} saved chunk files...", flush=True)
            return _combine_chunk_files(
                chunk_files, run_id, processed_keys, total_chunks, output_table
            )
        print("  ⚠ No saved chunk or features files found.", flush=True)
        print("  Run with --no-resume to re-process all data.", flush=True)
        return pl.DataFrame()

    # Process chunks
    all_features = []
    chunk_start_time = time.time()

    for chunk_idx in range(start_chunk, total_chunks):
        chunk_start_idx = (chunk_idx - start_chunk) * CHUNK_SIZE
        chunk_end_idx = min(chunk_start_idx + CHUNK_SIZE, remaining_keys.shape[0])

        if chunk_start_idx >= remaining_keys.shape[0]:
            break

        chunk_keys = remaining_keys.slice(
            chunk_start_idx, chunk_end_idx - chunk_start_idx
        )
        chunk_num = chunk_idx + 1

        print(f"\n{'─'*60}", flush=True)
        print(
            f"Processing chunk {chunk_num}/{total_chunks} "
            f"({chunk_keys.shape[0]:,} combinations)",
            flush=True,
        )
        print(f"{'─'*60}", flush=True)

        # Filter data for this chunk
        chunk_df = filter_df_by_keys(df, chunk_keys)

        # Process the chunk
        try:
            chunk_features = engineer_features_chunk(chunk_df)

            # Persist this chunk to disk immediately so it survives crashes
            chunk_path = get_chunk_path(run_id, chunk_num)
            chunk_features.write_parquet(chunk_path)

            all_features.append(chunk_features)

            # Update processed keys
            chunk_key_dicts = chunk_keys.to_dicts()
            processed_keys.extend(chunk_key_dicts)

            # Save checkpoint
            save_checkpoint(
                run_id=run_id,
                chunk_index=chunk_num,
                processed_keys=processed_keys,
                total_chunks=total_chunks,
                output_table=output_table,
            )

            chunk_elapsed = time.time() - chunk_start_time
            print(
                f"  ✓ Chunk {chunk_num} complete: {chunk_features.shape[0]:,} rows, "
                f"{chunk_features.shape[1]} features ({chunk_elapsed:.1f}s)",
                flush=True,
            )
            chunk_start_time = time.time()

        except Exception as e:
            print(f"  ✗ Error processing chunk {chunk_num}: {e}", flush=True)
            print(
                f"  Checkpoint saved. You can resume with run_id={run_id}", flush=True
            )
            raise

    # Combine all chunks (in-memory list built during this run)
    if all_features:
        return _combine_chunk_files(
            load_chunk_files(run_id), run_id, processed_keys, total_chunks, output_table
        )
    return pl.DataFrame()


def _combine_chunk_files(
    chunk_files: list,
    run_id: str,
    processed_keys: list,
    total_chunks: int,
    output_table: str,
) -> pl.DataFrame:
    """Read per-chunk parquet files, normalise schemas, concat, persist final result."""
    print(f"\n{'='*60}", flush=True)
    print(f"Combining {len(chunk_files)} chunk files...", flush=True)

    dfs = [pl.read_parquet(p) for p in chunk_files]

    # Normalise schemas to the first chunk so UInt32/Int32 mismatches don't error
    ref_schema = dfs[0].schema
    normalised = []
    for feat_df in dfs:
        casts = [
            pl.col(c).cast(ref_schema[c])
            for c in feat_df.columns
            if c in ref_schema and feat_df.schema[c] != ref_schema[c]
        ]
        normalised.append(feat_df.with_columns(casts) if casts else feat_df)

    combined_features = pl.concat(normalised, how="vertical")
    print(
        f"  ✓ Combined: {combined_features.shape[0]:,} rows, "
        f"{combined_features.shape[1]} features",
        flush=True,
    )
    print(f"{'='*60}\n", flush=True)

    # Persist the final combined parquet
    save_checkpoint(
        run_id=run_id,
        chunk_index=total_chunks,
        processed_keys=processed_keys,
        total_chunks=total_chunks,
        output_table=output_table,
        features=combined_features,
    )

    # Clean up individual chunk files now that the combined file is saved
    clear_chunk_files(run_id)

    return combined_features


# ---------------------------------------------------------------------------
# Main pipeline (original - for backward compatibility)
# ---------------------------------------------------------------------------


def engineer_features(df: pl.DataFrame) -> pl.DataFrame:
    """Original non-chunked feature engineering (for backward compatibility)."""
    required = {
        "LOAN_ID",
        "CUTOFF_DATE",
        "REFERENCE_ID",
        "HISTORY_STATUS",
        "HISTORY_UPDATED_AT",
    }
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    print("Step 1/4 — Classifying cancellation reasons ...", flush=True)
    df = add_reason_bucket(df)

    print("Step 2/4 — Extracting META signals ...", flush=True)
    df = add_meta_signals(df)
    df = add_event_timing(df)

    print("Step 3/4 — Building reference-level features ...", flush=True)
    ref = ref_agg_polars(df)
    ref = ref_python_pass(ref)

    print(f"  → {ref.shape[0]:,} mandate references", flush=True)

    print("Step 4/4 — Building loan-cutoff-level features ...", flush=True)
    features = loan_agg_polars(df, ref)

    print(
        f"  → {features.shape[0]:,} loan-cutoff rows, {features.shape[1]} features",
        flush=True,
    )

    # Replace inf with null
    features = features.with_columns(
        [
            pl.when(pl.col(c).is_infinite()).then(None).otherwise(pl.col(c)).alias(c)
            for c in features.columns
            if features[c].dtype in (pl.Float32, pl.Float64)
        ]
    )

    return features


# ---------------------------------------------------------------------------
# Sample report
# ---------------------------------------------------------------------------


def print_sample_report(features: pl.DataFrame, sample_size: int):
    cols = [
        "LOAN_ID",
        "CUTOFF_DATE",
        "REFERENCE_IDS",
        "LATEST_REFERENCE_ID",
        "MANDATE_ATTEMPT_COUNT",
        "UNIQUE_MODES_TRIED",
        "HAS_MULTI_MODE_HISTORY",
        "UPI__REFERENCE_COUNT",
        "ENACH__REFERENCE_COUNT",
        "PNACH__REFERENCE_COUNT",
        "TOTAL_SUCCESSFUL_ATTEMPTS",
        "OVERALL_SUCCESS_RATE",
        "CANCELLED_ATTEMPTS_BEFORE_FIRST_SUCCESS",
        "PROBLEM_CANCELLED_BEFORE_FIRST_SUCCESS",
        "UPI__PRE_SUCCESS_RETRY_EVENTS_TOTAL",
        "ENACH__PRE_SUCCESS_RETRY_EVENTS_TOTAL",
        "UPI__POST_CANCEL_PROBLEM_TOTAL",
        "ENACH__POST_CANCEL_PROBLEM_TOTAL",
        "MAX_SIMULTANEOUS_MANDATES",
        "MAX_SIMULTANEOUS_UPI_MANDATES",
        "MAX_SIMULTANEOUS_ACTIVE_MANDATES",
        "UPI__AVG_TIME_TO_SUCCESS_DAYS",
        "ENACH__AVG_TIME_TO_SUCCESS_DAYS",
        "UPI__MIN_ACTIVE_TO_CANCEL_DAYS",
        "ENACH__MIN_ACTIVE_TO_CANCEL_DAYS",
    ]
    available = [c for c in cols if c in features.columns]
    sample = (
        features.sort(
            [
                "CANCELLED_ATTEMPTS_BEFORE_FIRST_SUCCESS",
                "PROBLEM_CANCELLED_BEFORE_FIRST_SUCCESS",
                "LOAN_ID",
                "CUTOFF_DATE",
            ],
            descending=[True, True, False, False],
        )
        .head(sample_size)
        .select(available)
    )
    # Format CUTOFF_DATE as YYYY-MM-DD string for readability
    if "CUTOFF_DATE" in sample.columns:
        sample = sample.with_columns(
            pl.col("CUTOFF_DATE").dt.strftime("%Y-%m-%d").alias("CUTOFF_DATE")
        )
    print(sample)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    args = parse_args()
    apply_warehouse_override(args.warehouse)

    # Handle checkpoint clearing
    if args.clear_checkpoint:
        if args.run_id:
            clear_checkpoint(args.run_id)
            print(f"Checkpoint cleared for run_id={args.run_id}", flush=True)
        else:
            print("Error: --clear-checkpoint requires --run-id", flush=True)
        return

    # Load data
    df = load_data(args.csv)

    # Override chunk size if provided via CLI
    global CHUNK_SIZE
    if args.chunk_size is not None:
        CHUNK_SIZE = args.chunk_size
        print(f"Using chunk size from CLI: {CHUNK_SIZE}", flush=True)

    # Process features
    if args.chunked:
        # Use chunked processing with checkpointing
        run_id = args.run_id

        # If no run_id provided and resume is enabled, try to use latest checkpoint
        if run_id is None and not args.no_resume:
            latest_checkpoint = get_latest_checkpoint(args.snowflake_table)
            if latest_checkpoint:
                run_id = latest_checkpoint["run_id"]
                print(f"  → Using latest checkpoint run_id: {run_id}", flush=True)

        features = engineer_features_chunked(
            df=df,
            output_table=args.snowflake_table,
            run_id=run_id,
            resume=not args.no_resume,
        )
    else:
        # Use original non-chunked processing
        features = engineer_features(df)

    if features.is_empty():
        print("No features generated.", flush=True)
        return

    print(
        f"\nGenerated {features.shape[0]:,} loan-cutoff-level rows "
        f"with {features.shape[1]} features."
    )

    if args.no_upload:
        print("--no-upload set, skipping Snowflake write.", flush=True)
    else:
        from db_service import upload_to_snowflake

        print(f"Uploading to Snowflake: {args.snowflake_table} ...", flush=True)

        # Convert date-only columns to YYYY-MM-DD strings in pandas.
        # write_pandas auto_create_table maps pandas datetime64 → TIMESTAMP, which
        # is wrong for date-only columns.  Using formatted strings lets us then
        # ALTER the columns to DATE after the table is created.
        date_only_cols = ["CUTOFF_DATE", "LOAN_DISBURSED_DATE", "ACTUAL_LOAN_END_DATE"]
        pdf = features.with_columns(
            [
                pl.col(col).cast(pl.Date).dt.strftime("%Y-%m-%d").alias(col)
                for col in date_only_cols
                if col in features.columns
            ]
        ).to_pandas()

        if args.chunked:
            # Use replace on a fresh run so stale table schemas don't cause
            # column mismatch errors; use append only when genuinely resuming.
            upload_mode = "replace" if args.no_resume else "append"
            upload_to_snowflake(pdf, args.snowflake_table, if_exists=upload_mode)
        else:
            upload_to_snowflake(pdf, args.snowflake_table, if_exists="replace")

        # After upload, ALTER the string columns to proper DATE type in Snowflake
        try:
            from db_service import execute_query

            parts = args.snowflake_table.split(".")
            fq = args.snowflake_table  # fully-qualified
            for col in date_only_cols:
                if col in features.columns:
                    execute_query(
                        f"ALTER TABLE {fq} ALTER COLUMN {col} SET DATA TYPE DATE "
                        f"USING TO_DATE({col}, 'YYYY-MM-DD')"
                    )
            print("  ✓ Date columns cast to DATE in Snowflake.", flush=True)
        except Exception as alter_err:
            print(f"  ⚠ Could not ALTER date columns: {alter_err}", flush=True)
            print(
                "  Tip: Run manually: ALTER TABLE {fq} ALTER COLUMN CUTOFF_DATE SET DATA TYPE DATE USING TO_DATE(CUTOFF_DATE, 'YYYY-MM-DD')",
                flush=True,
            )

        # Clear checkpoint after successful upload
        if args.chunked and args.run_id:
            clear_checkpoint(args.run_id)

    if args.sample_report:
        print_sample_report(features, args.sample_size)


if __name__ == "__main__":
    main()
