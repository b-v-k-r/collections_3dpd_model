"""

python /Users/vamsi.k/Documents/Collection Models/collections_3dpd_model/model_trainer.py \
  --cache-parquet /Users/vamsi.k/Documents/Collection Models/collections_3dpd_model/model_outputs/combined_features.parquet \
  --hpo-objective ks_at_coverage \
  --hpo-coverage 0.50 \
  --n-trials 50 \
  --hpo-subsample 0.3 \
  --n-cv-folds 2 \
  --top-n-features 150

  

model_trainer.py
================
Optuna-tuned XGBoost binary classifier for early DPD risk detection.

Target   : TARGET_RISK_BUCKET_3D  (0 = safe, 1 = risky, -1 = no future data → dropped)
Data src : analytics.data_science.early_dpd3_combined_features_part{1..9}
           (built by data_creator.py — base + SMS + in-app + activity
            + ledger + bureau + transactional + renewal + aa + ai_calling
            + legal_automation)

Pipeline
--------
1.  Fetch data from Snowflake.
2.  Drop ID / leakage / metadata columns.
3.  Preprocess: encode categoricals, fill nulls.
4.  Temporal train / val / test split on CUTOFF_DATE (60 / 20 / 20 by chronology).
5.  Feature selection on train set only (no leakage) — combined score of:
      (a) Prior XGBoost gain  — fast model on 30 % of train rows, 100 trees
      (b) Mutual Information  — sklearn mutual_info_classif
      (c) Information Value   — WoE binning (credit-risk standard)
    Scores normalised to [0,1], averaged → top N features selected.
6.  Optuna HPO (TPE + MedianPruner) on the reduced feature set.
    Uses TimeSeriesSplit CV + subsampled HPO data for speed.
7.  Retrain best model on train+val.
8.  Tune event threshold on validation (max F1 with precision floor).
9.  Evaluate on held-out test set, save model artefacts.

Temporal Splits
---------------
  ├── train  (oldest 60%)  ── feature selection + Optuna CV here
  ├── val    (next   20%)  ── early stopping for final model
  └── test   (newest 20%)  ── held-out, evaluated once at the end

Speed Flags
-----------
  --n-trials 50        default; reduce further if still slow
  --timeout 900        hard wall-clock cap (seconds)
  --hpo-subsample 0.4  fraction of train rows used per Optuna trial
  --n-cv-folds 2       default; TimeSeriesSplit folds inside HPO
  --top-n-features 100 features kept after selection (default 100)
  --no-feature-sel     skip feature selection, use all features
  --no-optuna          skip Optuna entirely, use default hyper-params

Usage
-----
  python model_trainer.py
  python model_trainer.py --n-trials 30 --timeout 600
  python model_trainer.py --local-csv data.csv --no-optuna
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import pickle
import sys
import warnings
import time
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)

import matplotlib

matplotlib.use("Agg")  # non-interactive — safe for server / subprocess runs
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# SHAP — graceful fallback if not installed
try:
    import shap

    _SHAP_AVAILABLE = True
except ImportError:
    _SHAP_AVAILABLE = False
    print("Note: SHAP unavailable — pip install shap")

# ---------------------------------------------------------------------------
# Heavy deps
# ---------------------------------------------------------------------------

try:
    import xgboost as xgb
except ImportError:
    raise ImportError("pip install xgboost")

try:
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    raise ImportError("pip install optuna")

# XGBoostPruningCallback — moved to optuna-integration in newer optuna
try:
    from optuna_integration import XGBoostPruningCallback

    _PRUNING_AVAILABLE = True
except ImportError:
    try:
        from optuna.integration import XGBoostPruningCallback

        _PRUNING_AVAILABLE = True
    except ImportError:
        _PRUNING_AVAILABLE = False
        print(
            "Note: Pruning disabled — install with:  "
            "pip install optuna-integration[xgboost]"
        )

try:
    from sklearn.feature_selection import mutual_info_classif
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.preprocessing import LabelEncoder
    from sklearn.metrics import (
        roc_auc_score,
        average_precision_score,
        classification_report,
        confusion_matrix,
        precision_score,
        recall_score,
        f1_score,
    )
except ImportError:
    raise ImportError("pip install scikit-learn")

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COMBINED_FEATURES_TABLES = [
    "analytics.data_science.early_dpd3_combined_features_part1",
    "analytics.data_science.early_dpd3_combined_features_part2",
    "analytics.data_science.early_dpd3_combined_features_part3",
    "analytics.data_science.early_dpd3_combined_features_part4",
    # Transactional features: SMS / bank-account / CC (windowed by 1-7d, 8-15d, 16-30d)
    "analytics.data_science.early_dpd3_combined_features_part5",
    # Renewal DPD features: current-loan performance (30-day lookback, 4 windows)
    "analytics.data_science.early_dpd3_combined_features_part6",
    # Account-Aggregator transaction features (30-day lookback, 3 windows)
    "analytics.data_science.early_dpd3_combined_features_part7",
    # Historical AI-calling features (30-day lookback, 3 windows)
    "analytics.data_science.early_dpd3_combined_features_part8",
    # Historical legal-automation features (30-day lookback, 3 windows)
    "analytics.data_science.early_dpd3_combined_features_part9",
]
MERGE_KEYS = ["USER_ID", "CUTOFF_DATE", "LOAN_ID"]
TARGET_COL = "TARGET_RISK_BUCKET_3D"  # uppercased — matches Snowflake response
DATE_COL = "CUTOFF_DATE"  # temporal ordering key; dropped before training
RANDOM_SEED = 42
OUTPUT_DIR = Path(__file__).parent / "model_outputs"

ID_AND_LEAKAGE_COLS = {
    "LOAN_ID",
    "USER_ID",
    "CUTOFF_DATE",
    "REPAYMENTS_TRANSACTIONS_SPLIT",
    "FUTURE_MAX_DPD",
    "ACTUAL_DPD",  # always 2 by base.sql construction — zero variance
    "TEAM_ALLOCATION",
    "IS_ALLOCATION_EXPERIMENT",
    "SETTLEMENT_MIS_FLAG",
    "LENDER_LEAD_FLAG",  # requested to be dropped
}

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

_NULL_SENTINELS = {"nan", "none", "nat", "<na>", ""}

# Null-rate threshold: columns with more than this fraction missing are dropped
# before feature selection (they can't contribute real signal).
_MAX_NULL_RATE = 0.80

# Suffix groups used for automatic ratio / delta feature engineering.
# Any two features sharing the same prefix but different suffixes get a ratio column.
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
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    p = argparse.ArgumentParser(description="Optuna + XGBoost risk model trainer")
    p.add_argument(
        "--local-csv", metavar="PATH", help="Local CSV — skips Snowflake fetch."
    )
    p.add_argument(
        "--local-parquet",
        metavar="PATH",
        help="Local parquet file — skips Snowflake fetch.",
    )
    p.add_argument(
        "--cache-parquet",
        metavar="PATH",
        help=(
            "Persistent parquet cache for the combined dataset. "
            "If the file exists, load it and skip Snowflake. "
            "Otherwise fetch once from Snowflake and save it here."
        ),
    )
    p.add_argument(
        "--refresh-cache",
        action="store_true",
        help=(
            "Force a fresh Snowflake fetch when using --cache-parquet, "
            "then overwrite the cached parquet."
        ),
    )
    p.add_argument(
        "--n-trials", type=int, default=150, help="Optuna trials (default: 150)."
    )
    p.add_argument(
        "--timeout", type=int, default=None, help="Stop Optuna after N seconds."
    )
    p.add_argument(
        "--n-cv-folds",
        type=int,
        default=4,
        help="TimeSeriesSplit folds inside Optuna (default: 4).",
    )
    p.add_argument(
        "--hpo-subsample",
        type=float,
        default=0.8,
        help="Fraction of train rows per Optuna trial (default: 0.8).",
    )
    p.add_argument(
        "--top-n-features",
        type=int,
        default=50,
        help="Features to keep after selection (default: 50).",
    )
    p.add_argument(
        "--no-feature-sel",
        action="store_true",
        help="Skip feature selection, use all features.",
    )
    p.add_argument(
        "--no-optuna",
        action="store_true",
        help="Skip Optuna, use default hyper-params.",
    )
    p.add_argument("--output-dir", type=str, default=str(OUTPUT_DIR))
    p.add_argument(
        "--sample",
        type=int,
        default=None,
        metavar="N",
        help="Use only first N rows by date (quick sanity check).",
    )
    p.add_argument(
        "--event-precision-floor",
        type=float,
        default=0.20,
        help=(
            "Minimum precision constraint while tuning threshold for event class "
            "(default: 0.20). Set to 0.0 to optimise F1 without constraint."
        ),
    )
    p.add_argument(
        "--hpo-precision-floor",
        type=float,
        default=0.0,
        help=(
            "Minimum precision constraint INSIDE Optuna CV folds (default: 0.0 — "
            "pure F1 maximisation). Setting this > 0 tends to lower F1."
        ),
    )
    p.add_argument(
        "--hpo-objective",
        choices=["ks_at_coverage", "f1"],
        default="ks_at_coverage",
        help=(
            "Optuna objective metric (default: ks_at_coverage). "
            "Use f1 to keep prior behaviour."
        ),
    )
    p.add_argument(
        "--hpo-coverage",
        type=float,
        default=0.50,
        help=(
            "Coverage used when hpo-objective=ks_at_coverage "
            "(default: 0.50 for KS@50%%)."
        ),
    )
    p.add_argument(
        "--load-join",
        choices=["left", "inner"],
        default="left",
        help=(
            "How to join combined feature parts while loading from Snowflake "
            "(default: left)."
        ),
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _ensure_merge_keys(df: pd.DataFrame, table_name: str) -> None:
    missing = [k for k in MERGE_KEYS if k not in df.columns]
    if missing:
        raise ValueError(
            f"Table '{table_name}' missing merge keys: {missing}. "
            f"Available columns count={len(df.columns)}"
        )


def _dedupe_by_keys(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    before = len(df)
    out = df.drop_duplicates(subset=MERGE_KEYS, keep="last")
    dropped = before - len(out)
    if dropped > 0:
        print(
            f"  Deduped {dropped:,} duplicate rows on keys in {table_name}",
            flush=True,
        )
    return out


def _downcast_floats(df: pd.DataFrame) -> pd.DataFrame:
    """Cast float64 columns to float32, halving their memory footprint.

    XGBoost operates in float32 internally, so this is lossless for the model.
    Integer columns are downcast to the smallest signed int type that fits.
    """
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
    """Return current process RSS in GB (best-effort; 0.0 if psutil unavailable)."""
    try:
        import psutil, os as _os
        return psutil.Process(_os.getpid()).memory_info().rss / 1e9
    except Exception:
        return 0.0


def load_from_snowflake_chunked(join_how: str = "left") -> pd.DataFrame:
    from db_service import fetch_data

    if join_how not in {"left", "inner"}:
        raise ValueError(f"join_how must be 'left' or 'inner', got: {join_how}")

    df_combined = None
    for idx, table in enumerate(COMBINED_FEATURES_TABLES, start=1):
        t0 = time.time()
        print(f"Fetching from Snowflake: {table}", flush=True)
        df = fetch_data(f"SELECT * FROM {table}")
        df.columns = [c.upper() for c in df.columns]
        _ensure_merge_keys(df, table)
        df = _dedupe_by_keys(df, table)
        df = _downcast_floats(df)
        print(
            f"  → Fetched {len(df):,} rows, {df.shape[1]} columns"
            f"  [{df.memory_usage(deep=False).sum() / 1e9:.2f} GB]",
            flush=True,
        )

        if df_combined is None:
            df_combined = df
            del df
        else:
            common_cols = list(set(df_combined.columns).intersection(df.columns))
            cols_to_drop = [c for c in common_cols if c not in MERGE_KEYS]
            df = df.drop(columns=cols_to_drop)
            before_rows = len(df_combined)
            df_combined = pd.merge(
                df_combined,
                df,
                on=MERGE_KEYS,
                how=join_how,
                validate="one_to_one",
            )
            del df
            after_rows = len(df_combined)
            new_cols = [c for c in df_combined.columns if c not in MERGE_KEYS]
            if new_cols:
                coverage = 1.0 - float(df_combined[new_cols[0]].isna().mean())
                print(
                    f"  Merge {idx}/{len(COMBINED_FEATURES_TABLES)} ({join_how}) "
                    f"rows: {before_rows:,} -> {after_rows:,} | "
                    f"feature coverage ~{coverage:.2%}",
                    flush=True,
                )

        gc.collect()
        print(
            f"  Loaded in {time.time() - t0:.1f}s"
            f"  [RSS {_rss_gb():.2f} GB after part {idx}]",
            flush=True,
        )

    print(
        f"  → Final combined: {len(df_combined):,} rows, {df_combined.shape[1]} columns",
        flush=True,
    )
    return df_combined


def _read_parquet(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    print(f"Loading parquet: {path}", flush=True)
    try:
        df = pd.read_parquet(path)
    except ImportError as e:
        raise ImportError(
            "Reading parquet requires 'pyarrow' or 'fastparquet'. "
            "Install one of them, or use --local-csv instead."
        ) from e
    print(f"  → {len(df):,} rows, {df.shape[1]} columns", flush=True)
    return df


def _write_parquet(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Saving combined dataset to parquet: {path}", flush=True)
    try:
        df.to_parquet(path, index=False)
    except ImportError as e:
        raise ImportError(
            "Writing parquet requires 'pyarrow' or 'fastparquet'. "
            "Install one of them, or omit --cache-parquet."
        ) from e
    print("  Parquet cache saved.", flush=True)


def load_data(
    local_csv=None,
    local_parquet=None,
    cache_parquet=None,
    refresh_cache: bool = False,
    load_join: str = "left",
) -> pd.DataFrame:
    if local_csv:
        print(f"Loading CSV: {local_csv}", flush=True)
        df = pd.read_csv(local_csv)
        print(f"  → {len(df):,} rows, {df.shape[1]} columns", flush=True)
        return df
    if local_parquet:
        return _read_parquet(local_parquet)
    if cache_parquet:
        cache_path = Path(cache_parquet)
        if cache_path.exists() and not refresh_cache:
            print(
                f"Using cached parquet (skipping Snowflake): {cache_path}",
                flush=True,
            )
            return _read_parquet(cache_path)
        if cache_path.exists() and refresh_cache:
            print(
                f"Refreshing cached parquet from Snowflake: {cache_path}",
                flush=True,
            )

    df = load_from_snowflake_chunked(join_how=load_join)
    if cache_parquet:
        _write_parquet(df, cache_parquet)
    return df


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------


def _normalise_cat_col(s: pd.Series) -> pd.Series:
    """Cast to str, normalise nulls → '__MISSING__'."""
    s = s.astype(str)
    return s.apply(lambda v: "__MISSING__" if v.lower() in _NULL_SENTINELS else v)


def _downcast_numeric_df(X: pd.DataFrame) -> pd.DataFrame:
    """Reduce numeric memory footprint without changing semantic values."""
    num_cols = list(X.select_dtypes(include=[np.number]).columns)
    if not num_cols:
        return X

    before_mb = X[num_cols].memory_usage(deep=True).sum() / (1024**2)

    float_cols = list(X[num_cols].select_dtypes(include=["float"]).columns)
    int_cols = list(X[num_cols].select_dtypes(include=["integer"]).columns)

    for col in float_cols:
        X[col] = pd.to_numeric(X[col], downcast="float")
    for col in int_cols:
        X[col] = pd.to_numeric(X[col], downcast="integer")

    after_mb = X[num_cols].memory_usage(deep=True).sum() / (1024**2)
    print(f"  Downcast numeric cols: {before_mb:.1f} MB → {after_mb:.1f} MB")
    return X


def preprocess(df: pd.DataFrame):
    """Returns (X, y, dates, label_encoders)."""
    df.columns = [c.upper() for c in df.columns]
    print(f"\nPreprocessing: {df.shape[1]} columns, {len(df):,} rows")

    # Parse date
    if DATE_COL not in df.columns:
        raise ValueError(f"'{DATE_COL}' not found. Available: {list(df.columns)}")
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    # Drop target == -1
    before = len(df)
    df = df[df[TARGET_COL] != -1].copy()
    print(f"  Dropped {before - len(df):,} rows (target == -1)")

    df[TARGET_COL] = df[TARGET_COL].astype(int)

    # Separate target + date
    y = df.pop(TARGET_COL)
    dates = df[DATE_COL].copy()

    # Drop ID / leakage
    drop_cols = [
        c for c in df.columns if c in ID_AND_LEAKAGE_COLS and c != DATE_COL
    ]
    print(f"  Dropping {len(drop_cols)} ID/leakage cols: {drop_cols}")
    df.drop(columns=drop_cols, inplace=True)
    X = df

    # Drop unknown object blobs
    obj_cols = [c for c in X.columns if X[c].dtype == object]
    known_cats = [c for c in obj_cols if c in CATEGORICAL_COLS]
    unknown_blobs = [c for c in obj_cols if c not in CATEGORICAL_COLS]
    if unknown_blobs:
        print(f"  Dropping {len(unknown_blobs)} blob cols: {unknown_blobs}")
        X.drop(columns=unknown_blobs, inplace=True)

    # Convert categoricals to pandas category dtype
    label_encoders = {}
    for col in known_cats:
        if col not in X.columns:
            continue
        X[col] = _normalise_cat_col(X[col]).astype("category")
        label_encoders[col] = "category"  # Dummy value

    # Missing values (np.nan) in numeric columns are deliberately left alone.
    # Both XGBoost and LightGBM handle NaNs natively by learning the optimal split direction.

    # Drop any surviving object cols
    for col in list(X.columns):
        if X[col].dtype == object:
            print(f"  Warning: '{col}' still object — dropping.")
            X.drop(columns=[col], inplace=True)

    X = _downcast_numeric_df(X)

    print(f"  Feature matrix: {X.shape[1]} features, {len(X):,} rows")
    print(f"  Target dist: {y.value_counts().to_dict()}")
    print(f"  Date range:  {dates.min().date()} → {dates.max().date()}")
    return X, y, dates, label_encoders


def prepare_raw_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lightweight cleanup on the full frame before any split-specific preprocessing.
    Keeps peak memory lower than materializing a full preprocessed matrix up front.
    """
    df.columns = [c.upper() for c in df.columns]
    print(f"\nPreprocessing: {df.shape[1]} columns, {len(df):,} rows")

    if DATE_COL not in df.columns:
        raise ValueError(f"'{DATE_COL}' not found. Available: {list(df.columns)}")
    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    before = len(df)
    keep_mask = df[TARGET_COL] != -1
    if not bool(keep_mask.all()):
        df = df.loc[keep_mask].copy()
    print(f"  Dropped {before - len(df):,} rows (target == -1)")
    df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], downcast="integer")

    drop_cols = [
        c for c in df.columns if c in ID_AND_LEAKAGE_COLS and c != DATE_COL
    ]
    print(f"  Dropping {len(drop_cols)} ID/leakage cols: {drop_cols}")
    df.drop(columns=drop_cols, inplace=True)

    obj_cols = [c for c in df.columns if df[c].dtype == object]
    known_cats = [c for c in obj_cols if c in CATEGORICAL_COLS]
    unknown_blobs = [c for c in obj_cols if c not in CATEGORICAL_COLS]
    if unknown_blobs:
        print(f"  Dropping {len(unknown_blobs)} blob cols: {unknown_blobs}")
        df.drop(columns=unknown_blobs, inplace=True)

    print(f"  Raw frame after cleanup: {df.shape[1]} columns, {len(df):,} rows")
    if known_cats:
        print(f"  Known categorical cols retained: {len(known_cats)}")

    df = _downcast_numeric_df(df)
    return df


def temporal_split_raw_df(df, train_frac=0.60, val_frac=0.20):
    """Sort raw rows by date, then slice train/val/test before heavy transforms."""
    order = np.argsort(df[DATE_COL].to_numpy())
    df_s = df.iloc[order].reset_index(drop=True)
    n = len(df_s)
    t1, t2 = int(n * train_frac), int(n * (train_frac + val_frac))

    splits = {
        "train": df_s.iloc[:t1].copy(),
        "val": df_s.iloc[t1:t2].copy(),
        "test": df_s.iloc[t2:].copy(),
    }

    print("\nTemporal splits:")
    for name, part in splits.items():
        print(
            f"  {name:5s}: {len(part):>7,} rows | "
            f"{part[DATE_COL].min().date()} → {part[DATE_COL].max().date()}"
        )
    return splits["train"], splits["val"], splits["test"]


def preprocess_partition(
    df: pd.DataFrame,
    category_levels: dict[str, list[str]] | None = None,
    fit_categories: bool = False,
):
    """Preprocess one already-split partition using train-derived category levels."""
    y = pd.to_numeric(df.pop(TARGET_COL), downcast="integer").astype(int)
    dates = df.pop(DATE_COL)
    X = df

    inferred_levels = {} if fit_categories else (category_levels or {})

    for col in list(X.columns):
        if X[col].dtype != object:
            continue
        if col not in CATEGORICAL_COLS:
            print(f"  Warning: '{col}' still object — dropping.")
            X.drop(columns=[col], inplace=True)
            continue

        normalized = _normalise_cat_col(X[col])
        if fit_categories:
            cats = sorted(pd.Index(normalized.dropna().unique()).tolist())
            inferred_levels[col] = cats
        else:
            cats = inferred_levels.get(col, sorted(pd.Index(normalized.dropna().unique()).tolist()))
        X[col] = pd.Categorical(normalized, categories=cats)

    X = _downcast_numeric_df(X)
    return X, y, dates, inferred_levels


def align_columns_to_train(
    X_train: pd.DataFrame,
    X_other: pd.DataFrame,
) -> pd.DataFrame:
    """Align a split to the train feature schema, filling absent cols with NaN."""
    missing = [c for c in X_train.columns if c not in X_other.columns]
    if missing:
        for col in missing:
            X_other[col] = np.nan
    extra = [c for c in X_other.columns if c not in X_train.columns]
    if extra:
        X_other = X_other.drop(columns=extra)
    return X_other[X_train.columns]


# ---------------------------------------------------------------------------
# In-memory feature engineering  (ratio / delta between time windows)
# ---------------------------------------------------------------------------


def engineer_features(X: pd.DataFrame) -> pd.DataFrame:
    """
    Create ratio and delta features between paired time-window columns.

    For every (suffix_a, suffix_b) pair in _WINDOW_SUFFIX_PAIRS, we find
    all columns whose name ends with suffix_a, look for a corresponding column
    whose name is `prefix + suffix_b`, and build:
      - ratio : col_a / (col_b + 1)   — robust, no divide-by-zero
      - delta : col_a - col_b

    Only numeric columns are considered. Returns X with the new columns appended.
    Runs ONLY on the full X before temporal split (or is column-aligned post-split).
    """
    new_cols: dict = {}
    numeric_cols = set(X.select_dtypes(include=[np.number]).columns)

    for suf_a, suf_b in _WINDOW_SUFFIX_PAIRS:
        for col_a in X.columns:
            if not col_a.endswith(suf_a):
                continue
            if col_a not in numeric_cols:
                continue
            prefix = col_a[: -len(suf_a)]
            col_b = prefix + suf_b
            if col_b not in X.columns or col_b not in numeric_cols:
                continue
            ratio_name = f"RATIO__{prefix}{suf_a}_vs{suf_b}"
            delta_name = f"DELTA__{prefix}{suf_a}_vs{suf_b}"
            if ratio_name not in new_cols:
                new_cols[ratio_name] = (
                    X[col_a].astype(np.float32) / (X[col_b].abs().astype(np.float32) + 1.0)
                ).astype(np.float32)
            if delta_name not in new_cols:
                new_cols[delta_name] = (
                    X[col_a].astype(np.float32) - X[col_b].astype(np.float32)
                ).astype(np.float32)

    if new_cols:
        new_df = pd.DataFrame(new_cols, index=X.index)
        X = pd.concat([X, new_df], axis=1)
        print(f"  Engineered {len(new_cols)} ratio/delta features → total {X.shape[1]}")
    else:
        print("  No window-pair ratio/delta features generated.")
    return X


def drop_high_null_cols(
    X: pd.DataFrame, threshold: float = _MAX_NULL_RATE
) -> pd.DataFrame:
    """
    Drop columns where the fraction of null values exceeds `threshold`.
    These features can't provide meaningful signal and slow down selection.
    """
    null_rates = X.isnull().mean()
    to_drop = null_rates[null_rates > threshold].index.tolist()
    if to_drop:
        print(f"  Dropping {len(to_drop)} columns with >{threshold:.0%} nulls.")
        X = X.drop(columns=to_drop)
    return X


# ---------------------------------------------------------------------------
# Temporal split
# ---------------------------------------------------------------------------


def temporal_split(X, y, dates, train_frac=0.60, val_frac=0.20):
    """Sort by CUTOFF_DATE, slice into train/val/test — no random shuffling."""
    order = dates.argsort()
    X_s, y_s, d_s = X.iloc[order], y.iloc[order], dates.iloc[order]
    n = len(X_s)
    t1, t2 = int(n * train_frac), int(n * (train_frac + val_frac))

    splits = {
        "train": (X_s.iloc[:t1], y_s.iloc[:t1], d_s.iloc[:t1]),
        "val": (X_s.iloc[t1:t2], y_s.iloc[t1:t2], d_s.iloc[t1:t2]),
        "test": (X_s.iloc[t2:], y_s.iloc[t2:], d_s.iloc[t2:]),
    }
    print("\nTemporal splits:")
    for name, (xp, _, dp) in splits.items():
        print(
            f"  {name:5s}: {len(xp):>7,} rows | {dp.min().date()} → {dp.max().date()}"
        )

    return (
        splits["train"][0],
        splits["val"][0],
        splits["test"][0],
        splits["train"][1],
        splits["val"][1],
        splits["test"][1],
        splits["train"][2],
        splits["val"][2],
        splits["test"][2],
    )


# ---------------------------------------------------------------------------
# Feature selection  (fast tree priors + targeted IV)
# ---------------------------------------------------------------------------


def _compute_iv(X: pd.DataFrame, y: pd.Series, n_bins: int = 10) -> pd.Series:
    """
    Compute Information Value (IV) for every feature via quantile binning.
    IV = Σ (Dist_Events_i − Dist_NonEvents_i) × WoE_i
    Returns a Series indexed by column name.
    """
    eps = 1e-8
    total_events = max((y == 1).sum(), 1)
    total_nonevents = max((y == 0).sum(), 1)
    ivs = {}
    for col in X.columns:
        try:
            # Quantile-bin the feature
            binned = pd.qcut(X[col], q=n_bins, duplicates="drop", labels=False)
        except Exception:
            ivs[col] = 0.0
            continue

        tmp = pd.DataFrame({"bin": binned, "y": y.values})
        grp = tmp.groupby("bin")["y"].agg(
            events=lambda s: (s == 1).sum(),
            nonevents=lambda s: (s == 0).sum(),
        )
        grp["dist_ev"] = grp["events"] / total_events
        grp["dist_nev"] = grp["nonevents"] / total_nonevents
        grp["woe"] = np.log((grp["dist_ev"] + eps) / (grp["dist_nev"] + eps))
        grp["iv_part"] = (grp["dist_ev"] - grp["dist_nev"]) * grp["woe"]
        ivs[col] = grp["iv_part"].sum()

    return pd.Series(ivs)


def _sample_rows_for_selection(
    X: pd.DataFrame,
    y: pd.Series,
    max_rows: int,
    seed: int,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Cap the row count used in feature-selection helper models.
    Keeps class balance reasonable by sampling positives / negatives separately.
    """
    if len(X) <= max_rows:
        return X, y

    pos_idx = np.flatnonzero(y.to_numpy() == 1)
    neg_idx = np.flatnonzero(y.to_numpy() == 0)
    rng = np.random.default_rng(seed)

    target_pos = min(len(pos_idx), max(1, int(round(max_rows * len(pos_idx) / len(X)))))
    target_neg = min(len(neg_idx), max_rows - target_pos)

    if target_pos == 0 and len(pos_idx) > 0:
        target_pos = 1
        target_neg = max_rows - 1
    if target_neg <= 0:
        target_neg = min(len(neg_idx), max_rows)
        target_pos = max(0, max_rows - target_neg)

    sampled = []
    if target_pos > 0:
        sampled.append(rng.choice(pos_idx, size=target_pos, replace=False))
    if target_neg > 0:
        sampled.append(rng.choice(neg_idx, size=target_neg, replace=False))

    idx = np.concatenate(sampled)
    idx.sort()
    return X.iloc[idx], y.iloc[idx]


def _compute_prior_lgb(
    X: pd.DataFrame, y: pd.Series, subsample_frac: float = 0.3
) -> pd.Series:
    """Fast LightGBM prior as an alternative to Mutual Info."""
    try:
        import lightgbm as lgb
    except ImportError:
        print("     LightGBM not installed. Returning zeros.")
        return pd.Series(0.0, index=X.columns)

    n = int(len(X) * subsample_frac)
    rng = np.random.default_rng(RANDOM_SEED + 1)
    idx = rng.choice(len(X), size=n, replace=False)
    Xs, ys = X.iloc[idx], y.iloc[idx]

    neg, pos = (ys == 0).sum(), (ys == 1).sum()
    spw = neg / max(pos, 1)

    model = lgb.LGBMClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=spw,
        random_state=RANDOM_SEED,
        n_jobs=-1,
        verbosity=-1,
    )
    model.fit(Xs, ys)
    return pd.Series(model.feature_importances_, index=X.columns)


def _compute_prior_xgb(
    X: pd.DataFrame,
    y: pd.Series,
    subsample_frac: float = 0.3,
) -> pd.Series:
    """
    Train a fast XGBoost (100 trees) on a random subsample of train data.
    Returns gain-based feature importances.
    """
    n = int(len(X) * subsample_frac)
    rng = np.random.default_rng(RANDOM_SEED)
    idx = rng.choice(len(X), size=n, replace=False)
    Xs, ys = X.iloc[idx], y.iloc[idx]

    neg, pos = (ys == 0).sum(), (ys == 1).sum()
    spw = neg / max(pos, 1)

    prior = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=spw,
        tree_method="hist",
        device="cpu",
        verbosity=0,
        seed=RANDOM_SEED,
        enable_categorical=True,
    )
    prior.fit(Xs, ys, verbose=False)

    scores = prior.get_booster().get_score(importance_type="gain")
    return pd.Series(scores)  # indexed by feature name (f0, f1, … or column name)


def _minmax(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    if hi == lo:
        return pd.Series(0.0, index=s.index)
    return (s - lo) / (hi - lo)


def select_features(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    top_n: int = 100,
    output_dir: Path = OUTPUT_DIR,
) -> list:
    """
    Combine fast tree-based priors with a targeted IV pass on a short list.
    This keeps the original spirit of the selector while making it much faster
    on wide tables.
    Returns list of selected column names.
    """
    print(f"\nFeature selection: {X_train.shape[1]} → top {top_n}")
    feat_names = list(X_train.columns)
    base = pd.Series(0.0, index=feat_names)
    candidate_pool_size = min(len(feat_names), max(top_n * 4, 200))

    # ---------- (a) Prior XGBoost gain ----------
    print("  [1/3] Prior XGBoost (100 trees, 30% subsample) …", flush=True)
    try:
        xgb_raw = _compute_prior_xgb(X_train, y_train, subsample_frac=0.30)
        # XGBoost names features as the actual column names when fitted on DataFrame
        xgb_scores = base.copy()
        for feat, val in xgb_raw.items():
            if feat in xgb_scores.index:
                xgb_scores[feat] = val
        xgb_norm = _minmax(xgb_scores)
        print(f"     → {(xgb_norm > 0).sum()} features with non-zero gain")
    except Exception as e:
        print(f"     Prior XGBoost failed ({e}), skipping.")
        xgb_norm = base.copy()
        xgb_scores = base.copy()

    # ---------- (b) Prior LightGBM gain ----------
    print("  [2/3] Prior LightGBM (100 trees, 30% subsample) …", flush=True)
    try:
        lgb_raw = _compute_prior_lgb(X_train, y_train, subsample_frac=0.30)
        lgb_norm = _minmax(lgb_raw)
        print(f"     → {(lgb_norm > 0).sum()} features with non-zero gain")
    except Exception as e:
        print(f"     Prior LightGBM failed ({e}), skipping.")
        lgb_norm = base.copy()
        lgb_raw = base.copy()

    # ---------- Candidate shortlist from fast priors ----------
    candidate_cols = set(xgb_scores.nlargest(candidate_pool_size).index.tolist())
    candidate_cols.update(lgb_raw.nlargest(candidate_pool_size).index.tolist())
    candidate_cols = [c for c in feat_names if c in candidate_cols]
    if not candidate_cols:
        candidate_cols = feat_names
    print(
        f"  Shortlisting {len(candidate_cols)} candidate features "
        f"from prior model gains before IV.",
        flush=True,
    )

    # ---------- (c) Information Value ----------
    print("  [3/3] Information Value (WoE binning on shortlist) …", flush=True)
    try:
        X_iv, y_iv = _sample_rows_for_selection(
            X_train[candidate_cols],
            y_train,
            max_rows=40000,
            seed=RANDOM_SEED + 7,
        )
        iv_raw_short = _compute_iv(X_iv, y_iv, n_bins=8)
        iv_norm_short = _minmax(iv_raw_short.clip(lower=0))  # negative IV = noise
        iv_norm = base.copy()
        for feat, val in iv_norm_short.items():
            iv_norm[feat] = val
        print(
            f"     → computed IV for {len(candidate_cols)} shortlisted features "
            f"using {len(X_iv):,} rows"
        )
        print(f"     → top-5 IV: {iv_raw_short.nlargest(5).to_dict()}")
    except Exception as e:
        print(f"     IV failed ({e}), skipping.")
        iv_norm = base.copy()

    # ---------- Combined score ----------
    # Heavier weight on the two fast prior models; IV acts as a focused tiebreaker.
    combined = (0.45 * xgb_norm) + (0.45 * lgb_norm) + (0.10 * iv_norm)

    selected = combined.nlargest(min(top_n, len(combined))).index.tolist()
    print(
        f"  Selected {len(selected)} features "
        f"(weighted XGBgain + LGBgain + shortlist-IV)"
    )

    # Persist the full ranking for inspection
    output_dir.mkdir(parents=True, exist_ok=True)
    ranking = pd.DataFrame(
        {
            "feature": feat_names,
            "xgb_gain": xgb_norm,
            "lgb_gain": lgb_norm,
            "iv": iv_norm,
            "combined": combined,
        }
    ).sort_values("combined", ascending=False)
    ranking.to_csv(output_dir / "feature_selection_ranking.csv", index=False)
    print(f"  Full ranking → {output_dir / 'feature_selection_ranking.csv'}")

    return selected


# ---------------------------------------------------------------------------
# Optuna objective  (TimeSeriesSplit CV, subsampled for speed)
# ---------------------------------------------------------------------------


def build_objective(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    n_cv_folds: int = 2,
    hpo_subsample: float = 0.4,
    event_precision_floor: float = 0.0,  # 0 = pure F1 maximisation inside HPO
    optimize_metric: str = "ks_at_coverage",
    optimize_coverage: float = 0.50,
) -> callable:
    """
    Objective for Optuna. For each trial:
      - Subsample hpo_subsample fraction of X_train (chronologically — keep latest)
      - Run n_cv_folds TimeSeriesSplit folds
      - Optimise either:
          * KS at fixed coverage (default, ranking objective), or
          * positive-class F1 (threshold tuned on each CV fold)

    NOTE: event_precision_floor defaults to 0 inside HPO so Optuna can freely
    maximise F1.  The precision constraint is applied when tuning the final
    threshold on the held-out validation set (controlled by --event-precision-floor).
    """
    tscv = TimeSeriesSplit(n_splits=n_cv_folds)

    # Subsample: keep the most-recent rows (temporally safe — train is sorted)
    n_sub = max(int(len(X_train) * hpo_subsample), 1000)
    X_sub = X_train.iloc[-n_sub:].copy()
    y_sub = y_train.iloc[-n_sub:].copy()

    base_spw = (y_sub == 0).sum() / max((y_sub == 1).sum(), 1)

    print(
        f"\nOptuna will use {len(X_sub):,} train rows "
        f"({hpo_subsample:.0%} of {len(X_train):,}) "
        f"× {n_cv_folds} CV folds per trial. "
        f"Objective={optimize_metric}"
    )
    if optimize_metric == "ks_at_coverage":
        print(f"  KS coverage target inside HPO: {optimize_coverage:.1%}")

    def objective(trial: optuna.Trial) -> float:
        # --- expanded search space for better F1 ---
        n_est = trial.suggest_int("n_estimators", 400, 2000)
        max_depth = trial.suggest_int("max_depth", 3, 9)
        # max_leaves controls expressiveness independently of depth
        max_leaves = trial.suggest_int("max_leaves", 0, 256)  # 0 = unconstrained
        # max_bin: more bins → better split quality on dense numerics
        max_bin = trial.suggest_int("max_bin", 128, 512)

        params = dict(
            verbosity=0,
            objective="binary:logistic",
            eval_metric="aucpr",
            tree_method="hist",
            device="cpu",
            seed=RANDOM_SEED,
            n_estimators=n_est,
            max_depth=max_depth,
            max_leaves=max_leaves,
            max_bin=max_bin,
            learning_rate=trial.suggest_float("learning_rate", 5e-4, 0.08, log=True),
            min_child_weight=trial.suggest_float("min_child_weight", 1, 30),
            subsample=trial.suggest_float("subsample", 0.5, 1.0),
            colsample_bytree=trial.suggest_float("colsample_bytree", 0.3, 1.0),
            colsample_bylevel=trial.suggest_float("colsample_bylevel", 0.3, 1.0),
            colsample_bynode=trial.suggest_float("colsample_bynode", 0.5, 1.0),
            reg_alpha=trial.suggest_float("reg_alpha", 1e-8, 50.0, log=True),
            reg_lambda=trial.suggest_float("reg_lambda", 1e-8, 50.0, log=True),
            gamma=trial.suggest_float("gamma", 0.0, 10.0),
            # Allow SPW up to 3× base to push recall on minority class
            scale_pos_weight=trial.suggest_float(
                "scale_pos_weight", base_spw * 0.3, base_spw * 3.0
            ),
        )

        fold_scores = []
        for fold_idx, (tr_idx, cv_idx) in enumerate(tscv.split(X_sub)):
            X_tr, X_cv = X_sub.iloc[tr_idx], X_sub.iloc[cv_idx]
            y_tr, y_cv = y_sub.iloc[tr_idx], y_sub.iloc[cv_idx]

            callbacks = (
                [XGBoostPruningCallback(trial, "validation_0-aucpr")]
                if _PRUNING_AVAILABLE
                else None
            )
            model = xgb.XGBClassifier(
                **{k: v for k, v in params.items() if k != "n_estimators"},
                n_estimators=params["n_estimators"],
                early_stopping_rounds=30,
                callbacks=callbacks,
                enable_categorical=True,
            )
            model.fit(X_tr, y_tr, eval_set=[(X_cv, y_cv)], verbose=False)

            proba = model.predict_proba(X_cv)[:, 1]
            if optimize_metric == "ks_at_coverage":
                fold_score = ks_at_coverage(y_cv, proba, coverage=optimize_coverage)[
                    "ks"
                ]
            else:
                # Pure F1 maximisation inside HPO (no precision floor)
                threshold_info = tune_event_threshold(
                    y_cv, proba, precision_floor=event_precision_floor
                )
                fold_score = float(threshold_info["f1"])
            fold_scores.append(float(fold_score))

            trial.report(np.mean(fold_scores), step=fold_idx)
            if trial.should_prune():
                raise optuna.exceptions.TrialPruned()

        return float(np.mean(fold_scores))

    return objective


# ---------------------------------------------------------------------------
# Default hyper-params
# ---------------------------------------------------------------------------


def default_params(scale_pos_weight: float) -> dict:
    return dict(
        verbosity=0,
        objective="binary:logistic",
        eval_metric="aucpr",
        tree_method="hist",
        device="cpu",
        seed=RANDOM_SEED,
        scale_pos_weight=scale_pos_weight,
        n_estimators=800,
        max_depth=7,
        max_leaves=64,
        max_bin=256,
        learning_rate=0.03,
        min_child_weight=10,
        subsample=0.8,
        colsample_bytree=0.8,
        colsample_bylevel=0.8,
        colsample_bynode=0.8,
        reg_alpha=1.0,
        reg_lambda=1.0,
        gamma=1.0,
        early_stopping_rounds=50,
    )


# ---------------------------------------------------------------------------
# Final model
# ---------------------------------------------------------------------------


def train_final_model(X_tv, y_tv, X_val, y_val, params) -> xgb.XGBClassifier:
    model = xgb.XGBClassifier(**params, enable_categorical=True)
    model.fit(X_tv, y_tv, eval_set=[(X_val, y_val)], verbose=100)
    return model


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def tune_event_threshold(y_true, proba, precision_floor: float = 0.20) -> dict:
    """
    Choose threshold on validation scores to maximize event-class F1
    while respecting a minimum precision floor.

    Uses a dense 361-point grid (0.01 to 0.99) so the optimal threshold
    is found with ~2.7 pp resolution. Falls back to pure F1 maximisation
    if no threshold meets the precision floor.
    """
    y_arr = np.asarray(y_true)
    p_arr = np.asarray(proba)
    # Denser grid: 361 points → ~2.7 pp step
    thresholds = np.linspace(0.01, 0.99, 361)
    rows = []
    for t in thresholds:
        pred = (p_arr >= t).astype(int)
        prec = precision_score(y_arr, pred, zero_division=0)
        rec = recall_score(y_arr, pred, zero_division=0)
        f1 = f1_score(y_arr, pred, zero_division=0)
        rows.append((t, prec, rec, f1))

    score_df = pd.DataFrame(rows, columns=["threshold", "precision", "recall", "f1"])

    if precision_floor > 0:
        constrained = score_df[score_df["precision"] >= precision_floor]
    else:
        constrained = pd.DataFrame()  # skip constraint

    if len(constrained) > 0:
        best = constrained.sort_values(
            ["f1", "recall", "precision"], ascending=False
        ).iloc[0]
        constraint_satisfied = True
    else:
        # Fall back to pure F1 maximisation
        best = score_df.sort_values(
            ["f1", "recall", "precision"], ascending=False
        ).iloc[0]
        constraint_satisfied = bool(best["precision"] >= precision_floor)

    return {
        "threshold": float(best["threshold"]),
        "precision": float(best["precision"]),
        "recall": float(best["recall"]),
        "f1": float(best["f1"]),
        "used_precision_floor": float(precision_floor),
        "constraint_satisfied": constraint_satisfied,
        "scores": score_df,
    }


def ks_at_coverage(y_true, proba, coverage: float = 0.50) -> dict:
    """
    Compute KS at fixed top-score population coverage.
    Example: coverage=0.50 -> KS among top 50% scored records.
    """
    cov = float(np.clip(coverage, 1e-6, 1.0))
    y = np.asarray(y_true).astype(int)
    p = np.asarray(proba)
    n = len(y)
    if n == 0:
        return {"ks": 0.0, "coverage": cov, "cutoff_rows": 0, "tpr": 0.0, "fpr": 0.0}

    order = np.argsort(-p)
    y_sorted = y[order]
    k = int(np.ceil(n * cov))
    k = min(max(k, 1), n)
    y_top = y_sorted[:k]

    total_ev = max(int(y.sum()), 1)
    total_nev = max(int((1 - y).sum()), 1)
    cum_ev = int(y_top.sum())
    cum_nev = int((1 - y_top).sum())
    tpr = cum_ev / total_ev
    fpr = cum_nev / total_nev
    ks = abs(tpr - fpr)

    return {
        "ks": float(ks),
        "coverage": cov,
        "cutoff_rows": int(k),
        "tpr": float(tpr),
        "fpr": float(fpr),
    }


def evaluate(model, X_test, y_test, d_test=None, threshold: float = 0.50) -> dict:
    proba = model.predict_proba(X_test)[:, 1]
    preds = (proba >= threshold).astype(int)
    roc = roc_auc_score(y_test, proba)
    ap = average_precision_score(y_test, proba)
    ev_precision = precision_score(y_test, preds, zero_division=0)
    ev_recall = recall_score(y_test, preds, zero_division=0)
    ev_f1 = f1_score(y_test, preds, zero_division=0)
    print("\n" + "=" * 60)
    print("TEST SET EVALUATION")
    if d_test is not None:
        print(f"  Period: {d_test.min().date()} → {d_test.max().date()}")
    print("=" * 60)
    print(f"  ROC-AUC       : {roc:.4f}")
    print(f"  Avg Precision : {ap:.4f}")
    print(f"  Threshold     : {threshold:.3f}")
    print(f"  Event Precision: {ev_precision:.4f}")
    print(f"  Event Recall   : {ev_recall:.4f}")
    print(f"  Event F1       : {ev_f1:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, preds, digits=4))
    print("Confusion Matrix:")
    print(confusion_matrix(y_test, preds))
    print("=" * 60)
    return {
        "roc_auc": roc,
        "avg_precision": ap,
        "threshold": float(threshold),
        "event_precision": ev_precision,
        "event_recall": ev_recall,
        "event_f1": ev_f1,
    }, proba


# ---------------------------------------------------------------------------
# SHAP analysis
# ---------------------------------------------------------------------------


def run_shap_analysis(
    model: xgb.XGBClassifier,
    X_test: pd.DataFrame,
    output_dir: Path,
    max_display: int = 30,
    max_rows: int = 5000,
) -> None:
    """
    Compute SHAP values on the test set and save:
      shap_values.csv           — raw SHAP matrix (sample ≤ max_rows rows)
      shap_mean_abs.csv         — mean |SHAP| per feature (ranked)
      shap_summary_beeswarm.png — beeswarm plot (top max_display)
      shap_summary_bar.png      — bar plot (top max_display)
    """
    if not _SHAP_AVAILABLE:
        print("  SHAP skipped — pip install shap")
        return

    print("\nSHAP analysis …", flush=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Sub-sample for speed (SHAP is O(n * depth * trees))
    if len(X_test) > max_rows:
        X_shap = X_test.sample(n=max_rows, random_state=RANDOM_SEED)
    else:
        X_shap = X_test.copy()

    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_shap)  # (n_rows, n_features)

    # ---- raw SHAP CSV ----
    shap_df = pd.DataFrame(shap_values, columns=X_shap.columns)
    shap_df.to_csv(output_dir / "shap_values.csv", index=False)

    # ---- mean |SHAP| ranking ----
    mean_abs = (
        pd.DataFrame(
            {
                "feature": X_shap.columns,
                "mean_abs_shap": np.abs(shap_values).mean(axis=0),
            }
        )
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )
    mean_abs.to_csv(output_dir / "shap_mean_abs.csv", index=False)

    # ---- beeswarm plot ----
    fig, ax = plt.subplots(figsize=(10, max(6, min(max_display, 30) * 0.35)))
    shap.summary_plot(
        shap_values,
        X_shap,
        max_display=max_display,
        show=False,
        plot_type="dot",
    )
    plt.tight_layout()
    plt.savefig(output_dir / "shap_summary_beeswarm.png", dpi=150, bbox_inches="tight")
    plt.close("all")

    # ---- bar plot ----
    shap.summary_plot(
        shap_values,
        X_shap,
        max_display=max_display,
        show=False,
        plot_type="bar",
    )
    plt.tight_layout()
    plt.savefig(output_dir / "shap_summary_bar.png", dpi=150, bbox_inches="tight")
    plt.close("all")

    print(f"  SHAP values        → {output_dir / 'shap_values.csv'}")
    print(f"  SHAP mean |abs|    → {output_dir / 'shap_mean_abs.csv'}")
    print(f"  SHAP beeswarm plot → {output_dir / 'shap_summary_beeswarm.png'}")
    print(f"  SHAP bar plot      → {output_dir / 'shap_summary_bar.png'}")


# ---------------------------------------------------------------------------
# Decile / Lift / KS analysis
# ---------------------------------------------------------------------------


def run_decile_ks_analysis(
    y_true: pd.Series,
    proba: np.ndarray,
    output_dir: Path,
    n_deciles: int = 10,
) -> pd.DataFrame:
    """
    Build a population-based decile table and compute KS statistic.

    Deciles are formed by splitting the test population into N equal-sized
    buckets ordered by descending predicted probability.

    Columns in the output CSV
    -------------------------
    decile            : 1 (highest score) … N (lowest)
    score_min/max     : score range within the decile
    count             : number of records
    events            : actual positives (y=1)
    non_events        : actual negatives (y=0)
    event_rate        : events / count
    decile_precision  : decile events / decile count
    decile_recall     : decile events / total events overall
    cumulative_decile_precision : cumulative events / cumulative count up to decile
    cumulative_decile_recall    : cumulative events / total events overall
    cum_events_pct    : cumulative % of all events captured up to this decile
    cum_nonevents_pct : cumulative % of all non-events captured up to this decile
    cum_population_pct: cumulative % of total population
    lift              : event_rate / overall_event_rate
    cum_lift          : cumulative event_rate / overall_event_rate
    ks                : |cum_events_pct - cum_nonevents_pct| per decile
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    y = np.asarray(y_true)
    p = np.asarray(proba)

    # Sort descending by score
    order = np.argsort(-p)
    y_sorted = y[order]
    p_sorted = p[order]

    n = len(y_sorted)
    total_ev = max(y_sorted.sum(), 1)
    total_nev = max((1 - y_sorted).sum(), 1)
    overall_er = total_ev / n

    # Build decile boundaries
    decile_size = n / n_deciles
    rows = []
    for d in range(1, n_deciles + 1):
        lo = int(round((d - 1) * decile_size))
        hi = int(round(d * decile_size))
        hi = min(hi, n)
        slice_y = y_sorted[lo:hi]
        slice_p = p_sorted[lo:hi]
        ev = int(slice_y.sum())
        nev = int((1 - slice_y).sum())
        rows.append(
            {
                "decile": d,
                "score_max": float(slice_p.max()),
                "score_min": float(slice_p.min()),
                "count": hi - lo,
                "events": ev,
                "non_events": nev,
            }
        )

    df = pd.DataFrame(rows)
    df["event_rate"] = np.where(df["count"] > 0, df["events"] / df["count"], 0.0)
    df["decile_precision"] = df["event_rate"]
    df["decile_recall"] = df["events"] / total_ev
    df["cum_events"] = df["events"].cumsum()
    df["cum_nonevents"] = df["non_events"].cumsum()
    df["cum_population"] = df["count"].cumsum()
    df["cumulative_decile_precision"] = np.where(
        df["cum_population"] > 0, df["cum_events"] / df["cum_population"], 0.0
    )
    df["cumulative_decile_recall"] = df["cum_events"] / total_ev
    df["cum_events_pct"] = df["cum_events"] / total_ev
    df["cum_nonevents_pct"] = df["cum_nonevents"] / total_nev
    df["cum_population_pct"] = df["cum_population"] / n
    df["lift"] = df["event_rate"] / overall_er
    df["cum_event_rate"] = df["cum_events"] / df["cum_population"]
    df["cum_lift"] = df["cum_event_rate"] / overall_er
    df["ks"] = (df["cum_events_pct"] - df["cum_nonevents_pct"]).abs()

    ks_stat = df["ks"].max()
    ks_decile = int(df.loc[df["ks"].idxmax(), "decile"])
    ks50 = ks_at_coverage(y_true, proba, coverage=0.50)["ks"]

    # --- print table ---
    print("\n" + "=" * 80)
    print("DECILE / LIFT / KS TABLE  (test set, population sorted by descending score)")
    print("=" * 80)
    display_cols = [
        "decile",
        "count",
        "events",
        "event_rate",
        "decile_precision",
        "decile_recall",
        "cumulative_decile_precision",
        "cumulative_decile_recall",
        "cum_events_pct",
        "cum_population_pct",
        "lift",
        "cum_lift",
        "ks",
    ]
    fmts = {
        "event_rate": "{:.2%}",
        "decile_precision": "{:.2%}",
        "decile_recall": "{:.2%}",
        "cumulative_decile_precision": "{:.2%}",
        "cumulative_decile_recall": "{:.2%}",
        "cum_events_pct": "{:.2%}",
        "cum_population_pct": "{:.2%}",
        "lift": "{:.2f}",
        "cum_lift": "{:.2f}",
        "ks": "{:.4f}",
    }
    disp = df[display_cols].copy()
    for col, fmt in fmts.items():
        disp[col] = disp[col].apply(lambda v: fmt.format(v))
    print(disp.to_string(index=False))
    print(f"\nKS statistic = {ks_stat:.4f}  (peak at decile {ks_decile})")
    print(f"KS@50% coverage = {ks50:.4f}")
    print("=" * 80)

    # --- save CSV ---
    decile_path = output_dir / "decile_lift_ks.csv"
    df.to_csv(decile_path, index=False)
    print(f"  Decile table → {decile_path}")

    # --- plots ---
    _plot_ks_lift(df, ks_stat, ks_decile, overall_er, output_dir)

    return df


def _plot_ks_lift(df, ks_stat, ks_decile, overall_er, output_dir: Path):
    """Generate and save three diagnostic charts."""
    plt.style.use("seaborn-v0_8-whitegrid")

    # ---- 1. Event rate per decile (bar) ----
    fig, ax = plt.subplots(figsize=(10, 5))
    colors = ["#e74c3c" if r["lift"] >= 1 else "#3498db" for _, r in df.iterrows()]
    ax.bar(
        df["decile"], df["event_rate"] * 100, color=colors, edgecolor="white", width=0.7
    )
    ax.axhline(
        overall_er * 100,
        color="black",
        linestyle="--",
        linewidth=1.2,
        label=f"Overall event rate ({overall_er:.1%})",
    )
    ax.set_xlabel("Decile (1 = highest predicted risk)", fontsize=11)
    ax.set_ylabel("Actual Event Rate (%)", fontsize=11)
    ax.set_title("Event Rate by Score Decile", fontsize=13, fontweight="bold")
    ax.set_xticks(df["decile"])
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter("%.1f%%"))
    ax.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig(output_dir / "decile_event_rate.png", dpi=150, bbox_inches="tight")
    plt.close("all")

    # ---- 2. Cumulative Lift curve ----
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(
        df["cum_population_pct"] * 100,
        df["cum_lift"],
        marker="o",
        color="#2ecc71",
        linewidth=2,
        label="Cumulative Lift",
    )
    ax.axhline(1.0, color="grey", linestyle="--", linewidth=1, label="Random baseline")
    ax.set_xlabel("% Population Contacted (cumulative)", fontsize=11)
    ax.set_ylabel("Cumulative Lift", fontsize=11)
    ax.set_title("Cumulative Lift Curve", fontsize=13, fontweight="bold")
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter("%.0f%%"))
    ax.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig(output_dir / "cumulative_lift.png", dpi=150, bbox_inches="tight")
    plt.close("all")

    # ---- 3. KS curve ----
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(
        df["cum_population_pct"] * 100,
        df["cum_events_pct"] * 100,
        marker="o",
        color="#e74c3c",
        linewidth=2,
        label="Cumulative Events %",
    )
    ax.plot(
        df["cum_population_pct"] * 100,
        df["cum_nonevents_pct"] * 100,
        marker="s",
        color="#3498db",
        linewidth=2,
        label="Cumulative Non-Events %",
    )
    # KS gap line
    kd = df[df["decile"] == ks_decile].iloc[0]
    ax.vlines(
        kd["cum_population_pct"] * 100,
        kd["cum_nonevents_pct"] * 100,
        kd["cum_events_pct"] * 100,
        colors="#f39c12",
        linewidth=2.5,
        label=f"KS = {ks_stat:.4f} @ decile {ks_decile}",
    )
    ax.set_xlabel("% Population (cumulative)", fontsize=11)
    ax.set_ylabel("Cumulative %", fontsize=11)
    ax.set_title("KS Chart", fontsize=13, fontweight="bold")
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter("%.0f%%"))
    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter("%.0f%%"))
    ax.legend(fontsize=10)
    plt.tight_layout()
    plt.savefig(output_dir / "ks_chart.png", dpi=150, bbox_inches="tight")
    plt.close("all")

    print(f"  Event-rate chart   → {output_dir / 'decile_event_rate.png'}")
    print(f"  Cumulative lift    → {output_dir / 'cumulative_lift.png'}")
    print(f"  KS chart           → {output_dir / 'ks_chart.png'}")


# ---------------------------------------------------------------------------
# Save artefacts
# ---------------------------------------------------------------------------


def save_artefacts(
    model,
    feature_names,
    label_encoders,
    metrics,
    output_dir,
    best_params=None,
    selected_features=None,
    run_config=None,
):
    output_dir.mkdir(parents=True, exist_ok=True)

    model_path = output_dir / "xgb_model.ubj"
    model.save_model(str(model_path))
    print(f"\nModel             → {model_path}")

    enc_path = output_dir / "label_encoders.pkl"
    with open(enc_path, "wb") as f:
        pickle.dump(label_encoders, f)
    print(f"Label encoders    → {enc_path}")

    booster = model.get_booster()
    scores = booster.get_score(importance_type="gain")
    imp = (
        pd.DataFrame([{"feature": k, "importance_gain": v} for k, v in scores.items()])
        .sort_values("importance_gain", ascending=False)
        .reset_index(drop=True)
    )
    imp_path = output_dir / "feature_importances.csv"
    imp.to_csv(imp_path, index=False)
    print(f"Feature importances → {imp_path}")

    meta = dict(
        target=TARGET_COL,
        split_strategy="temporal (CUTOFF_DATE)",
        n_features=len(feature_names),
        selected_features=selected_features or feature_names,
        metrics=metrics,
        best_params=best_params or {},
        run_config=run_config or {},
        input_tables=COMBINED_FEATURES_TABLES,
    )
    meta_path = output_dir / "metadata.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"Metadata          → {meta_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_step(step_name: str, fn, *args, **kwargs):
    """Run a pipeline step with timing + consistent logs."""
    t0 = time.time()
    print(f"\n[STEP] {step_name} ...", flush=True)
    out = fn(*args, **kwargs)
    print(f"[STEP] {step_name} done in {time.time() - t0:.1f}s", flush=True)
    return out


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)

    # 1. Load
    df = run_step(
        "load_data",
        load_data,
        local_csv=args.local_csv,
        local_parquet=args.local_parquet,
        cache_parquet=args.cache_parquet,
        refresh_cache=args.refresh_cache,
        load_join=args.load_join,
    )

    # 2. Lightweight raw cleanup on the full frame
    df = run_step("preprocess", prepare_raw_dataframe, df)

    if args.sample:
        df = df.nsmallest(min(args.sample, len(df)), DATE_COL).copy()
        print(f"Sampled first {len(df):,} rows by date.")

    # 3. Temporal split first, then preprocess each partition separately
    train_raw, val_raw, test_raw = run_step("temporal_split", temporal_split_raw_df, df)
    del df

    print("\n[STEP] preprocess_partitions ...", flush=True)
    X_train, y_train, d_train, category_levels = preprocess_partition(
        train_raw,
        fit_categories=True,
    )
    del train_raw
    X_val, y_val, d_val, _ = preprocess_partition(
        val_raw,
        category_levels=category_levels,
        fit_categories=False,
    )
    del val_raw
    X_test, y_test, d_test, _ = preprocess_partition(
        test_raw,
        category_levels=category_levels,
        fit_categories=False,
    )
    del test_raw
    label_encoders = {col: "category" for col in category_levels}
    print("[STEP] preprocess_partitions done", flush=True)

    # 3b. Drop high-null columns based on TRAIN only, then align val/test
    print("\n[STEP] drop_high_null_cols ...", flush=True)
    keep_cols = drop_high_null_cols(X_train, threshold=_MAX_NULL_RATE).columns.tolist()
    X_train = X_train[keep_cols]
    X_val = align_columns_to_train(X_train, X_val)
    X_test = align_columns_to_train(X_train, X_test)
    print(f"[STEP] drop_high_null_cols done — {X_train.shape[1]} train features remain", flush=True)

    # 3c. Feature engineering split-wise, aligned to train schema
    print("\n[STEP] engineer_features ...", flush=True)
    X_train = engineer_features(X_train)
    X_val = align_columns_to_train(X_train, engineer_features(X_val))
    X_test = align_columns_to_train(X_train, engineer_features(X_test))
    print(f"[STEP] engineer_features done — {X_train.shape[1]} train features total", flush=True)

    all_feature_names = list(X_train.columns)

    # Class weight (train only)
    neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
    scale_pos_weight = neg / max(pos, 1)
    print(
        f"\nClass balance (train) → neg:{neg:,}  pos:{pos:,}  "
        f"scale_pos_weight:{scale_pos_weight:.2f}"
    )

    # 4. Feature selection (on train only — no val/test leakage)
    selected_features = all_feature_names
    if not args.no_feature_sel:
        selected_features = select_features(
            X_train,
            y_train,
            top_n=args.top_n_features,
            output_dir=output_dir,
        )
        X_train = X_train[selected_features]
        X_val = X_val[selected_features]
        X_test = X_test[selected_features]
        print(f"  Using {len(selected_features)} features for HPO + final model.")

    # 5. Optuna HPO
    if args.no_optuna:
        best_params = default_params(scale_pos_weight)
        print("\nSkipping Optuna — default hyper-params.")
    else:
        print(
            f"\nOptuna: {args.n_trials} trials | "
            f"{args.n_cv_folds} TimeSeriesSplit folds | "
            f"HPO subsample={args.hpo_subsample:.0%} | "
            f"timeout={args.timeout}s | "
            f"objective={args.hpo_objective}"
        )
        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=RANDOM_SEED),
            pruner=optuna.pruners.MedianPruner(n_warmup_steps=5),
            study_name="xgb_risk_model",
        )
        objective = build_objective(
            X_train,
            y_train,
            n_cv_folds=args.n_cv_folds,
            hpo_subsample=args.hpo_subsample,
            # Use hpo_precision_floor (default 0) inside HPO for pure F1 maximisation.
            # The final validation threshold uses the stricter event_precision_floor.
            event_precision_floor=args.hpo_precision_floor,
            optimize_metric=args.hpo_objective,
            optimize_coverage=args.hpo_coverage,
        )
        study.optimize(
            objective,
            n_trials=args.n_trials,
            timeout=args.timeout,
            show_progress_bar=True,
        )

        print(f"\nBest trial   : #{study.best_trial.number}")
        if args.hpo_objective == "ks_at_coverage":
            print(
                f"Best CV KS@{args.hpo_coverage:.0%}: {study.best_value:.4f} "
                f"(higher is better)"
            )
        else:
            print(f"Best CV F1+  : {study.best_value:.4f} (positive class)")
        print(f"Best params : {study.best_params}")

        best_params = default_params(scale_pos_weight)
        best_params.update(study.best_params)

        output_dir.mkdir(parents=True, exist_ok=True)
        study.trials_dataframe().to_csv(output_dir / "optuna_trials.csv", index=False)
        print(f"Optuna trials → {output_dir / 'optuna_trials.csv'}")

    # 6. Final model on train+val; early stopping against val
    print("\nFinal model (train + val) …")
    X_tv = pd.concat([X_train, X_val])
    y_tv = pd.concat([y_train, y_val])

    final_params = {
        k: v for k, v in best_params.items() if k != "early_stopping_rounds"
    }
    final_params["early_stopping_rounds"] = 50

    model = train_final_model(X_tv, y_tv, X_val, y_val, final_params)

    # 7. Tune event threshold on validation set, then evaluate on future test set
    val_proba = model.predict_proba(X_val)[:, 1]
    threshold_info = tune_event_threshold(
        y_val, val_proba, precision_floor=args.event_precision_floor
    )
    threshold_scores = threshold_info.pop("scores")
    threshold_scores_path = output_dir / "threshold_precision_recall.csv"
    threshold_scores.to_csv(threshold_scores_path, index=False)
    chosen_threshold = threshold_info["threshold"]
    print("\nThreshold tuning (validation set):")
    print(
        f"  threshold={chosen_threshold:.3f} | "
        f"precision={threshold_info['precision']:.4f} | "
        f"recall={threshold_info['recall']:.4f} | "
        f"f1={threshold_info['f1']:.4f} | "
        f"precision_floor={threshold_info['used_precision_floor']:.2f} | "
        f"constraint_satisfied={threshold_info['constraint_satisfied']}"
    )
    print(f"  Threshold metrics → {threshold_scores_path}")

    metrics, test_proba = evaluate(
        model, X_test, y_test, d_test=d_test, threshold=chosen_threshold
    )
    metrics["validation_threshold_tuning"] = threshold_info

    # 8. Decile / Lift / KS analysis (test set)
    run_decile_ks_analysis(y_test, test_proba, output_dir)
    metrics["ks_at_50_test"] = ks_at_coverage(y_test, test_proba, coverage=0.50)["ks"]
    metrics["ks_at_hpo_coverage_test"] = ks_at_coverage(
        y_test, test_proba, coverage=args.hpo_coverage
    )["ks"]

    # 9. Save core artefacts (after adding KS metrics)
    save_artefacts(
        model=model,
        feature_names=all_feature_names,
        label_encoders=label_encoders,
        metrics=metrics,
        output_dir=output_dir,
        best_params=best_params,
        selected_features=selected_features,
        run_config={
            "local_csv": args.local_csv,
            "local_parquet": args.local_parquet,
            "cache_parquet": args.cache_parquet,
            "refresh_cache": args.refresh_cache,
            "n_trials": args.n_trials,
            "timeout": args.timeout,
            "n_cv_folds": args.n_cv_folds,
            "hpo_subsample": args.hpo_subsample,
            "top_n_features": args.top_n_features,
            "no_feature_sel": args.no_feature_sel,
            "no_optuna": args.no_optuna,
            "sample": args.sample,
            "event_precision_floor": args.event_precision_floor,
            "hpo_precision_floor": args.hpo_precision_floor,
            "hpo_objective": args.hpo_objective,
            "hpo_coverage": args.hpo_coverage,
            "load_join": args.load_join,
        },
    )

    # 10. SHAP analysis (test set)
    run_shap_analysis(model, X_test, output_dir)

    print("\nDone! ✓")
    print(f"All outputs in: {output_dir.resolve()}")


if __name__ == "__main__":
    main()
