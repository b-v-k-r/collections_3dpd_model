"""
run_pipeline.py
===============
Executes feature pipeline SQL files against Snowflake.

    1. activity_features.sql      – day-level activity features
    2. bureau_features.sql         – bureau features
    3. ledger_features.sql         – ledger features
    4. transactional_features.sql  – SMS / bank-account / CC txn features
    5. renewal_features.sql        – current-loan DPD & repayment features (30d lookback)
    6. aa_features.sql             – Account-Aggregator transaction features (30d lookback)

Each file starts with a `SET base_tbl = '...';` variable declaration
followed by one or more CREATE TABLE statements.  Because the Snowflake
Python connector only accepts one statement per cursor.execute() call, the
script splits each file on ";" and executes statements one-by-one.

Usage
-----
    # Run pipeline with the default base table:
    python run_pipeline.py

    # Override the base table at runtime:
    python run_pipeline.py --base-tbl analytics.data_science.my_custom_base
"""

import sys
import re
import time
import argparse
from pathlib import Path

# ── Base table used across all three pipeline SQL files ──────────────────────
# Change this value here to point all pipelines at a different table.
BASE_TBL = "analytics.data_science.early_dpd_base_2dpd"

# ── The db_service/config modules live in this repo root ─────────────────
# Add this repository root to sys.path so imports resolve locally.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
import db_service  # noqa: E402
from db_service import execute_query  # noqa: E402

# ── SQL files ───────────
PIPELINE_DIR = Path(__file__).resolve().parent
SQL_FILES = [
    PIPELINE_DIR / "activity_features.sql",
    PIPELINE_DIR / "bureau_features.sql",
    PIPELINE_DIR / "ledger_features.sql",
    PIPELINE_DIR / "transactional_features.sql",
    PIPELINE_DIR / "renewal_features.sql",
    PIPELINE_DIR / "aa_features.sql",
    PIPELINE_DIR / "ai_calling_features.sql",
    PIPELINE_DIR / "legal_automations.sql",
]

# Regex to detect lines that are purely SQL comments (-- …)
_COMMENT_ONLY = re.compile(r"^\s*--")
_CREATE_TABLE_RE = re.compile(
    r"(?i)\bcreate\s+or\s+replace\s+(?:transient\s+)?table\s+([A-Za-z0-9_.]+)"
)


def normalize_parser_suffix(parser_name: str | None) -> str | None:
    if parser_name is None:
        return None
    suffix = re.sub(r"[^A-Za-z0-9_]+", "_", parser_name.strip())
    suffix = re.sub(r"_+", "_", suffix).strip("_").lower()
    return suffix or None


def append_parser_suffix(table_name: str, parser_suffix: str | None) -> str:
    if not parser_suffix:
        return table_name
    parts = table_name.split(".")
    leaf = parts[-1]
    suffix = f"_{parser_suffix}"
    if leaf.lower().endswith(suffix.lower()):
        return table_name
    parts[-1] = f"{leaf}{suffix}"
    return ".".join(parts)


def extract_created_tables(sql: str) -> list[str]:
    return [
        table_name
        for match in _CREATE_TABLE_RE.finditer(sql)
        if re.search(r"[A-Za-z0-9]", (table_name := match.group(1)))
    ]


def replace_table_references(sql: str, table_name_map: dict[str, str] | None) -> str:
    if not table_name_map:
        return sql
    updated = sql
    for original, replacement in sorted(
        table_name_map.items(), key=lambda kv: len(kv[0]), reverse=True
    ):
        updated = re.sub(rf"(?i)\b{re.escape(original)}\b", replacement, updated)
    return updated


def build_table_name_map(
    sql_paths: list[Path], parser_suffix: str | None
) -> dict[str, str]:
    if not parser_suffix:
        return {}
    created_tables: dict[str, str] = {}
    for path in sql_paths:
        for table_name in extract_created_tables(path.read_text()):
            created_tables.setdefault(
                table_name, append_parser_suffix(table_name, parser_suffix)
            )
    return created_tables


def apply_warehouse_override(warehouse: str | None) -> None:
    if not warehouse:
        return
    db_service.SNOWFLAKE_CONFIG["warehouse"] = warehouse
    db_service._connection_pool.close_all()
    print(f"Warehouse override: {warehouse}")


def is_runnable(stmt: str) -> bool:
    """Return True if the statement is non-empty, not purely comments,
    and not a SET variable declaration (handled by Python-level substitution)."""
    lines = [l for l in stmt.splitlines() if l.strip()]
    if not lines:
        return False
    non_comment_lines = [l for l in lines if not _COMMENT_ONLY.match(l)]
    if not non_comment_lines:
        return False
    # Skip SET statements — Python already injects the base table via regex;
    # SET only makes sense inside a Snowflake worksheet session.
    first_token = non_comment_lines[0].strip().split()[0].lower()
    if first_token == "set":
        return False
    return True


def split_statements(sql: str) -> list[str]:
    """Split raw SQL text on semicolons, returning only runnable statements."""
    return [s.strip() for s in sql.split(";") if is_runnable(s.strip())]


def run_file(
    path: Path,
    base_tbl: str | None = None,
    table_name_map: dict[str, str] | None = None,
) -> None:
    sql = path.read_text()

    # Optionally override the base table variable before splitting
    if base_tbl:
        # 1) Replace the SET declaration line (keeps the SQL file self-contained
        #    for use in a Snowflake worksheet)
        sql = re.sub(
            r"(?i)(set\s+base_tbl\s*=\s*')[^']*(')",
            rf"\g<1>{base_tbl}\g<2>",
            sql,
        )
        # 2) Replace every identifier($base_tbl) in the query body with the
        #    literal table name — this is what actually matters during Python
        #    execution, since we skip the SET statement.
        sql = re.sub(
            r"(?i)identifier\(\s*\$base_tbl\s*\)",
            base_tbl,
            sql,
        )

    sql = replace_table_references(sql, table_name_map)

    statements = split_statements(sql)
    total = len(statements)

    print(f"\n{'=' * 60}")
    print(f"  File : {path.name}")
    print(f"  Statements to execute: {total}")
    print(f"{'=' * 60}")

    for i, stmt in enumerate(statements, 1):
        # Short preview for the log (first non-comment line, max 100 chars)
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
            elapsed = time.time() - t0
            print(f"         ✓  Done in {elapsed:.1f}s")
        except Exception as exc:
            elapsed = time.time() - t0
            print(f"         ✗  FAILED after {elapsed:.1f}s: {exc}")
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Run feature pipeline")
    parser.add_argument(
        "--base-tbl",
        default=None,
        help=(
            "Override the base table for all pipelines "
            "(e.g. analytics.data_science.my_base). "
            "If omitted, the value inside each SQL file is used."
        ),
    )
    parser.add_argument(
        "--file",
        choices=[
            "activity",
            "bureau",
            "ledger",
            "transactional",
            "renewal",
            "aa",
            "ai_calling",
            "legal_automation",
            "all",
        ],
        default="all",
        help="Which pipeline(s) to run (default: all).",
    )
    parser.add_argument(
        "--parser",
        default=None,
        help="Suffix appended to CREATE OR REPLACE TABLE targets in the SQL pipeline.",
    )
    parser.add_argument(
        "--warehouse",
        default=None,
        help="Override the Snowflake warehouse for this run.",
    )
    args = parser.parse_args()

    file_map = {
        "activity": [PIPELINE_DIR / "activity_features.sql"],
        "bureau": [PIPELINE_DIR / "bureau_features.sql"],
        "ledger": [PIPELINE_DIR / "ledger_features.sql"],
        "transactional": [PIPELINE_DIR / "transactional_features.sql"],
        "renewal": [PIPELINE_DIR / "renewal_features.sql"],
        "aa": [PIPELINE_DIR / "aa_features.sql"],
        "ai_calling": [PIPELINE_DIR / "ai_calling_features.sql"],
        "legal_automation": [PIPELINE_DIR / "legal_automations.sql"],
        "all": SQL_FILES,
    }
    files_to_run = file_map[args.file]

    # CLI flag overrides the hardcoded constant; otherwise use BASE_TBL
    effective_base_tbl = args.base_tbl if args.base_tbl else BASE_TBL
    parser_suffix = normalize_parser_suffix(args.parser)
    table_name_map = build_table_name_map(files_to_run, parser_suffix)
    apply_warehouse_override(args.warehouse)

    overall_start = time.time()
    print(f"\nStarting pipeline — {len(files_to_run)} file(s) to execute.")
    print(f"Base table: {effective_base_tbl}")
    if parser_suffix:
        print(f"Parser suffix: {parser_suffix}")

    for sql_path in files_to_run:
        run_file(
            sql_path,
            base_tbl=effective_base_tbl,
            table_name_map=table_name_map,
        )

    total_elapsed = time.time() - overall_start
    print(f"\n{'=' * 60}")
    print(f"  All done in {total_elapsed / 60:.1f} min ({total_elapsed:.0f}s)")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
