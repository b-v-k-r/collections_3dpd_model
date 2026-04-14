"""
Execute base.sql against Snowflake, one statement at a time.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db_service
from db_service import execute_query

BASE_SQL_PATH = PROJECT_ROOT / "base.sql"
_COMMENT_ONLY = re.compile(r"^\s*--")


def apply_warehouse_override(warehouse: str | None) -> None:
    if not warehouse:
        return
    db_service.SNOWFLAKE_CONFIG["warehouse"] = warehouse
    db_service._connection_pool.close_all()
    print(f"Warehouse override: {warehouse}", flush=True)


def is_runnable(stmt: str) -> bool:
    lines = [line for line in stmt.splitlines() if line.strip()]
    if not lines:
        return False
    non_comment_lines = [line for line in lines if not _COMMENT_ONLY.match(line)]
    return bool(non_comment_lines)


def split_statements(sql: str) -> list[str]:
    return [stmt.strip() for stmt in sql.split(";") if is_runnable(stmt.strip())]


def run_sql_file(path: Path) -> None:
    statements = split_statements(path.read_text())
    total = len(statements)

    print(f"\n{'=' * 60}")
    print(f"  File : {path.name}")
    print(f"  Statements to execute: {total}")
    print(f"{'=' * 60}")

    for idx, stmt in enumerate(statements, 1):
        preview_lines = [
            line.strip()
            for line in stmt.splitlines()
            if line.strip() and not _COMMENT_ONLY.match(line.strip())
        ]
        preview = (preview_lines[0][:100] + "...") if preview_lines else stmt[:100]
        print(f"\n  [{idx}/{total}] {preview}", flush=True)
        started_at = time.time()
        execute_query(stmt)
        print(f"         ✓  Done in {time.time() - started_at:.1f}s", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute base.sql in Snowflake")
    parser.add_argument(
        "--sql-file",
        default=str(BASE_SQL_PATH),
        help=f"SQL file to execute (default: {BASE_SQL_PATH})",
    )
    parser.add_argument(
        "--warehouse",
        default=None,
        help="Override the Snowflake warehouse for this run.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    apply_warehouse_override(args.warehouse)
    run_sql_file(Path(args.sql_file))


if __name__ == "__main__":
    main()
