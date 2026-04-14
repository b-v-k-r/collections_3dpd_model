from __future__ import annotations

import os
import sys

# Add project root to sys.path for imports
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from db_service import execute_query, fetch_data

tables = {
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
base_table = "analytics.data_science.early_dpd_base_2dpd"

TARGET_TABLE = "analytics.data_science.early_dpd2_combined_features"


def get_columns(table_name):
    """Return ordered column names for a table using data/metadata fallbacks."""
    df = fetch_data(f"SELECT * FROM {table_name} LIMIT 1")
    cols = list(df.columns)
    if cols:
        return cols

    # Fallback: fetch schema from Snowflake metadata when the table has 0 rows.
    # (Our fetch_data() fallback path returns an empty DataFrame with no columns.)
    metadata_queries = [
        f"SHOW COLUMNS IN TABLE {table_name}",
        f"DESC TABLE {table_name}",
    ]
    for query in metadata_queries:
        meta_df = fetch_data(query)
        if meta_df.empty:
            continue
        name_col = next(
            (
                c
                for c in ("name", "NAME", "column_name", "COLUMN_NAME")
                if c in meta_df.columns
            ),
            None,
        )
        if not name_col:
            continue
        meta_cols = [c for c in meta_df[name_col].tolist() if isinstance(c, str) and c]
        if meta_cols:
            return meta_cols

    raise ValueError(f"Could not determine columns for table '{table_name}'.")


def quote_ident(identifier: str) -> str:
    """Safely quote a Snowflake identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def normalize_col_name(col_name: str) -> str:
    """Normalize a column name for case/format-insensitive matching."""
    return "".join(ch for ch in col_name.upper() if ch.isalnum())


def resolve_join_column(requested_col: str, available_cols: list[str]) -> str | None:
    """
    Resolve a requested join key against available columns using:
    1) exact case-insensitive match
    2) normalized match (ignore underscores/symbols)
    3) common aliases
    4) conservative heuristic fallback
    """
    available_upper = {col.upper(): col for col in available_cols}
    req_upper = requested_col.upper()
    if req_upper in available_upper:
        return available_upper[req_upper]

    normalized_map = {}
    for col in available_cols:
        norm = normalize_col_name(col)
        if norm not in normalized_map:
            normalized_map[norm] = col

    req_norm = normalize_col_name(requested_col)
    if req_norm in normalized_map:
        return normalized_map[req_norm]

    alias_map = {
        "USER_ID": ["USERID", "CUSTOMER_ID", "CUSTOMERID", "KB_ID", "PROFILE_ID"],
        "CUTOFF_DATE": ["CUTOFFDATE", "CUTOFF_DT", "AS_OF_DATE", "SNAPSHOT_DATE"],
    }
    for alias in alias_map.get(req_upper, []):
        if alias in available_upper:
            return available_upper[alias]
        alias_norm = normalize_col_name(alias)
        if alias_norm in normalized_map:
            return normalized_map[alias_norm]

    if req_upper == "USER_ID":
        user_like = [
            col
            for col in available_cols
            if "USER" in col.upper() and "ID" in col.upper()
        ]
        if len(user_like) == 1:
            return user_like[0]
    elif req_upper == "CUTOFF_DATE":
        cutoff_like = [
            col
            for col in available_cols
            if "CUTOFF" in col.upper()
            and (
                "DATE" in col.upper()
                or col.upper().endswith("_DT")
                or col.upper().endswith("DT")
            )
        ]
        if len(cutoff_like) == 1:
            return cutoff_like[0]

    return None


def features_combinator(
    base_table,
    tables,
    target_table=TARGET_TABLE,
    join_columns=["cutoff_date", "user_id"],
):
    """
    Build and execute a CREATE OR REPLACE TABLE ... AS SELECT query that
    LEFT JOINs every table in `tables` onto `base_table`, writing the
    result directly to `target_table` in Snowflake (no DataFrame created).

    Dynamically excludes any column from joined tables that already
    exists in base_table (or prior joins), preventing duplicate column
    name errors in CTAS output.

    Args:
        base_table (str): Fully-qualified base table name.
        tables (dict): Mapping of alias -> fully-qualified table name.
        target_table (str): Destination fully-qualified table name.
        join_columns (list): Columns to join on (present in all tables).
    """
    # Discover base columns once
    print("Fetching base table columns...", flush=True)
    base_cols = get_columns(base_table)
    base_col_map = {}
    for col in base_cols:
        col_upper = col.upper()
        if col_upper not in base_col_map:
            base_col_map[col_upper] = col

    # Build explicit select list while tracking UPPER-cased names already emitted.
    # This avoids duplicate output names when sources differ only by identifier casing.
    seen_cols: set[str] = set(base_col_map)  # tracks UPPER-cased columns added so far
    select_parts = []
    join_parts = []

    for alias, table_name in tables.items():
        print(f"Fetching columns for {alias} ({table_name})...", flush=True)
        table_cols = get_columns(table_name)
        table_col_map = {}
        for col in table_cols:
            col_upper = col.upper()
            if col_upper not in table_col_map:
                table_col_map[col_upper] = col

        # Keep only not-yet-seen output column names (case-insensitive).
        # Also skip duplicate names within the same joined table after upper-casing.
        selected_count = 0
        skipped_count = 0
        local_seen_upper = set()
        for col in table_cols:
            col_upper = col.upper()
            if col_upper in seen_cols or col_upper in local_seen_upper:
                skipped_count += 1
                continue
            local_seen_upper.add(col_upper)
            seen_cols.add(col_upper)
            select_parts.append(f"{alias}.{quote_ident(col)}")
            selected_count += 1

        if skipped_count:
            print(
                f"Skipping {skipped_count} duplicate columns from {alias}; "
                f"selecting {selected_count}.",
                flush=True,
            )

        resolved_join_pairs = []
        missing_join_cols = []
        for requested_col in join_columns:
            base_col = resolve_join_column(requested_col, base_cols)
            table_col = resolve_join_column(requested_col, table_cols)
            if not base_col or not table_col:
                missing_join_cols.append(requested_col)
                continue
            resolved_join_pairs.append((base_col, table_col))

        if missing_join_cols:
            key_like_cols = [
                col
                for col in table_cols
                if any(k in col.upper() for k in ("USER", "CUTOFF", "DATE", "DT", "ID"))
            ][:30]
            raise ValueError(
                f"Missing join columns {missing_join_cols} for table '{table_name}'. "
                f"Available key-like columns: {key_like_cols}"
            )

        on_conditions = " AND ".join(
            f"base.{quote_ident(base_col)} = {alias}.{quote_ident(table_col)}"
            for base_col, table_col in resolved_join_pairs
        )
        join_parts.append(f"LEFT JOIN {table_name} AS {alias} ON {on_conditions}")

    join_clauses = "\n    ".join(join_parts)
    select_cols = ",\n    ".join(["base.*"] + select_parts)

    query = (
        f"CREATE OR REPLACE TABLE {target_table} AS\n"
        f"SELECT\n"
        f"    {select_cols}\n"
        f"FROM {base_table} AS base\n"
        f"    {join_clauses}\n"
    )

    print("Executing CTAS query...", flush=True)
    execute_query(query)
    print(f"Table '{target_table}' created/replaced successfully.")


# Due to Snowflake's soft limit on the number of column objects per table (~1600 to 2000),
# we split the `tables` dictionary into chunks to keep the output table width within limits.
# chunk 1: ~1480 columns
tables_part1 = {
    "collect_dbt": tables["collect_dbt"],
    "sms_final": tables["sms_final"],
}
# chunk 2: ~840 columns
tables_part2 = {
    "inapp": tables["inapp"],
    "activity": tables["activity"],
}
# chunk 3: ~1623 columns (bureau itself is 1595)
tables_part3 = {
    "bureau": tables["bureau"],
}
# chunk 4: ledger features (kept separate to avoid table-width issues)
tables_part4 = {
    "ledger": tables["ledger"],
}

# chunk 5: transactional SMS / bank-account / CC features
tables_part5 = {
    "transactional": tables["transactional"],
}
# chunk 6: renewal / DPD features (loan-level, 30-day lookback)
tables_part6 = {
    "renewal": tables["renewal"],
}
# chunk 7: Account-Aggregator transaction features
tables_part7 = {
    "aa": tables["aa"],
}
# chunk 8: historical AI-calling features
tables_part8 = {
    "ai_calling": tables["ai_calling"],
}
# chunk 9: historical legal-automation features
tables_part9 = {
    "legal_automation": tables["legal_automation"],
}

parts = [
    ("Part 1", tables_part1, "_part1"),
    ("Part 2", tables_part2, "_part2"),
    # Run ledger before bureau so ledger is always generated even if bureau is slow/fails.
    ("Part 4", tables_part4, "_part4"),
    ("Part 3", tables_part3, "_part3"),
    ("Part 5", tables_part5, "_part5"),
    ("Part 6", tables_part6, "_part6"),
    ("Part 7", tables_part7, "_part7"),
    ("Part 8", tables_part8, "_part8"),
    ("Part 9", tables_part9, "_part9"),
]

for part_name, part_tables, suffix in parts:
    print(f"\n--- Processing {part_name} ---")
    features_combinator(base_table, part_tables, target_table=TARGET_TABLE + suffix)
