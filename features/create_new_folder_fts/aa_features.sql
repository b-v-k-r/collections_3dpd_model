-- aa_features.sql
-- ============================================================
-- Produces one row per (USER_ID, CUTOFF_DATE) with windowed
-- Account-Aggregator transaction features derived from
-- app_backend.loan_service_prod.public_account_aggregator_insights_vw.
--
-- Pattern matches activity_features.sql / bureau_features.sql:
--   • SET base_tbl  — overridden at runtime by run_pipeline.py
--   • identifier($base_tbl) — resolves via Python regex substitution
-- ============================================================

set base_tbl = 'analytics.data_science.early_dpd3_base';

create or replace table analytics.data_science.aa_features_for_early_dpd3 as

with base as (
    select
        user_id,
        cutoff_date
    from identifier($base_tbl)
    where user_id is not null
      and cutoff_date is not null
),

raw_txns as (
    select
        b.user_id,
        b.cutoff_date,
        try_cast(f.value:amount::text as double)          as amount,
        try_cast(f.value:currentBalance::text as double)  as current_balance,
        f.value:mode::text                                as mode,
        to_date(f.value:transactionTimestamp)             as txn_date,
        f.value:type::text                                as txn_type
    from base b
    join (
        select
            a.user_id,
            b2.cutoff_date,
            a.created_at,
            a.data,
            row_number() over (
                partition by a.user_id, b2.cutoff_date
                order by a.created_at desc
            ) as rn
        from base b2
        join app_backend.loan_service_prod.public_account_aggregator_insights_vw a
            on  b2.user_id = a.user_id
            and date(a.created_at) <= b2.cutoff_date
    ) a
        on  b.user_id     = a.user_id
        and b.cutoff_date = a.cutoff_date
        and a.rn = 1
    , lateral flatten(input => parse_json(a.data):data:account) acc
    , lateral flatten(input => acc.value) f
    where acc.value is not null
      and to_date(f.value:transactionTimestamp) between dateadd(day, -30, b.cutoff_date)
                                                     and dateadd(day,  -1, b.cutoff_date)
      and to_date(f.value:transactionTimestamp) < b.cutoff_date   -- strictly no future leakage
),

mapped_txns as (
    select
        user_id,
        cutoff_date,
        txn_date,
        amount,
        current_balance,
        case
            when mode = 'CHECK' then 'cheque'
            when mode = 'CASH'  then 'cash'
            when mode = 'UPI'   then 'upi'
            when mode = 'ATM'   then 'atm'
            when mode in ('FT','IB') then 'rtgs'
            else 'NA'
        end as mode_transac,
        case when txn_type = 'DEBIT'  then amount end as debit_amount,
        case when txn_type = 'CREDIT' then amount end as credit_amount
    from raw_txns
),

day_agg as (
    select
        user_id,
        cutoff_date,
        txn_date,
        count(*)                                         as num_total_txns,
        sum(case when credit_amount > 0 then 1 else 0 end) as num_credit_txns,
        sum(case when debit_amount  > 0 then 1 else 0 end) as num_debit_txns,

        sum(credit_amount)                               as total_credit_amt,
        sum(debit_amount)                                as total_debit_amt,
        max(credit_amount)                               as max_credit_amt,
        max(debit_amount)                                as max_debit_amt,
        avg(nullif(credit_amount, 0))                    as avg_credit_amt,
        avg(nullif(debit_amount, 0))                     as avg_debit_amt,

        min(current_balance)                             as min_balance,
        max(current_balance)                             as max_balance,
        avg(current_balance)                             as avg_balance,

        -- by mode — counts
        sum(case when mode_transac = 'upi'   and debit_amount > 0 then 1 else 0 end) as num_debit_txns_upi,
        sum(case when mode_transac = 'atm'   and debit_amount > 0 then 1 else 0 end) as num_debit_txns_atm,
        sum(case when mode_transac in ('rtgs','neft','imps') and debit_amount > 0 then 1 else 0 end) as num_debit_txns_bank_transfer,
        sum(case when mode_transac = 'cash'  and debit_amount > 0 then 1 else 0 end) as num_debit_txns_cash,
        sum(case when mode_transac = 'cheque' and debit_amount > 0 then 1 else 0 end) as num_debit_txns_cheque,
        sum(case when mode_transac = 'upi'   and credit_amount > 0 then 1 else 0 end) as num_credit_txns_upi,
        sum(case when mode_transac = 'atm'   and credit_amount > 0 then 1 else 0 end) as num_credit_txns_atm,
        sum(case when mode_transac in ('rtgs','neft','imps') and credit_amount > 0 then 1 else 0 end) as num_credit_txns_bank_transfer,
        sum(case when mode_transac = 'cash'  and credit_amount > 0 then 1 else 0 end) as num_credit_txns_cash,
        sum(case when mode_transac = 'cheque' and credit_amount > 0 then 1 else 0 end) as num_credit_txns_cheque,

        -- by mode — amounts
        sum(case when mode_transac = 'upi'   and debit_amount > 0 then debit_amount else 0 end) as total_debit_amt_upi,
        sum(case when mode_transac = 'atm'   and debit_amount > 0 then debit_amount else 0 end) as total_debit_amt_atm,
        sum(case when mode_transac in ('rtgs','neft','imps') and debit_amount > 0 then debit_amount else 0 end) as total_debit_amt_bank_transfer,
        sum(case when mode_transac = 'cash'  and debit_amount > 0 then debit_amount else 0 end) as total_debit_amt_cash,
        sum(case when mode_transac = 'cheque' and debit_amount > 0 then debit_amount else 0 end) as total_debit_amt_cheque,
        sum(case when mode_transac = 'upi'   and credit_amount > 0 then credit_amount else 0 end) as total_credit_amt_upi,
        sum(case when mode_transac = 'atm'   and credit_amount > 0 then credit_amount else 0 end) as total_credit_amt_atm,
        sum(case when mode_transac in ('rtgs','neft','imps') and credit_amount > 0 then credit_amount else 0 end) as total_credit_amt_bank_transfer,
        sum(case when mode_transac = 'cash'  and credit_amount > 0 then credit_amount else 0 end) as total_credit_amt_cash,
        sum(case when mode_transac = 'cheque' and credit_amount > 0 then credit_amount else 0 end) as total_credit_amt_cheque
    from mapped_txns
    group by user_id, cutoff_date, txn_date
)

select
    user_id,
    cutoff_date,

    -- ── Transaction counts ─────────────────────────────────────────────
    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_total_txns else 0 end) as num_total_txns_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_total_txns else 0 end) as num_total_txns_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_total_txns else 0 end) as num_total_txns_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_credit_txns else 0 end) as num_credit_txns_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_credit_txns else 0 end) as num_credit_txns_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_credit_txns else 0 end) as num_credit_txns_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_debit_txns else 0 end) as num_debit_txns_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_debit_txns else 0 end) as num_debit_txns_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_debit_txns else 0 end) as num_debit_txns_16_to_30_d,

    -- ── Amounts ────────────────────────────────────────────────────────
    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_credit_amt else 0 end) as total_credit_amt_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_credit_amt else 0 end) as total_credit_amt_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_credit_amt else 0 end) as total_credit_amt_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_debit_amt else 0 end) as total_debit_amt_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_debit_amt else 0 end) as total_debit_amt_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_debit_amt else 0 end) as total_debit_amt_16_to_30_d,

    max(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then max_credit_amt end) as max_credit_amt_1_to_7_d,
    max(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then max_credit_amt end) as max_credit_amt_8_to_15_d,
    max(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then max_credit_amt end) as max_credit_amt_16_to_30_d,

    max(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then max_debit_amt end) as max_debit_amt_1_to_7_d,
    max(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then max_debit_amt end) as max_debit_amt_8_to_15_d,
    max(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then max_debit_amt end) as max_debit_amt_16_to_30_d,

    avg(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then avg_credit_amt end) as avg_credit_amt_1_to_7_d,
    avg(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then avg_credit_amt end) as avg_credit_amt_8_to_15_d,
    avg(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then avg_credit_amt end) as avg_credit_amt_16_to_30_d,

    avg(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then avg_debit_amt end) as avg_debit_amt_1_to_7_d,
    avg(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then avg_debit_amt end) as avg_debit_amt_8_to_15_d,
    avg(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then avg_debit_amt end) as avg_debit_amt_16_to_30_d,

    -- ── Balance ────────────────────────────────────────────────────────
    min(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then min_balance end) as min_balance_1_to_7_d,
    min(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then min_balance end) as min_balance_8_to_15_d,
    min(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then min_balance end) as min_balance_16_to_30_d,

    max(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then max_balance end) as max_balance_1_to_7_d,
    max(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then max_balance end) as max_balance_8_to_15_d,
    max(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then max_balance end) as max_balance_16_to_30_d,

    avg(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then avg_balance end) as avg_balance_1_to_7_d,
    avg(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then avg_balance end) as avg_balance_8_to_15_d,
    avg(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then avg_balance end) as avg_balance_16_to_30_d,

    -- ── Debit by mode — counts ─────────────────────────────────────────
    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_debit_txns_upi   else 0 end) as num_debit_txns_upi_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_debit_txns_upi   else 0 end) as num_debit_txns_upi_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_debit_txns_upi   else 0 end) as num_debit_txns_upi_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_debit_txns_atm   else 0 end) as num_debit_txns_atm_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_debit_txns_atm   else 0 end) as num_debit_txns_atm_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_debit_txns_atm   else 0 end) as num_debit_txns_atm_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_debit_txns_bank_transfer else 0 end) as num_debit_txns_bank_transfer_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_debit_txns_bank_transfer else 0 end) as num_debit_txns_bank_transfer_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_debit_txns_bank_transfer else 0 end) as num_debit_txns_bank_transfer_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_debit_txns_cash  else 0 end) as num_debit_txns_cash_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_debit_txns_cash  else 0 end) as num_debit_txns_cash_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_debit_txns_cash  else 0 end) as num_debit_txns_cash_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_debit_txns_cheque else 0 end) as num_debit_txns_cheque_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_debit_txns_cheque else 0 end) as num_debit_txns_cheque_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_debit_txns_cheque else 0 end) as num_debit_txns_cheque_16_to_30_d,

    -- ── Credit by mode — counts ────────────────────────────────────────
    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_credit_txns_upi   else 0 end) as num_credit_txns_upi_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_credit_txns_upi   else 0 end) as num_credit_txns_upi_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_credit_txns_upi   else 0 end) as num_credit_txns_upi_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_credit_txns_atm   else 0 end) as num_credit_txns_atm_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_credit_txns_atm   else 0 end) as num_credit_txns_atm_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_credit_txns_atm   else 0 end) as num_credit_txns_atm_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_credit_txns_bank_transfer else 0 end) as num_credit_txns_bank_transfer_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_credit_txns_bank_transfer else 0 end) as num_credit_txns_bank_transfer_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_credit_txns_bank_transfer else 0 end) as num_credit_txns_bank_transfer_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_credit_txns_cash  else 0 end) as num_credit_txns_cash_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_credit_txns_cash  else 0 end) as num_credit_txns_cash_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_credit_txns_cash  else 0 end) as num_credit_txns_cash_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then num_credit_txns_cheque else 0 end) as num_credit_txns_cheque_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then num_credit_txns_cheque else 0 end) as num_credit_txns_cheque_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_credit_txns_cheque else 0 end) as num_credit_txns_cheque_16_to_30_d,

    -- ── Debit by mode — amounts ────────────────────────────────────────
    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_debit_amt_upi   else 0 end) as total_debit_amt_upi_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_debit_amt_upi   else 0 end) as total_debit_amt_upi_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_debit_amt_upi   else 0 end) as total_debit_amt_upi_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_debit_amt_atm   else 0 end) as total_debit_amt_atm_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_debit_amt_atm   else 0 end) as total_debit_amt_atm_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_debit_amt_atm   else 0 end) as total_debit_amt_atm_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_debit_amt_bank_transfer else 0 end) as total_debit_amt_bank_transfer_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_debit_amt_bank_transfer else 0 end) as total_debit_amt_bank_transfer_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_debit_amt_bank_transfer else 0 end) as total_debit_amt_bank_transfer_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_debit_amt_cash  else 0 end) as total_debit_amt_cash_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_debit_amt_cash  else 0 end) as total_debit_amt_cash_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_debit_amt_cash  else 0 end) as total_debit_amt_cash_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_debit_amt_cheque else 0 end) as total_debit_amt_cheque_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_debit_amt_cheque else 0 end) as total_debit_amt_cheque_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_debit_amt_cheque else 0 end) as total_debit_amt_cheque_16_to_30_d,

    -- ── Credit by mode — amounts ───────────────────────────────────────
    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_credit_amt_upi   else 0 end) as total_credit_amt_upi_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_credit_amt_upi   else 0 end) as total_credit_amt_upi_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_credit_amt_upi   else 0 end) as total_credit_amt_upi_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_credit_amt_atm   else 0 end) as total_credit_amt_atm_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_credit_amt_atm   else 0 end) as total_credit_amt_atm_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_credit_amt_atm   else 0 end) as total_credit_amt_atm_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_credit_amt_bank_transfer else 0 end) as total_credit_amt_bank_transfer_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_credit_amt_bank_transfer else 0 end) as total_credit_amt_bank_transfer_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_credit_amt_bank_transfer else 0 end) as total_credit_amt_bank_transfer_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_credit_amt_cash  else 0 end) as total_credit_amt_cash_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_credit_amt_cash  else 0 end) as total_credit_amt_cash_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_credit_amt_cash  else 0 end) as total_credit_amt_cash_16_to_30_d,

    sum(case when txn_date between dateadd(day, -7, cutoff_date)  and dateadd(day, -1, cutoff_date)  then total_credit_amt_cheque else 0 end) as total_credit_amt_cheque_1_to_7_d,
    sum(case when txn_date between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date)  then total_credit_amt_cheque else 0 end) as total_credit_amt_cheque_8_to_15_d,
    sum(case when txn_date between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_credit_amt_cheque else 0 end) as total_credit_amt_cheque_16_to_30_d

from day_agg
group by user_id, cutoff_date;

-- Sanity check row count
select count(*) as total_rows
from analytics.data_science.aa_features_for_early_dpd3;
