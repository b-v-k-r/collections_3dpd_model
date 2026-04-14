-- transactional_features.sql
-- ============================================================
-- Produces one row per (USER_ID, CUTOFF_DATE) with windowed
-- SMS / bank-account / credit-card features derived from
-- analytics.data_science.sbi_hdfc_bob_fact_3_vw.
--
-- Pattern matches activity_features.sql / bureau_features.sql:
--   • SET base_tbl  — overridden at runtime by run_pipeline.py
--   • identifier($base_tbl) — resolves via Python regex substitution
-- ============================================================

set base_tbl = 'analytics.data_science.early_dpd3_base';

create or replace table analytics.data_science.transactional_features_for_early_dpd3 as

with base as (
    -- one row per (user_id, cutoff_date) from the training base
    select
        user_id,
        cutoff_date
    from identifier($base_tbl)
    where user_id is not null
      and cutoff_date is not null
),

raw_txns as (
    -- flatten the FACT JSON once -- keep only rows within the 30-day lookback
    select
        b.user_id,
        b.cutoff_date,
        f.device_id,
        f.template_id,
        date(f.date)                               as txn_dt,
        to_double(f.fact:debit_amount)             as debit_amount,
        to_double(f.fact:credit_amount)            as credit_amount,
        to_double(f.fact:available_balance)        as available_balance,
        to_double(f.fact:total_due_amount)         as total_due_amount,
        to_double(f.fact:available_limit)          as available_limit,
        lower(f.fact:mode_of_transaction::text)    as mode_transac,
        f.fact:category::text                      as category
    from base b
    join analytics.data_science.sbi_hdfc_bob_fact_3_vw f
        on  b.user_id = f.user_id
        and date(f.date) between dateadd(day, -30, b.cutoff_date)
                             and dateadd(day,  -1, b.cutoff_date)
),

day_agg as (
    -- aggregate to (user_id, cutoff_date, category, txn_dt) first so we
    -- can then pivot into three time windows without repeating the CASE logic
    select
        user_id,
        cutoff_date,
        txn_dt,
        -- SMS totals
        count(*)                                                             as num_total_sms,
        sum(case when template_id is not null then 1 else 0 end)            as num_readable_sms,

        -- ── credit-card signals ──────────────────────────────────────────
        max(case when category = 'credit_card' then total_due_amount end)   as max_due_amt_cc,
        min(case when category = 'credit_card' then total_due_amount end)   as min_due_amt_cc,
        max(case when category = 'credit_card' then available_limit end)    as max_avail_limit_cc,
        min(case when category = 'credit_card' then available_limit end)    as min_avail_limit_cc,

        -- CC credit txn counts by bucket
        sum(case when category='credit_card' and credit_amount between 1 and 99      then 1 else 0 end) as num_credit_txns_lt100_cc,
        sum(case when category='credit_card' and credit_amount between 100 and 499   then 1 else 0 end) as num_credit_txns_100_500_cc,
        sum(case when category='credit_card' and credit_amount between 500 and 1999  then 1 else 0 end) as num_credit_txns_500_2000_cc,
        sum(case when category='credit_card' and credit_amount between 2000 and 4999 then 1 else 0 end) as num_credit_txns_2000_5000_cc,
        sum(case when category='credit_card' and credit_amount between 5000 and 9999 then 1 else 0 end) as num_credit_txns_5000_10000_cc,
        sum(case when category='credit_card' and credit_amount >= 10000              then 1 else 0 end) as num_credit_txns_gt10000_cc,
        sum(case when category='credit_card' and credit_amount > 1                   then 1 else 0 end) as num_credit_txns_cc,

        -- CC debit txn counts by bucket
        sum(case when category='credit_card' and debit_amount between 1 and 99      then 1 else 0 end) as num_debit_txns_lt100_cc,
        sum(case when category='credit_card' and debit_amount between 100 and 499   then 1 else 0 end) as num_debit_txns_100_500_cc,
        sum(case when category='credit_card' and debit_amount between 500 and 1999  then 1 else 0 end) as num_debit_txns_500_2000_cc,
        sum(case when category='credit_card' and debit_amount between 2000 and 4999 then 1 else 0 end) as num_debit_txns_2000_5000_cc,
        sum(case when category='credit_card' and debit_amount between 5000 and 9999 then 1 else 0 end) as num_debit_txns_5000_10000_cc,
        sum(case when category='credit_card' and debit_amount >= 10000              then 1 else 0 end) as num_debit_txns_gt10000_cc,
        sum(case when category='credit_card' and debit_amount > 1                   then 1 else 0 end) as num_debit_txns_cc,

        -- CC amounts
        sum(case when category='credit_card' and credit_amount > 1 then credit_amount else 0 end) as total_credit_amt_cc,
        min(case when category='credit_card' and credit_amount > 1 then credit_amount end)         as min_credit_amt_cc,
        max(case when category='credit_card' and credit_amount > 1 then credit_amount end)         as max_credit_amt_cc,
        sum(case when category='credit_card' and debit_amount > 1  then debit_amount  else 0 end) as total_debit_amt_cc,
        min(case when category='credit_card' and debit_amount > 1  then debit_amount  end)         as min_debit_amt_cc,
        max(case when category='credit_card' and debit_amount > 1  then debit_amount  end)         as max_debit_amt_cc,

        -- ── bank-account signals ─────────────────────────────────────────
        max(case when category='bank_account' then available_balance end)   as max_avail_balance,
        min(case when category='bank_account' then available_balance end)   as min_avail_balance,
        sum(case when category='bank_account' then available_balance end)   as total_avail_balance,
        sum(case when category='bank_account' and available_balance is not null then 1 else 0 end) as num_avail_balance_obs,

        -- bank credit/debit count by bucket
        sum(case when category='bank_account' and credit_amount between 1 and 99      then 1 else 0 end) as num_credit_txns_lt100,
        sum(case when category='bank_account' and credit_amount between 100 and 499   then 1 else 0 end) as num_credit_txns_100_500,
        sum(case when category='bank_account' and credit_amount between 500 and 1999  then 1 else 0 end) as num_credit_txns_500_2000,
        sum(case when category='bank_account' and credit_amount between 2000 and 4999 then 1 else 0 end) as num_credit_txns_2000_5000,
        sum(case when category='bank_account' and credit_amount between 5000 and 9999 then 1 else 0 end) as num_credit_txns_5000_10000,
        sum(case when category='bank_account' and credit_amount >= 10000              then 1 else 0 end) as num_credit_txns_gt10000,
        sum(case when category='bank_account' and credit_amount > 1                   then 1 else 0 end) as num_credit_txns,

        sum(case when category='bank_account' and debit_amount between 1 and 99      then 1 else 0 end) as num_debit_txns_lt100,
        sum(case when category='bank_account' and debit_amount between 100 and 499   then 1 else 0 end) as num_debit_txns_100_500,
        sum(case when category='bank_account' and debit_amount between 500 and 1999  then 1 else 0 end) as num_debit_txns_500_2000,
        sum(case when category='bank_account' and debit_amount between 2000 and 4999 then 1 else 0 end) as num_debit_txns_2000_5000,
        sum(case when category='bank_account' and debit_amount between 5000 and 9999 then 1 else 0 end) as num_debit_txns_5000_10000,
        sum(case when category='bank_account' and debit_amount >= 10000              then 1 else 0 end) as num_debit_txns_gt10000,
        sum(case when category='bank_account' and debit_amount > 1                   then 1 else 0 end) as num_debit_txns,

        -- bank amounts (total)
        sum(case when category='bank_account' and credit_amount > 1 then credit_amount else 0 end) as total_credit_amt,
        min(case when category='bank_account' and credit_amount > 1 then credit_amount end)         as min_credit_amt,
        max(case when category='bank_account' and credit_amount > 1 then credit_amount end)         as max_credit_amt,
        sum(case when category='bank_account' and debit_amount > 1  then debit_amount  else 0 end) as total_debit_amt,
        min(case when category='bank_account' and debit_amount > 1  then debit_amount  end)         as min_debit_amt,
        max(case when category='bank_account' and debit_amount > 1  then debit_amount  end)         as max_debit_amt,

        -- bank by mode — counts
        sum(case when category='bank_account' and mode_transac='upi'                        and debit_amount>1  then 1 else 0 end) as num_debit_txns_upi,
        sum(case when category='bank_account' and mode_transac='atm'                        and debit_amount>1  then 1 else 0 end) as num_debit_txns_atm,
        sum(case when category='bank_account' and mode_transac in ('rtgs','neft','imps','dd') and debit_amount>1 then 1 else 0 end) as num_debit_txns_bank_transfer,
        sum(case when category='bank_account' and mode_transac='cash'                       and debit_amount>1  then 1 else 0 end) as num_debit_txns_cash,
        sum(case when category='bank_account' and mode_transac='upi'                        and credit_amount>1 then 1 else 0 end) as num_credit_txns_upi,
        sum(case when category='bank_account' and mode_transac='atm'                        and credit_amount>1 then 1 else 0 end) as num_credit_txns_atm,
        sum(case when category='bank_account' and mode_transac in ('rtgs','neft','imps','dd') and credit_amount>1 then 1 else 0 end) as num_credit_txns_bank_transfer,
        sum(case when category='bank_account' and mode_transac='cash'                       and credit_amount>1 then 1 else 0 end) as num_credit_txns_cash,

        -- bank by mode — amounts
        sum(case when category='bank_account' and mode_transac='upi'                        and debit_amount>1  then debit_amount  else 0 end) as total_debit_amt_upi,
        sum(case when category='bank_account' and mode_transac='atm'                        and debit_amount>1  then debit_amount  else 0 end) as total_debit_amt_atm,
        sum(case when category='bank_account' and mode_transac in ('rtgs','neft','imps','dd') and debit_amount>1 then debit_amount  else 0 end) as total_debit_amt_bank_transfer,
        sum(case when category='bank_account' and mode_transac='cash'                       and debit_amount>1  then debit_amount  else 0 end) as total_debit_amt_cash,
        sum(case when category='bank_account' and mode_transac='upi'                        and credit_amount>1 then credit_amount else 0 end) as total_credit_amt_upi,
        sum(case when category='bank_account' and mode_transac='atm'                        and credit_amount>1 then credit_amount else 0 end) as total_credit_amt_atm,
        sum(case when category='bank_account' and mode_transac in ('rtgs','neft','imps','dd') and credit_amount>1 then credit_amount else 0 end) as total_credit_amt_bank_transfer,
        sum(case when category='bank_account' and mode_transac='cash'                       and credit_amount>1 then credit_amount else 0 end) as total_credit_amt_cash
    from raw_txns
    group by user_id, cutoff_date, txn_dt
)

-- ── Final pivot: three time windows (1-7d, 8-15d, 16-30d) ──────────────────
select
    user_id,
    cutoff_date,

    -- ── SMS ──────────────────────────────────────────────────────────────────
    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_total_sms   else 0 end) as num_total_sms_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_total_sms   else 0 end) as num_total_sms_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_total_sms   else 0 end) as num_total_sms_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_readable_sms else 0 end) as num_readable_sms_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_readable_sms else 0 end) as num_readable_sms_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_readable_sms else 0 end) as num_readable_sms_16_to_30_d,

    -- ── Credit card: limits & due ─────────────────────────────────────────
    max(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then max_avail_limit_cc end) as max_avail_limit_cc_1_to_7_d,
    max(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then max_avail_limit_cc end) as max_avail_limit_cc_8_to_15_d,
    max(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then max_avail_limit_cc end) as max_avail_limit_cc_16_to_30_d,

    min(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then min_avail_limit_cc end) as min_avail_limit_cc_1_to_7_d,
    min(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then min_avail_limit_cc end) as min_avail_limit_cc_8_to_15_d,
    min(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then min_avail_limit_cc end) as min_avail_limit_cc_16_to_30_d,

    max(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then max_due_amt_cc end) as max_due_amt_cc_1_to_7_d,
    max(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then max_due_amt_cc end) as max_due_amt_cc_8_to_15_d,
    max(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then max_due_amt_cc end) as max_due_amt_cc_16_to_30_d,

    -- ── Credit card: txn counts ───────────────────────────────────────────
    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_credit_txns_cc else 0 end) as num_credit_txns_cc_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_credit_txns_cc else 0 end) as num_credit_txns_cc_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_credit_txns_cc else 0 end) as num_credit_txns_cc_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_debit_txns_cc  else 0 end) as num_debit_txns_cc_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_debit_txns_cc  else 0 end) as num_debit_txns_cc_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_debit_txns_cc  else 0 end) as num_debit_txns_cc_16_to_30_d,

    -- CC amounts
    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_credit_amt_cc else 0 end) as total_credit_amt_cc_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_credit_amt_cc else 0 end) as total_credit_amt_cc_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_credit_amt_cc else 0 end) as total_credit_amt_cc_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_debit_amt_cc  else 0 end) as total_debit_amt_cc_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_debit_amt_cc  else 0 end) as total_debit_amt_cc_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_debit_amt_cc  else 0 end) as total_debit_amt_cc_16_to_30_d,

    max(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then max_credit_amt_cc end) as max_credit_amt_cc_1_to_7_d,
    max(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then max_credit_amt_cc end) as max_credit_amt_cc_8_to_15_d,
    max(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then max_credit_amt_cc end) as max_credit_amt_cc_16_to_30_d,

    min(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then min_credit_amt_cc end) as min_credit_amt_cc_1_to_7_d,
    min(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then min_credit_amt_cc end) as min_credit_amt_cc_8_to_15_d,
    min(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then min_credit_amt_cc end) as min_credit_amt_cc_16_to_30_d,

    max(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then max_debit_amt_cc end) as max_debit_amt_cc_1_to_7_d,
    max(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then max_debit_amt_cc end) as max_debit_amt_cc_8_to_15_d,
    max(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then max_debit_amt_cc end) as max_debit_amt_cc_16_to_30_d,

    min(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then min_debit_amt_cc end) as min_debit_amt_cc_1_to_7_d,
    min(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then min_debit_amt_cc end) as min_debit_amt_cc_8_to_15_d,
    min(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then min_debit_amt_cc end) as min_debit_amt_cc_16_to_30_d,

    -- ── Bank account: balance ─────────────────────────────────────────────
    max(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then max_avail_balance end) as max_avail_balance_1_to_7_d,
    max(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then max_avail_balance end) as max_avail_balance_8_to_15_d,
    max(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then max_avail_balance end) as max_avail_balance_16_to_30_d,

    min(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then min_avail_balance end) as min_avail_balance_1_to_7_d,
    min(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then min_avail_balance end) as min_avail_balance_8_to_15_d,
    min(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then min_avail_balance end) as min_avail_balance_16_to_30_d,

    avg(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_avail_balance end) as avg_avail_balance_1_to_7_d,
    avg(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_avail_balance end) as avg_avail_balance_8_to_15_d,
    avg(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_avail_balance end) as avg_avail_balance_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_avail_balance else 0 end) as total_avail_balance_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_avail_balance else 0 end) as total_avail_balance_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_avail_balance else 0 end) as total_avail_balance_16_to_30_d,

    -- ── Bank account: txn counts ──────────────────────────────────────────
    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_credit_txns else 0 end) as num_credit_txns_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_credit_txns else 0 end) as num_credit_txns_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_credit_txns else 0 end) as num_credit_txns_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_debit_txns  else 0 end) as num_debit_txns_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_debit_txns  else 0 end) as num_debit_txns_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_debit_txns  else 0 end) as num_debit_txns_16_to_30_d,

    -- Bank amounts
    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_credit_amt else 0 end) as total_credit_amt_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_credit_amt else 0 end) as total_credit_amt_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_credit_amt else 0 end) as total_credit_amt_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_debit_amt  else 0 end) as total_debit_amt_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_debit_amt  else 0 end) as total_debit_amt_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_debit_amt  else 0 end) as total_debit_amt_16_to_30_d,

    max(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then max_credit_amt end) as max_credit_amt_1_to_7_d,
    max(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then max_credit_amt end) as max_credit_amt_8_to_15_d,
    max(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then max_credit_amt end) as max_credit_amt_16_to_30_d,

    min(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then min_credit_amt end) as min_credit_amt_1_to_7_d,
    min(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then min_credit_amt end) as min_credit_amt_8_to_15_d,
    min(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then min_credit_amt end) as min_credit_amt_16_to_30_d,

    max(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then max_debit_amt end) as max_debit_amt_1_to_7_d,
    max(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then max_debit_amt end) as max_debit_amt_8_to_15_d,
    max(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then max_debit_amt end) as max_debit_amt_16_to_30_d,

    min(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then min_debit_amt end) as min_debit_amt_1_to_7_d,
    min(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then min_debit_amt end) as min_debit_amt_8_to_15_d,
    min(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then min_debit_amt end) as min_debit_amt_16_to_30_d,

    -- ── Bank by payment mode: amounts ─────────────────────────────────────
    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_debit_amt_upi   else 0 end) as total_debit_amt_upi_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_debit_amt_upi   else 0 end) as total_debit_amt_upi_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_debit_amt_upi   else 0 end) as total_debit_amt_upi_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_credit_amt_upi  else 0 end) as total_credit_amt_upi_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_credit_amt_upi  else 0 end) as total_credit_amt_upi_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_credit_amt_upi  else 0 end) as total_credit_amt_upi_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_debit_amt_bank_transfer  else 0 end) as total_debit_amt_bank_transfer_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_debit_amt_bank_transfer  else 0 end) as total_debit_amt_bank_transfer_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_debit_amt_bank_transfer  else 0 end) as total_debit_amt_bank_transfer_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then total_credit_amt_bank_transfer else 0 end) as total_credit_amt_bank_transfer_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then total_credit_amt_bank_transfer else 0 end) as total_credit_amt_bank_transfer_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then total_credit_amt_bank_transfer else 0 end) as total_credit_amt_bank_transfer_16_to_30_d,

    -- ── Bank by payment mode: counts ──────────────────────────────────────
    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_debit_txns_upi   else 0 end) as num_debit_txns_upi_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_debit_txns_upi   else 0 end) as num_debit_txns_upi_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_debit_txns_upi   else 0 end) as num_debit_txns_upi_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_credit_txns_upi  else 0 end) as num_credit_txns_upi_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_credit_txns_upi  else 0 end) as num_credit_txns_upi_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_credit_txns_upi  else 0 end) as num_credit_txns_upi_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_debit_txns_atm   else 0 end) as num_debit_txns_atm_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_debit_txns_atm   else 0 end) as num_debit_txns_atm_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_debit_txns_atm   else 0 end) as num_debit_txns_atm_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_debit_txns_bank_transfer  else 0 end) as num_debit_txns_bank_transfer_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_debit_txns_bank_transfer  else 0 end) as num_debit_txns_bank_transfer_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_debit_txns_bank_transfer  else 0 end) as num_debit_txns_bank_transfer_16_to_30_d,

    sum(case when txn_dt between dateadd(day,-7,cutoff_date)  and dateadd(day,-1,cutoff_date)  then num_credit_txns_bank_transfer else 0 end) as num_credit_txns_bank_transfer_1_to_7_d,
    sum(case when txn_dt between dateadd(day,-15,cutoff_date) and dateadd(day,-8,cutoff_date)  then num_credit_txns_bank_transfer else 0 end) as num_credit_txns_bank_transfer_8_to_15_d,
    sum(case when txn_dt between dateadd(day,-30,cutoff_date) and dateadd(day,-16,cutoff_date) then num_credit_txns_bank_transfer else 0 end) as num_credit_txns_bank_transfer_16_to_30_d

from day_agg
group by user_id, cutoff_date;

-- Sanity check row count
select count(*) as total_rows
from analytics.data_science.transactional_features_for_early_dpd3;
