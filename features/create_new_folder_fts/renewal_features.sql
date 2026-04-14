-- renewal_features.sql
-- ============================================================
-- Produces one row per (LOAN_ID, CUTOFF_DATE) with windowed
-- DPD and repayment behaviour features from the PREVIOUS
-- (renewal) loan, limited to a 30-day lookback window before
-- each cutoff date.
--
-- Time windows (days before cutoff):
--   W1 : 1  – 7  days
--   W2 : 8  – 15 days
--   W3 : 16 – 22 days
--   W4 : 23 – 30 days
--
-- DPD buckets compacted to 5 bands (instead of per-value):
--   0 days past due, 1-2, 3-5, 6-10, >10
--
-- Pattern matches activity_features.sql / bureau_features.sql:
--   • SET base_tbl  — overridden at runtime by run_pipeline.py
--   • identifier($base_tbl) — resolved via Python regex substitution
-- ============================================================

set base_tbl = 'analytics.data_science.early_dpd3_base';

create or replace table analytics.data_science.renewal_features_for_early_dpd3 as

with base as (
    -- one row per (loan_id, cutoff_date) from the training population
    select
        loan_id,
        user_id,
        cutoff_date
    from identifier($base_tbl)
    where loan_id    is not null
      and user_id    is not null
      and cutoff_date is not null
),

-- Loan-level static attributes from the CURRENT loan
loan_meta as (
    select
        loan_id,
        loan_disbursed_date,
        loan_amount,
        tenure_months,
        actual_ideal_edi                                       as edi_amount,
        -- last_loan_id is the renewal predecessor loan (if any)
        last_loan_id
    from analytics.model.loan_origination_characteristics
    where loan_id is not null
),

-- Join base → current loan meta → predecessor (last) loan meta
base_with_loans as (
    select
        b.loan_id,
        b.user_id,
        b.cutoff_date,
        cm.loan_disbursed_date,
        cm.loan_amount,
        cm.tenure_months,
        cm.edi_amount,
        cm.last_loan_id,
        datediff('day', cm.loan_disbursed_date, b.cutoff_date) as days_since_loan_start,
        -- predecessor loan attributes (the loan this user repaid before renewal)
        pm.loan_amount                                         as prev_loan_amount,
        pm.tenure_months                                       as prev_tenure_months,
        pm.actual_ideal_edi                                    as prev_edi_amount,
        pm.loan_disbursed_date                                 as prev_loan_disbursed_date
    from base b
    left join loan_meta cm on b.loan_id = cm.loan_id
    left join analytics.model.loan_origination_characteristics pm
          on cm.last_loan_id = pm.loan_id
),

-- Daily DPD from the CURRENT loan's performance history, 30 days before cutoff
raw_dpd as (
    select
        bwl.loan_id,
        bwl.cutoff_date,
        cpd.full_date                                          as obs_date,
        datediff('day', cpd.full_date, bwl.cutoff_date)       as days_before_cutoff,
        cpd.actual_dpd
    from base_with_loans bwl
    join analytics.log.credit_performance_daily cpd
      on bwl.loan_id   = cpd.loan_id
     and cpd.full_date between dateadd(day, -30, bwl.cutoff_date)
                           and dateadd(day,  -1, bwl.cutoff_date)
    where cpd.full_date < bwl.cutoff_date   -- strictly no future leakage
),

-- Aggregate into 4 windows per (loan_id, cutoff_date)
agg as (
    select
        loan_id,
        cutoff_date,

        -- ── Window 1 : 1–7 days before cutoff ──────────────────────────
        count(case when days_before_cutoff between 1  and 7  then 1 end) as obs_days_w1,
        max(case when days_before_cutoff between 1  and 7  then actual_dpd end) as max_dpd_w1,
        min(case when days_before_cutoff between 1  and 7  then actual_dpd end) as min_dpd_w1,
        avg(case when days_before_cutoff between 1  and 7  then actual_dpd::float end) as avg_dpd_w1,
        sum(case when days_before_cutoff between 1  and 7  and actual_dpd = 0    then 1 else 0 end) as days_dpd_0_w1,
        sum(case when days_before_cutoff between 1  and 7  and actual_dpd between 1 and 2  then 1 else 0 end) as days_dpd_1_2_w1,
        sum(case when days_before_cutoff between 1  and 7  and actual_dpd between 3 and 5  then 1 else 0 end) as days_dpd_3_5_w1,
        sum(case when days_before_cutoff between 1  and 7  and actual_dpd between 6 and 10 then 1 else 0 end) as days_dpd_6_10_w1,
        sum(case when days_before_cutoff between 1  and 7  and actual_dpd > 10   then 1 else 0 end) as days_dpd_gt10_w1,
        max(case when days_before_cutoff between 1  and 7  and actual_dpd = 0 then 1 else 0 end) as ever_0dpd_w1,
        max(case when days_before_cutoff between 1  and 7  and actual_dpd > 0 then 1 else 0 end) as ever_late_w1,

        -- ── Window 2 : 8–15 days before cutoff ─────────────────────────
        count(case when days_before_cutoff between 8  and 15 then 1 end) as obs_days_w2,
        max(case when days_before_cutoff between 8  and 15 then actual_dpd end) as max_dpd_w2,
        min(case when days_before_cutoff between 8  and 15 then actual_dpd end) as min_dpd_w2,
        avg(case when days_before_cutoff between 8  and 15 then actual_dpd::float end) as avg_dpd_w2,
        sum(case when days_before_cutoff between 8  and 15 and actual_dpd = 0    then 1 else 0 end) as days_dpd_0_w2,
        sum(case when days_before_cutoff between 8  and 15 and actual_dpd between 1 and 2  then 1 else 0 end) as days_dpd_1_2_w2,
        sum(case when days_before_cutoff between 8  and 15 and actual_dpd between 3 and 5  then 1 else 0 end) as days_dpd_3_5_w2,
        sum(case when days_before_cutoff between 8  and 15 and actual_dpd between 6 and 10 then 1 else 0 end) as days_dpd_6_10_w2,
        sum(case when days_before_cutoff between 8  and 15 and actual_dpd > 10   then 1 else 0 end) as days_dpd_gt10_w2,
        max(case when days_before_cutoff between 8  and 15 and actual_dpd = 0 then 1 else 0 end) as ever_0dpd_w2,
        max(case when days_before_cutoff between 8  and 15 and actual_dpd > 0 then 1 else 0 end) as ever_late_w2,

        -- ── Window 3 : 16–22 days before cutoff ────────────────────────
        count(case when days_before_cutoff between 16 and 22 then 1 end) as obs_days_w3,
        max(case when days_before_cutoff between 16 and 22 then actual_dpd end) as max_dpd_w3,
        min(case when days_before_cutoff between 16 and 22 then actual_dpd end) as min_dpd_w3,
        avg(case when days_before_cutoff between 16 and 22 then actual_dpd::float end) as avg_dpd_w3,
        sum(case when days_before_cutoff between 16 and 22 and actual_dpd = 0    then 1 else 0 end) as days_dpd_0_w3,
        sum(case when days_before_cutoff between 16 and 22 and actual_dpd between 1 and 2  then 1 else 0 end) as days_dpd_1_2_w3,
        sum(case when days_before_cutoff between 16 and 22 and actual_dpd between 3 and 5  then 1 else 0 end) as days_dpd_3_5_w3,
        sum(case when days_before_cutoff between 16 and 22 and actual_dpd between 6 and 10 then 1 else 0 end) as days_dpd_6_10_w3,
        sum(case when days_before_cutoff between 16 and 22 and actual_dpd > 10   then 1 else 0 end) as days_dpd_gt10_w3,
        max(case when days_before_cutoff between 16 and 22 and actual_dpd = 0 then 1 else 0 end) as ever_0dpd_w3,
        max(case when days_before_cutoff between 16 and 22 and actual_dpd > 0 then 1 else 0 end) as ever_late_w3,

        -- ── Window 4 : 23–30 days before cutoff ────────────────────────
        count(case when days_before_cutoff between 23 and 30 then 1 end) as obs_days_w4,
        max(case when days_before_cutoff between 23 and 30 then actual_dpd end) as max_dpd_w4,
        min(case when days_before_cutoff between 23 and 30 then actual_dpd end) as min_dpd_w4,
        avg(case when days_before_cutoff between 23 and 30 then actual_dpd::float end) as avg_dpd_w4,
        sum(case when days_before_cutoff between 23 and 30 and actual_dpd = 0    then 1 else 0 end) as days_dpd_0_w4,
        sum(case when days_before_cutoff between 23 and 30 and actual_dpd between 1 and 2  then 1 else 0 end) as days_dpd_1_2_w4,
        sum(case when days_before_cutoff between 23 and 30 and actual_dpd between 3 and 5  then 1 else 0 end) as days_dpd_3_5_w4,
        sum(case when days_before_cutoff between 23 and 30 and actual_dpd between 6 and 10 then 1 else 0 end) as days_dpd_6_10_w4,
        sum(case when days_before_cutoff between 23 and 30 and actual_dpd > 10   then 1 else 0 end) as days_dpd_gt10_w4,
        max(case when days_before_cutoff between 23 and 30 and actual_dpd = 0 then 1 else 0 end) as ever_0dpd_w4,
        max(case when days_before_cutoff between 23 and 30 and actual_dpd > 0 then 1 else 0 end) as ever_late_w4,

        -- ── Full 30-day window summaries ────────────────────────────────
        count(*)                                         as total_obs_days_30d,
        max(actual_dpd)                                  as max_dpd_30d,
        min(actual_dpd)                                  as min_dpd_30d,
        avg(actual_dpd::float)                           as avg_dpd_30d,
        sum(case when actual_dpd = 0    then 1 else 0 end) as total_days_dpd_0_30d,
        sum(case when actual_dpd > 0    then 1 else 0 end) as total_days_late_30d,
        sum(case when actual_dpd > 10   then 1 else 0 end) as total_days_dpd_gt10_30d,

        -- DPD trend: recent (W1) vs early (W4) — positive means improving
        avg(case when days_before_cutoff between 1  and 7  then actual_dpd::float end)
        - avg(case when days_before_cutoff between 23 and 30 then actual_dpd::float end) as dpd_trend_recent_vs_early

    from raw_dpd
    group by loan_id, cutoff_date
)

-- Final: join loan metadata back onto aggregated DPD windows
select
    bwl.loan_id,
    bwl.user_id,
    bwl.cutoff_date,

    -- ── Loan-level static attributes ─────────────────────────────────
    bwl.loan_amount,
    bwl.tenure_months,
    bwl.edi_amount,
    bwl.days_since_loan_start,

    -- ── Renewal predecessor loan attributes (may be NULL for first loan)
    bwl.prev_loan_amount,
    bwl.prev_tenure_months,
    bwl.prev_edi_amount,
    -- ratio: did they borrow more this time?
    case when bwl.prev_loan_amount > 0
         then bwl.loan_amount / bwl.prev_loan_amount
         else null
    end as loan_amount_vs_prev_ratio,
    -- days between prev loan and current loan disbursement
    datediff('day', bwl.prev_loan_disbursed_date, bwl.loan_disbursed_date) as days_between_loans,

    -- ── Window 1 (1–7 days before cutoff) ────────────────────────────
    agg.obs_days_w1,
    agg.max_dpd_w1,
    agg.min_dpd_w1,
    agg.avg_dpd_w1,
    agg.days_dpd_0_w1,
    agg.days_dpd_1_2_w1,
    agg.days_dpd_3_5_w1,
    agg.days_dpd_6_10_w1,
    agg.days_dpd_gt10_w1,
    agg.ever_0dpd_w1,
    agg.ever_late_w1,

    -- ── Window 2 (8–15 days before cutoff) ───────────────────────────
    agg.obs_days_w2,
    agg.max_dpd_w2,
    agg.min_dpd_w2,
    agg.avg_dpd_w2,
    agg.days_dpd_0_w2,
    agg.days_dpd_1_2_w2,
    agg.days_dpd_3_5_w2,
    agg.days_dpd_6_10_w2,
    agg.days_dpd_gt10_w2,
    agg.ever_0dpd_w2,
    agg.ever_late_w2,

    -- ── Window 3 (16–22 days before cutoff) ──────────────────────────
    agg.obs_days_w3,
    agg.max_dpd_w3,
    agg.min_dpd_w3,
    agg.avg_dpd_w3,
    agg.days_dpd_0_w3,
    agg.days_dpd_1_2_w3,
    agg.days_dpd_3_5_w3,
    agg.days_dpd_6_10_w3,
    agg.days_dpd_gt10_w3,
    agg.ever_0dpd_w3,
    agg.ever_late_w3,

    -- ── Window 4 (23–30 days before cutoff) ──────────────────────────
    agg.obs_days_w4,
    agg.max_dpd_w4,
    agg.min_dpd_w4,
    agg.avg_dpd_w4,
    agg.days_dpd_0_w4,
    agg.days_dpd_1_2_w4,
    agg.days_dpd_3_5_w4,
    agg.days_dpd_6_10_w4,
    agg.days_dpd_gt10_w4,
    agg.ever_0dpd_w4,
    agg.ever_late_w4,

    -- ── Full 30-day summaries ─────────────────────────────────────────
    agg.total_obs_days_30d,
    agg.max_dpd_30d,
    agg.min_dpd_30d,
    agg.avg_dpd_30d,
    agg.total_days_dpd_0_30d,
    agg.total_days_late_30d,
    agg.total_days_dpd_gt10_30d,
    agg.dpd_trend_recent_vs_early,

    -- ── Derived cross-features ────────────────────────────────────────
    -- Share of observed days that were delinquent (0→perfect, 1→always late)
    case when agg.total_obs_days_30d > 0
         then agg.total_days_late_30d / agg.total_obs_days_30d::float
         else null
    end as pct_days_late_30d,

    -- EMI-to-max-DPD stress proxy
    case when agg.max_dpd_30d > 0
         then bwl.edi_amount / nullif(agg.max_dpd_30d, 0)
         else null
    end as emi_per_max_dpd_30d,

    -- DPD change: W1 avg minus W2 avg (negative = worsening in most recent week)
    agg.avg_dpd_w1 - agg.avg_dpd_w2  as dpd_delta_w1_vs_w2,

    -- Flag: any late day in the most recent 7 days
    agg.ever_late_w1                  as flag_late_last_7d

from base_with_loans bwl
left join agg
       on bwl.loan_id      = agg.loan_id
      and bwl.cutoff_date  = agg.cutoff_date;

-- Sanity check
select count(*) as total_rows
from analytics.data_science.renewal_features_for_early_dpd3;
