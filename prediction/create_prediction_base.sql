-- create_prediction_base.sql
-- Pure SELECT body — no DDL here.
-- predict.py wraps this in:
--   CREATE TABLE IF NOT EXISTS {pred_base_table} AS SELECT * FROM (...) _init WHERE 1=0
--   INSERT INTO {pred_base_table} <this body>
--   CREATE OR REPLACE TABLE {pred_daily_table} AS SELECT * FROM {pred_base_table} WHERE DATE(cutoff_date) = DATEADD('day', -1, CURRENT_DATE())
--
-- Filters applied:
--   • actual_dpd = 2           (same scope as training)
--   • cutoff_date = yesterday  (CURRENT_DATE - 1)
--   • LOAN_ACTIVE_AS_ON_DATE, is_hcb_loan_ondate = 0, ideal_amount IS NOT NULL
--     (mirrors base.sql training filters exactly)

WITH
ranked_repayments AS (
    SELECT
        a.loan_id,
        b.user_id,
        collect_at,
        type,
        lender_lead_flag,
        settlement_mis_flag,
        is_lender_payment_split_enabled,
        is_manual_repayment_on_lender_mid,
        ROW_NUMBER() OVER (
            PARTITION BY a.loan_id, DATE(collect_at)
            ORDER BY CASE WHEN type <> 'AUTO' THEN 1 ELSE 2 END
        ) AS rn
    FROM APP_BACKEND.LOAN_SERVICE_PROD.PUBLIC_REPAYMENTS_VW a
    LEFT JOIN (
        SELECT DISTINCT customer_id AS user_id, loan_id
        FROM analytics.model.loan_origination_characteristics
    ) b ON a.loan_id = b.loan_id
    WHERE (collected_amount_su > 0 AND status = 'SUCCESS')
       OR ideal_amount_su > 0
),

collections_repayments AS (
    SELECT
        a.user_id,
        a.loan_id,
        loan_status,
        collect_date                                                     AS cutoff_date,
        datediff('day', first_edi_date, collect_date)                    AS no_of_days_since_first_edi,
        ideal_amount                                                     AS edi,
        due_amount                                                       AS due_amount_bod,
        collected_amount                                                 AS collected_amount_eod,
        excess_collected_amount,
        collected_amount_cumulative,
        principal_overdue,
        interest_overdue,
        unpaid_amount,
        COALESCE(ARRAY_SIZE(OBJECT_KEYS(repayments_transactions_split)), 0) AS no_of_modes_of_repayments_used,
        repayments_transactions_split,
        pos,
        ios,
        actual_dpd,
        tilldate_max_dpd,
        is_upi_autopay_primary,
        is_allocation_experiment,
        team_allocation,
        b.type,
        b.lender_lead_flag,
        b.settlement_mis_flag,
        b.is_lender_payment_split_enabled,
        b.is_manual_repayment_on_lender_mid,
        datediff('day', c.loan_start_date, a.collect_date)              AS days_since_loan_start,
        datediff('day', a.collect_date, c.loan_end_date)                AS days_remaining_for_loan_end
    FROM LOG.COLLECTIONS_REPAYMENTS_BASEFACT a
    LEFT JOIN (SELECT * FROM ranked_repayments WHERE rn = 1) b
        ON date(a.collect_date) = date(b.collect_at)
       AND a.loan_id = b.loan_id
    LEFT JOIN (
        SELECT DISTINCT loan_id, loan_start_date, actual_loan_end_date AS loan_end_date
        FROM analytics.log.credit_performance_daily
    ) c ON a.loan_id = c.loan_id
    WHERE LOAN_ACTIVE_AS_ON_DATE
      AND is_hcb_loan_ondate = 0
      AND ideal_amount IS NOT NULL
      AND a.actual_dpd = 2
      AND DATE(collect_date) = DATEADD('day', -1, CURRENT_DATE())
)

SELECT
    cr.*,
    NULL::INTEGER AS future_max_dpd,
    -1            AS target_risk_bucket_2d
FROM collections_repayments cr
