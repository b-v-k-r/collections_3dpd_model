set base_tbl = 'analytics.data_science.field_disposition_base';

-- =====================================================
-- Step 1: Day-Level SMS Features for Collect DPD
-- =====================================================
CREATE OR REPLACE TRANSIENT TABLE ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_DAY_LEVEL_COLLECT_DPD AS (
    WITH lead_base AS (
        SELECT DISTINCT
            USER_ID,
            CUTOFF_DATE
        FROM identifier($base_tbl)
    ),
    base AS (
        SELECT
            user_id,
            CUTOFF_DATE,
            DATEADD('day', -rn, CUTOFF_DATE) AS dt
        FROM (
            SELECT
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.user_id, a.CUTOFF_DATE
                    ORDER BY SEQ4()
                ) AS rn
            FROM lead_base a
            JOIN TABLE(GENERATOR(ROWCOUNT => 30)) g ON 1=1
        )
    ),
    -- PRE-FILTER the huge SMS table first!
    filtered_sms AS (
        SELECT
            a.USER_ID,
            a.SENDER_GROUP,
            DATE(a.date) AS DT,
            a.FACT:label::TEXT AS label,
            a.FACT:emi_overdue::TEXT AS emi_overdue,
            TO_DOUBLE(a.FACT:due_amount) AS due_amount,
            TO_DOUBLE(a.FACT:overdue_amount) AS overdue_amount,
            TO_DOUBLE(a.FACT:emi_amount) AS emi_amount,
            TO_DOUBLE(a.FACT:days_past_due) AS days_past_due,
            TO_DOUBLE(a.FACT:loan_amount) AS loan_amount
        FROM RAW.DEVICE_DATA_PROD.SMS_FACTS_VW a
        WHERE
            -- Filter 1: Only your users
            a.user_id IN (SELECT distinct USER_ID FROM lead_base)
            -- Filter 2: Template and category upfront
            AND a.TEMPLATE_MATCHED = TRUE
            AND a.FACT:category::TEXT = 'loan'
            -- Filter 3: Broad date range (will refine later)
            AND DATE(a.date) >= (SELECT MIN(CUTOFF_DATE) - INTERVAL '30 days' FROM lead_base)
            AND DATE(a.date) < (SELECT MAX(CUTOFF_DATE) FROM lead_base)
    ),
    -- Now join the much smaller filtered dataset
    raw_sms_facts AS (
        SELECT
            f.*,
            b.CUTOFF_DATE
        FROM filtered_sms f
        INNER JOIN lead_base b
            ON TRIM(f.user_id) = TRIM(b.user_id)
            AND f.DT BETWEEN DATEADD('day', -30, b.CUTOFF_DATE)
                        AND DATEADD('day', -1, b.CUTOFF_DATE)
    ),
    transactional_features AS (
        SELECT
            USER_ID,
            SENDER_GROUP,
            DT,
            CUTOFF_DATE,
            -- [All your aggregations - same as before]
            SUM(CASE WHEN label = 'disburse' THEN 1 ELSE 0 END) AS total_disbursed_loans,
            SUM(CASE WHEN label = 'disburse' THEN loan_amount ELSE 0 END) AS total_loan_amount_disbursed_loans,
            MAX(CASE WHEN label = 'disburse' THEN loan_amount END) AS max_loan_amount_disbursed_loans,
            MIN(CASE WHEN label = 'disburse' THEN loan_amount END) AS min_loan_amount_disbursed_loans,
            SUM(CASE WHEN label = 'reminder' AND emi_overdue = 'true' THEN 1 ELSE 0 END) AS total_emi_overdue_sms,
            SUM(CASE WHEN label = 'reminder' AND overdue_amount > 1 THEN 1 ELSE 0 END) AS total_sms_with_overdue_amount,
            SUM(CASE WHEN label = 'reminder' AND overdue_amount > 1 THEN overdue_amount ELSE 0 END) AS total_overdue_amount,
            MAX(CASE WHEN label = 'reminder' AND overdue_amount > 1 THEN overdue_amount END) AS max_overdue_amount,
            MIN(CASE WHEN label = 'reminder' AND overdue_amount > 1 THEN overdue_amount END) AS min_overdue_amount,
            SUM(CASE WHEN label = 'reminder' AND due_amount > 1 THEN 1 ELSE 0 END) AS total_sms_with_due_amount,
            SUM(CASE WHEN label = 'reminder' AND due_amount > 1 THEN due_amount ELSE 0 END) AS total_due_amount,
            MAX(CASE WHEN label = 'reminder' AND due_amount > 1 THEN due_amount END) AS max_due_amount,
            MIN(CASE WHEN label = 'reminder' AND due_amount > 1 THEN due_amount END) AS min_due_amount,
            SUM(CASE WHEN label = 'reminder' AND days_past_due IS NOT NULL THEN 1 ELSE 0 END) AS total_sms_with_days_past_due,
            MAX(CASE WHEN label = 'reminder' THEN days_past_due END) AS max_days_past_due,
            MIN(CASE WHEN label = 'reminder' THEN days_past_due END) AS min_days_past_due,
            SUM(CASE WHEN label = 'closed' THEN 1 ELSE 0 END) AS total_loan_closing_sms,
            SUM(CASE WHEN label = 'approved' THEN 1 ELSE 0 END) AS total_approved_sms,
            SUM(CASE WHEN label = 'approved' THEN loan_amount ELSE 0 END) AS total_loan_amount_approved,
            MAX(CASE WHEN label = 'approved' THEN loan_amount END) AS max_loan_amount_approved,
            MIN(CASE WHEN label = 'approved' THEN loan_amount END) AS min_loan_amount_approved,
            SUM(CASE WHEN label = 'rejected' THEN 1 ELSE 0 END) AS total_loan_rejection_sms,
            SUM(CASE WHEN label = 'applied' THEN 1 ELSE 0 END) AS total_loan_applied_sms,
            SUM(CASE WHEN label = 'repayment' THEN 1 ELSE 0 END) AS total_repayment_sms,
            SUM(CASE WHEN label = 'repayment' THEN emi_amount ELSE 0 END) AS total_emi_amount_paid,
            MAX(CASE WHEN label = 'repayment' THEN emi_amount END) AS max_emi_amount_paid,
            MIN(CASE WHEN label = 'repayment' THEN emi_amount END) AS min_emi_amount_paid,
            SUM(CASE WHEN label = 'enquiry' THEN 1 ELSE 0 END) AS total_loan_enquiry_sms,
            SUM(CASE WHEN label = 'ready_for_disbursal' THEN 1 ELSE 0 END) AS total_ready_for_disbursal_sms
        FROM raw_sms_facts
        GROUP BY 1, 2, 3, 4
    )

    SELECT
        a.USER_ID,
        a.CUTOFF_DATE,
        a.dt,
        t.sender_group,
        t.TOTAL_DISBURSED_LOANS,
        t.TOTAL_LOAN_AMOUNT_DISBURSED_LOANS,
        t.MAX_LOAN_AMOUNT_DISBURSED_LOANS,
        t.MIN_LOAN_AMOUNT_DISBURSED_LOANS,
        t.TOTAL_EMI_OVERDUE_SMS,
        t.TOTAL_SMS_WITH_OVERDUE_AMOUNT,
        t.TOTAL_OVERDUE_AMOUNT,
        t.MAX_OVERDUE_AMOUNT,
        t.MIN_OVERDUE_AMOUNT,
        t.TOTAL_SMS_WITH_DUE_AMOUNT,
        t.TOTAL_DUE_AMOUNT,
        t.MAX_DUE_AMOUNT,
        t.MIN_DUE_AMOUNT,
        t.TOTAL_SMS_WITH_DAYS_PAST_DUE,
        t.MAX_DAYS_PAST_DUE,
        t.MIN_DAYS_PAST_DUE,
        t.TOTAL_LOAN_CLOSING_SMS,
        t.TOTAL_APPROVED_SMS,
        t.TOTAL_LOAN_AMOUNT_APPROVED,
        t.MAX_LOAN_AMOUNT_APPROVED,
        t.MIN_LOAN_AMOUNT_APPROVED,
        t.TOTAL_LOAN_REJECTION_SMS,
        t.TOTAL_LOAN_APPLIED_SMS,
        t.TOTAL_REPAYMENT_SMS,
        t.TOTAL_EMI_AMOUNT_PAID,
        t.MAX_EMI_AMOUNT_PAID,
        t.MIN_EMI_AMOUNT_PAID,
        t.TOTAL_LOAN_ENQUIRY_SMS,
        t.TOTAL_READY_FOR_DISBURSAL_SMS
    FROM base a
    LEFT JOIN transactional_features t
        ON a.user_id = t.user_id
        AND a.dt = t.dt
        AND a.cutoff_date = t.cutoff_date
);


-- SELECT COUNT(*) AS CN ,
--     COUNT(*) - COUNT(MAX_LOAN_AMOUNT_DISBURSED_LOANS)  AS MAXLOAN,
--     COUNT(*) - COUNT(MIN_LOAN_AMOUNT_DISBURSED_LOANS)  AS MINLOAN
-- FROM ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_DAY_LEVEL_COLLECT_DPD;



-- select * from RAW.DEVICE_DATA_PROD.SMS_FACTS_VW
-- where user_id = '61e9ae1b-c468-49b5-891a-6ee842bc47f0'
-- order by Date(upload_time)
-- limit 100
-- ;

-- select count(*) from ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_DAY_LEVEL_COLLECT_DPD;

-- SELECT COUNT(*) AS CN ,
--     COUNT(*) - COUNT(MAX_LOAN_AMOUNT_DISBURSED_LOANS)  AS MAXLOAN,
--     COUNT(*) - COUNT(MIN_LOAN_AMOUNT_DISBURSED_LOANS)  AS MINLOAN

-- FROM ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_DAY_LEVEL_COLLECT_DPD;


-- =====================================================
-- Step 2: Lead/User-Level SMS Features with 10 Time Windows
-- =====================================================
CREATE OR REPLACE TRANSIENT TABLE ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_FEATURES_COLLECT_DPD AS (
SELECT
    *,
    -- Ratio features: distinct_banks_loan_disbursed ratios
    CASE WHEN distinct_banks_loan_disbursed_3to4d > 0 THEN distinct_banks_loan_disbursed_last2d / distinct_banks_loan_disbursed_3to4d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_last2d_TO_3to4d,
    CASE WHEN distinct_banks_loan_disbursed_5to6d > 0 THEN distinct_banks_loan_disbursed_last2d / distinct_banks_loan_disbursed_5to6d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_last2d_TO_5to6d,
    CASE WHEN distinct_banks_loan_disbursed_7to8d > 0 THEN distinct_banks_loan_disbursed_last2d / distinct_banks_loan_disbursed_7to8d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_last2d_TO_7to8d,
    CASE WHEN distinct_banks_loan_disbursed_9to11d > 0 THEN distinct_banks_loan_disbursed_last2d / distinct_banks_loan_disbursed_9to11d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_last2d_TO_9to11d,
    CASE WHEN distinct_banks_loan_disbursed_12to14d > 0 THEN distinct_banks_loan_disbursed_last2d / distinct_banks_loan_disbursed_12to14d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_last2d_TO_12to14d,
    CASE WHEN distinct_banks_loan_disbursed_2week > 0 THEN distinct_banks_loan_disbursed_1week / distinct_banks_loan_disbursed_2week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_1week_TO_2week,
    CASE WHEN distinct_banks_loan_disbursed_3week > 0 THEN distinct_banks_loan_disbursed_1week / distinct_banks_loan_disbursed_3week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_1week_TO_3week,
    CASE WHEN distinct_banks_loan_disbursed_4week > 0 THEN distinct_banks_loan_disbursed_1week / distinct_banks_loan_disbursed_4week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_LOAN_DISBURSED_1week_TO_4week,

    -- Ratio features: distinct_banks_emi_overdue ratios
    CASE WHEN distinct_banks_emi_overdue_3to4d > 0 THEN distinct_banks_emi_overdue_last2d / distinct_banks_emi_overdue_3to4d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_last2d_TO_3to4d,
    CASE WHEN distinct_banks_emi_overdue_5to6d > 0 THEN distinct_banks_emi_overdue_last2d / distinct_banks_emi_overdue_5to6d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_last2d_TO_5to6d,
    CASE WHEN distinct_banks_emi_overdue_7to8d > 0 THEN distinct_banks_emi_overdue_last2d / distinct_banks_emi_overdue_7to8d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_last2d_TO_7to8d,
    CASE WHEN distinct_banks_emi_overdue_9to11d > 0 THEN distinct_banks_emi_overdue_last2d / distinct_banks_emi_overdue_9to11d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_last2d_TO_9to11d,
    CASE WHEN distinct_banks_emi_overdue_12to14d > 0 THEN distinct_banks_emi_overdue_last2d / distinct_banks_emi_overdue_12to14d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_last2d_TO_12to14d,
    CASE WHEN distinct_banks_emi_overdue_2week > 0 THEN distinct_banks_emi_overdue_1week / distinct_banks_emi_overdue_2week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_1week_TO_2week,
    CASE WHEN distinct_banks_emi_overdue_3week > 0 THEN distinct_banks_emi_overdue_1week / distinct_banks_emi_overdue_3week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_1week_TO_3week,
    CASE WHEN distinct_banks_emi_overdue_4week > 0 THEN distinct_banks_emi_overdue_1week / distinct_banks_emi_overdue_4week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_OVERDUE_1week_TO_4week,

    -- Ratio features: max_overdue_amount ratios
    CASE WHEN max_overdue_amount_3to4d > 0 THEN max_overdue_amount_last2d / max_overdue_amount_3to4d ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_last2d_TO_3to4d,
    CASE WHEN max_overdue_amount_5to6d > 0 THEN max_overdue_amount_last2d / max_overdue_amount_5to6d ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_last2d_TO_5to6d,
    CASE WHEN max_overdue_amount_7to8d > 0 THEN max_overdue_amount_last2d / max_overdue_amount_7to8d ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_last2d_TO_7to8d,
    CASE WHEN max_overdue_amount_9to11d > 0 THEN max_overdue_amount_last2d / max_overdue_amount_9to11d ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_last2d_TO_9to11d,
    CASE WHEN max_overdue_amount_12to14d > 0 THEN max_overdue_amount_last2d / max_overdue_amount_12to14d ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_last2d_TO_12to14d,
    CASE WHEN max_overdue_amount_2week > 0 THEN max_overdue_amount_1week / max_overdue_amount_2week ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_1week_TO_2week,
    CASE WHEN max_overdue_amount_3week > 0 THEN max_overdue_amount_1week / max_overdue_amount_3week ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_1week_TO_3week,
    CASE WHEN max_overdue_amount_4week > 0 THEN max_overdue_amount_1week / max_overdue_amount_4week ELSE 0 END AS RATIO_OF_MAX_OVERDUE_AMOUNT_1week_TO_4week,

    -- Ratio features: distinct_banks_emi_due ratios
    CASE WHEN distinct_banks_emi_due_3to4d > 0 THEN distinct_banks_emi_due_last2d / distinct_banks_emi_due_3to4d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_last2d_TO_3to4d,
    CASE WHEN distinct_banks_emi_due_5to6d > 0 THEN distinct_banks_emi_due_last2d / distinct_banks_emi_due_5to6d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_last2d_TO_5to6d,
    CASE WHEN distinct_banks_emi_due_7to8d > 0 THEN distinct_banks_emi_due_last2d / distinct_banks_emi_due_7to8d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_last2d_TO_7to8d,
    CASE WHEN distinct_banks_emi_due_9to11d > 0 THEN distinct_banks_emi_due_last2d / distinct_banks_emi_due_9to11d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_last2d_TO_9to11d,
    CASE WHEN distinct_banks_emi_due_12to14d > 0 THEN distinct_banks_emi_due_last2d / distinct_banks_emi_due_12to14d ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_last2d_TO_12to14d,
    CASE WHEN distinct_banks_emi_due_2week > 0 THEN distinct_banks_emi_due_1week / distinct_banks_emi_due_2week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_1week_TO_2week,
    CASE WHEN distinct_banks_emi_due_3week > 0 THEN distinct_banks_emi_due_1week / distinct_banks_emi_due_3week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_1week_TO_3week,
    CASE WHEN distinct_banks_emi_due_4week > 0 THEN distinct_banks_emi_due_1week / distinct_banks_emi_due_4week ELSE 0 END AS RATIO_OF_DISTINCT_BANKS_EMI_DUE_1week_TO_4week,

    -- Ratio features: max_due_amount ratios
    CASE WHEN max_due_amount_3to4d > 0 THEN max_due_amount_last2d / max_due_amount_3to4d ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_last2d_TO_3to4d,
    CASE WHEN max_due_amount_5to6d > 0 THEN max_due_amount_last2d / max_due_amount_5to6d ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_last2d_TO_5to6d,
    CASE WHEN max_due_amount_7to8d > 0 THEN max_due_amount_last2d / max_due_amount_7to8d ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_last2d_TO_7to8d,
    CASE WHEN max_due_amount_9to11d > 0 THEN max_due_amount_last2d / max_due_amount_9to11d ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_last2d_TO_9to11d,
    CASE WHEN max_due_amount_12to14d > 0 THEN max_due_amount_last2d / max_due_amount_12to14d ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_last2d_TO_12to14d,
    CASE WHEN max_due_amount_2week > 0 THEN max_due_amount_1week / max_due_amount_2week ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_1week_TO_2week,
    CASE WHEN max_due_amount_3week > 0 THEN max_due_amount_1week / max_due_amount_3week ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_1week_TO_3week,
    CASE WHEN max_due_amount_4week > 0 THEN max_due_amount_1week / max_due_amount_4week ELSE 0 END AS RATIO_OF_MAX_DUE_AMOUNT_1week_TO_4week,

    -- Ratio features: max_days_past_due ratios
    CASE WHEN max_days_past_due_3to4d > 0 THEN max_days_past_due_last2d / max_days_past_due_3to4d ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_last2d_TO_3to4d,
    CASE WHEN max_days_past_due_5to6d > 0 THEN max_days_past_due_last2d / max_days_past_due_5to6d ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_last2d_TO_5to6d,
    CASE WHEN max_days_past_due_7to8d > 0 THEN max_days_past_due_last2d / max_days_past_due_7to8d ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_last2d_TO_7to8d,
    CASE WHEN max_days_past_due_9to11d > 0 THEN max_days_past_due_last2d / max_days_past_due_9to11d ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_last2d_TO_9to11d,
    CASE WHEN max_days_past_due_12to14d > 0 THEN max_days_past_due_last2d / max_days_past_due_12to14d ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_last2d_TO_12to14d,
    CASE WHEN max_days_past_due_2week > 0 THEN max_days_past_due_1week / max_days_past_due_2week ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_1week_TO_2week,
    CASE WHEN max_days_past_due_3week > 0 THEN max_days_past_due_1week / max_days_past_due_3week ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_1week_TO_3week,
    CASE WHEN max_days_past_due_4week > 0 THEN max_days_past_due_1week / max_days_past_due_4week ELSE 0 END AS RATIO_OF_MAX_DAYS_PAST_DUE_1week_TO_4week,

    -- Cross-ratio features: repayment to overdue
    CASE WHEN avg_overdue_amount_last2d > 0 THEN avg_loan_repayment_amount_last2d / avg_overdue_amount_last2d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_last2d,
    CASE WHEN avg_overdue_amount_3to4d > 0 THEN avg_loan_repayment_amount_3to4d / avg_overdue_amount_3to4d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_3to4d,
    CASE WHEN avg_overdue_amount_5to6d > 0 THEN avg_loan_repayment_amount_5to6d / avg_overdue_amount_5to6d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_5to6d,
    CASE WHEN avg_overdue_amount_7to8d > 0 THEN avg_loan_repayment_amount_7to8d / avg_overdue_amount_7to8d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_7to8d,
    CASE WHEN avg_overdue_amount_9to11d > 0 THEN avg_loan_repayment_amount_9to11d / avg_overdue_amount_9to11d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_9to11d,
    CASE WHEN avg_overdue_amount_12to14d > 0 THEN avg_loan_repayment_amount_12to14d / avg_overdue_amount_12to14d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_12to14d,
    CASE WHEN avg_overdue_amount_1week > 0 THEN avg_loan_repayment_amount_1week / avg_overdue_amount_1week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_1week,
    CASE WHEN avg_overdue_amount_2week > 0 THEN avg_loan_repayment_amount_2week / avg_overdue_amount_2week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_2week,
    CASE WHEN avg_overdue_amount_3week > 0 THEN avg_loan_repayment_amount_3week / avg_overdue_amount_3week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_3week,
    CASE WHEN avg_overdue_amount_4week > 0 THEN avg_loan_repayment_amount_4week / avg_overdue_amount_4week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_OVERDUE_AMOUNT_IN_4week,

    -- Cross-ratio features: repayment to due
    CASE WHEN avg_due_amount_last2d > 0 THEN avg_loan_repayment_amount_last2d / avg_due_amount_last2d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_last2d,
    CASE WHEN avg_due_amount_3to4d > 0 THEN avg_loan_repayment_amount_3to4d / avg_due_amount_3to4d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_3to4d,
    CASE WHEN avg_due_amount_5to6d > 0 THEN avg_loan_repayment_amount_5to6d / avg_due_amount_5to6d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_5to6d,
    CASE WHEN avg_due_amount_7to8d > 0 THEN avg_loan_repayment_amount_7to8d / avg_due_amount_7to8d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_7to8d,
    CASE WHEN avg_due_amount_9to11d > 0 THEN avg_loan_repayment_amount_9to11d / avg_due_amount_9to11d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_9to11d,
    CASE WHEN avg_due_amount_12to14d > 0 THEN avg_loan_repayment_amount_12to14d / avg_due_amount_12to14d ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_12to14d,
    CASE WHEN avg_due_amount_1week > 0 THEN avg_loan_repayment_amount_1week / avg_due_amount_1week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_1week,
    CASE WHEN avg_due_amount_2week > 0 THEN avg_loan_repayment_amount_2week / avg_due_amount_2week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_2week,
    CASE WHEN avg_due_amount_3week > 0 THEN avg_loan_repayment_amount_3week / avg_due_amount_3week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_3week,
    CASE WHEN avg_due_amount_4week > 0 THEN avg_loan_repayment_amount_4week / avg_due_amount_4week ELSE 0 END AS RATIO_OF_AVG_LOAN_REPAYMENT_AMOUNT_TO_AVG_DUE_AMOUNT_IN_4week
FROM (
    SELECT
        *,
        -- Calculate average features from totals and counts
        CASE WHEN num_loan_disbursed_last2d > 0 THEN total_loan_amount_disbursed_last2d / num_loan_disbursed_last2d ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_last2d,
        CASE WHEN num_loan_disbursed_3to4d > 0 THEN total_loan_amount_disbursed_3to4d / num_loan_disbursed_3to4d ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_3to4d,
        CASE WHEN num_loan_disbursed_5to6d > 0 THEN total_loan_amount_disbursed_5to6d / num_loan_disbursed_5to6d ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_5to6d,
        CASE WHEN num_loan_disbursed_7to8d > 0 THEN total_loan_amount_disbursed_7to8d / num_loan_disbursed_7to8d ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_7to8d,
        CASE WHEN num_loan_disbursed_9to11d > 0 THEN total_loan_amount_disbursed_9to11d / num_loan_disbursed_9to11d ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_9to11d,
        CASE WHEN num_loan_disbursed_12to14d > 0 THEN total_loan_amount_disbursed_12to14d / num_loan_disbursed_12to14d ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_12to14d,
        CASE WHEN num_loan_disbursed_1week > 0 THEN total_loan_amount_disbursed_1week / num_loan_disbursed_1week ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_1week,
        CASE WHEN num_loan_disbursed_2week > 0 THEN total_loan_amount_disbursed_2week / num_loan_disbursed_2week ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_2week,
        CASE WHEN num_loan_disbursed_3week > 0 THEN total_loan_amount_disbursed_3week / num_loan_disbursed_3week ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_3week,
        CASE WHEN num_loan_disbursed_4week > 0 THEN total_loan_amount_disbursed_4week / num_loan_disbursed_4week ELSE 0 END AS AVG_LOAN_AMOUNT_DISBURSED_4week,

        CASE WHEN num_sms_with_overdue_amount_last2d > 0 THEN total_overdue_amount_last2d / num_sms_with_overdue_amount_last2d ELSE 0 END AS AVG_OVERDUE_AMOUNT_last2d,
        CASE WHEN num_sms_with_overdue_amount_3to4d > 0 THEN total_overdue_amount_3to4d / num_sms_with_overdue_amount_3to4d ELSE 0 END AS AVG_OVERDUE_AMOUNT_3to4d,
        CASE WHEN num_sms_with_overdue_amount_5to6d > 0 THEN total_overdue_amount_5to6d / num_sms_with_overdue_amount_5to6d ELSE 0 END AS AVG_OVERDUE_AMOUNT_5to6d,
        CASE WHEN num_sms_with_overdue_amount_7to8d > 0 THEN total_overdue_amount_7to8d / num_sms_with_overdue_amount_7to8d ELSE 0 END AS AVG_OVERDUE_AMOUNT_7to8d,
        CASE WHEN num_sms_with_overdue_amount_9to11d > 0 THEN total_overdue_amount_9to11d / num_sms_with_overdue_amount_9to11d ELSE 0 END AS AVG_OVERDUE_AMOUNT_9to11d,
        CASE WHEN num_sms_with_overdue_amount_12to14d > 0 THEN total_overdue_amount_12to14d / num_sms_with_overdue_amount_12to14d ELSE 0 END AS AVG_OVERDUE_AMOUNT_12to14d,
        CASE WHEN num_sms_with_overdue_amount_1week > 0 THEN total_overdue_amount_1week / num_sms_with_overdue_amount_1week ELSE 0 END AS AVG_OVERDUE_AMOUNT_1week,
        CASE WHEN num_sms_with_overdue_amount_2week > 0 THEN total_overdue_amount_2week / num_sms_with_overdue_amount_2week ELSE 0 END AS AVG_OVERDUE_AMOUNT_2week,
        CASE WHEN num_sms_with_overdue_amount_3week > 0 THEN total_overdue_amount_3week / num_sms_with_overdue_amount_3week ELSE 0 END AS AVG_OVERDUE_AMOUNT_3week,
        CASE WHEN num_sms_with_overdue_amount_4week > 0 THEN total_overdue_amount_4week / num_sms_with_overdue_amount_4week ELSE 0 END AS AVG_OVERDUE_AMOUNT_4week,

        CASE WHEN num_sms_with_due_amount_last2d > 0 THEN total_due_amount_last2d / num_sms_with_due_amount_last2d ELSE 0 END AS AVG_DUE_AMOUNT_last2d,
        CASE WHEN num_sms_with_due_amount_3to4d > 0 THEN total_due_amount_3to4d / num_sms_with_due_amount_3to4d ELSE 0 END AS AVG_DUE_AMOUNT_3to4d,
        CASE WHEN num_sms_with_due_amount_5to6d > 0 THEN total_due_amount_5to6d / num_sms_with_due_amount_5to6d ELSE 0 END AS AVG_DUE_AMOUNT_5to6d,
        CASE WHEN num_sms_with_due_amount_7to8d > 0 THEN total_due_amount_7to8d / num_sms_with_due_amount_7to8d ELSE 0 END AS AVG_DUE_AMOUNT_7to8d,
        CASE WHEN num_sms_with_due_amount_9to11d > 0 THEN total_due_amount_9to11d / num_sms_with_due_amount_9to11d ELSE 0 END AS AVG_DUE_AMOUNT_9to11d,
        CASE WHEN num_sms_with_due_amount_12to14d > 0 THEN total_due_amount_12to14d / num_sms_with_due_amount_12to14d ELSE 0 END AS AVG_DUE_AMOUNT_12to14d,
        CASE WHEN num_sms_with_due_amount_1week > 0 THEN total_due_amount_1week / num_sms_with_due_amount_1week ELSE 0 END AS AVG_DUE_AMOUNT_1week,
        CASE WHEN num_sms_with_due_amount_2week > 0 THEN total_due_amount_2week / num_sms_with_due_amount_2week ELSE 0 END AS AVG_DUE_AMOUNT_2week,
        CASE WHEN num_sms_with_due_amount_3week > 0 THEN total_due_amount_3week / num_sms_with_due_amount_3week ELSE 0 END AS AVG_DUE_AMOUNT_3week,
        CASE WHEN num_sms_with_due_amount_4week > 0 THEN total_due_amount_4week / num_sms_with_due_amount_4week ELSE 0 END AS AVG_DUE_AMOUNT_4week,

        CASE WHEN num_loan_repayment_sms_last2d > 0 THEN total_loan_repayment_amount_last2d / num_loan_repayment_sms_last2d ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_last2d,
        CASE WHEN num_loan_repayment_sms_3to4d > 0 THEN total_loan_repayment_amount_3to4d / num_loan_repayment_sms_3to4d ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_3to4d,
        CASE WHEN num_loan_repayment_sms_5to6d > 0 THEN total_loan_repayment_amount_5to6d / num_loan_repayment_sms_5to6d ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_5to6d,
        CASE WHEN num_loan_repayment_sms_7to8d > 0 THEN total_loan_repayment_amount_7to8d / num_loan_repayment_sms_7to8d ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_7to8d,
        CASE WHEN num_loan_repayment_sms_9to11d > 0 THEN total_loan_repayment_amount_9to11d / num_loan_repayment_sms_9to11d ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_9to11d,
        CASE WHEN num_loan_repayment_sms_12to14d > 0 THEN total_loan_repayment_amount_12to14d / num_loan_repayment_sms_12to14d ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_12to14d,
        CASE WHEN num_loan_repayment_sms_1week > 0 THEN total_loan_repayment_amount_1week / num_loan_repayment_sms_1week ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_1week,
        CASE WHEN num_loan_repayment_sms_2week > 0 THEN total_loan_repayment_amount_2week / num_loan_repayment_sms_2week ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_2week,
        CASE WHEN num_loan_repayment_sms_3week > 0 THEN total_loan_repayment_amount_3week / num_loan_repayment_sms_3week ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_3week,
        CASE WHEN num_loan_repayment_sms_4week > 0 THEN total_loan_repayment_amount_4week / num_loan_repayment_sms_4week ELSE 0 END AS AVG_LOAN_REPAYMENT_AMOUNT_4week
    FROM (
        SELECT

            USER_ID,
            CUTOFF_DATE,

            -- DISTINCT BANKS: Loan Disbursed (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_disbursed_loans > 0 THEN sender_group END) AS distinct_banks_loan_disbursed_4week,

            -- NUM LOANS DISBURSED (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_disbursed_loans > 0 THEN total_disbursed_loans ELSE 0 END) AS num_loan_disbursed_4week,

            -- TOTAL LOAN AMOUNT DISBURSED (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_disbursed_loans > 0 THEN total_loan_amount_disbursed_loans ELSE 0 END) AS total_loan_amount_disbursed_4week,

            -- MAX LOAN AMOUNT DISBURSED (10 time windows)
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_last2d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_3to4d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_5to6d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_7to8d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_9to11d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_12to14d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_1week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_2week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_3week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN max_loan_amount_disbursed_loans END) AS max_loan_amount_disbursed_4week,

            -- MIN LOAN AMOUNT DISBURSED (10 time windows)
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_last2d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_3to4d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_5to6d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_7to8d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_9to11d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_12to14d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_1week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_2week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_3week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN min_loan_amount_disbursed_loans END) AS min_loan_amount_disbursed_4week,

            -- DISTINCT BANKS: EMI OVERDUE (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_emi_overdue_sms > 0 THEN sender_group END) AS distinct_banks_emi_overdue_4week,

            -- NUM SMS WITH OVERDUE AMOUNT (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_sms_with_overdue_amount > 0 THEN total_sms_with_overdue_amount ELSE 0 END) AS num_sms_with_overdue_amount_4week,

            -- TOTAL OVERDUE AMOUNT (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_sms_with_overdue_amount > 0 THEN total_overdue_amount ELSE 0 END) AS total_overdue_amount_4week,

            -- MAX OVERDUE AMOUNT (10 time windows)
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN max_overdue_amount END) AS max_overdue_amount_last2d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN max_overdue_amount END) AS max_overdue_amount_3to4d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN max_overdue_amount END) AS max_overdue_amount_5to6d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN max_overdue_amount END) AS max_overdue_amount_7to8d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN max_overdue_amount END) AS max_overdue_amount_9to11d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN max_overdue_amount END) AS max_overdue_amount_12to14d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN max_overdue_amount END) AS max_overdue_amount_1week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN max_overdue_amount END) AS max_overdue_amount_2week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN max_overdue_amount END) AS max_overdue_amount_3week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN max_overdue_amount END) AS max_overdue_amount_4week,

            -- MIN OVERDUE AMOUNT (10 time windows)
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN min_overdue_amount END) AS min_overdue_amount_last2d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN min_overdue_amount END) AS min_overdue_amount_3to4d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN min_overdue_amount END) AS min_overdue_amount_5to6d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN min_overdue_amount END) AS min_overdue_amount_7to8d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN min_overdue_amount END) AS min_overdue_amount_9to11d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN min_overdue_amount END) AS min_overdue_amount_12to14d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN min_overdue_amount END) AS min_overdue_amount_1week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN min_overdue_amount END) AS min_overdue_amount_2week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN min_overdue_amount END) AS min_overdue_amount_3week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN min_overdue_amount END) AS min_overdue_amount_4week,

            -- DISTINCT BANKS: EMI DUE (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_sms_with_due_amount > 0 THEN sender_group END) AS distinct_banks_emi_due_4week,

            -- NUM SMS WITH DUE AMOUNT (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_sms_with_due_amount > 0 THEN total_sms_with_due_amount ELSE 0 END) AS num_sms_with_due_amount_4week,

            -- TOTAL DUE AMOUNT (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN total_due_amount ELSE 0 END) AS total_due_amount_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN total_due_amount ELSE 0 END) AS total_due_amount_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN total_due_amount ELSE 0 END) AS total_due_amount_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN total_due_amount ELSE 0 END) AS total_due_amount_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN total_due_amount ELSE 0 END) AS total_due_amount_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN total_due_amount ELSE 0 END) AS total_due_amount_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN total_due_amount ELSE 0 END) AS total_due_amount_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN total_due_amount ELSE 0 END) AS total_due_amount_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN total_due_amount ELSE 0 END) AS total_due_amount_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN total_due_amount ELSE 0 END) AS total_due_amount_4week,

            -- MAX DUE AMOUNT (10 time windows)
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN max_due_amount END) AS max_due_amount_last2d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN max_due_amount END) AS max_due_amount_3to4d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN max_due_amount END) AS max_due_amount_5to6d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN max_due_amount END) AS max_due_amount_7to8d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN max_due_amount END) AS max_due_amount_9to11d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN max_due_amount END) AS max_due_amount_12to14d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN max_due_amount END) AS max_due_amount_1week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN max_due_amount END) AS max_due_amount_2week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN max_due_amount END) AS max_due_amount_3week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN max_due_amount END) AS max_due_amount_4week,

            -- MIN DUE AMOUNT (10 time windows)
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN min_due_amount END) AS min_due_amount_last2d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN min_due_amount END) AS min_due_amount_3to4d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN min_due_amount END) AS min_due_amount_5to6d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN min_due_amount END) AS min_due_amount_7to8d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN min_due_amount END) AS min_due_amount_9to11d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN min_due_amount END) AS min_due_amount_12to14d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN min_due_amount END) AS min_due_amount_1week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN min_due_amount END) AS min_due_amount_2week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN min_due_amount END) AS min_due_amount_3week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN min_due_amount END) AS min_due_amount_4week,

            -- MAX DAYS PAST DUE (10 time windows)
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN max_days_past_due END) AS max_days_past_due_last2d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN max_days_past_due END) AS max_days_past_due_3to4d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN max_days_past_due END) AS max_days_past_due_5to6d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN max_days_past_due END) AS max_days_past_due_7to8d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN max_days_past_due END) AS max_days_past_due_9to11d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN max_days_past_due END) AS max_days_past_due_12to14d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN max_days_past_due END) AS max_days_past_due_1week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN max_days_past_due END) AS max_days_past_due_2week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN max_days_past_due END) AS max_days_past_due_3week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN max_days_past_due END) AS max_days_past_due_4week,

            -- MIN DAYS PAST DUE (10 time windows)
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN min_days_past_due END) AS min_days_past_due_last2d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN min_days_past_due END) AS min_days_past_due_3to4d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN min_days_past_due END) AS min_days_past_due_5to6d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN min_days_past_due END) AS min_days_past_due_7to8d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN min_days_past_due END) AS min_days_past_due_9to11d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN min_days_past_due END) AS min_days_past_due_12to14d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN min_days_past_due END) AS min_days_past_due_1week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN min_days_past_due END) AS min_days_past_due_2week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN min_days_past_due END) AS min_days_past_due_3week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN min_days_past_due END) AS min_days_past_due_4week,

            -- DISTINCT BANKS: WITH DPD (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_sms_with_days_past_due > 0 THEN sender_group END) AS distinct_banks_with_dpd_4week,

            -- NUM SMS WITH DPD (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_sms_with_days_past_due > 0 THEN total_sms_with_days_past_due ELSE 0 END) AS num_sms_with_dpd_4week,

            -- DISTINCT BANKS: LOAN CLOSED (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_loan_closing_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_closed_4week,

            -- DISTINCT BANKS: LOAN APPROVED (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_approved_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_approved_4week,

            -- MAX LOAN AMOUNT APPROVED (10 time windows)
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN max_loan_amount_approved END) AS max_loan_amount_approved_last2d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN max_loan_amount_approved END) AS max_loan_amount_approved_3to4d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN max_loan_amount_approved END) AS max_loan_amount_approved_5to6d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN max_loan_amount_approved END) AS max_loan_amount_approved_7to8d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN max_loan_amount_approved END) AS max_loan_amount_approved_9to11d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN max_loan_amount_approved END) AS max_loan_amount_approved_12to14d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN max_loan_amount_approved END) AS max_loan_amount_approved_1week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN max_loan_amount_approved END) AS max_loan_amount_approved_2week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN max_loan_amount_approved END) AS max_loan_amount_approved_3week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN max_loan_amount_approved END) AS max_loan_amount_approved_4week,

            -- MIN LOAN AMOUNT APPROVED (10 time windows)
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN min_loan_amount_approved END) AS min_loan_amount_approved_last2d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN min_loan_amount_approved END) AS min_loan_amount_approved_3to4d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN min_loan_amount_approved END) AS min_loan_amount_approved_5to6d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN min_loan_amount_approved END) AS min_loan_amount_approved_7to8d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN min_loan_amount_approved END) AS min_loan_amount_approved_9to11d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN min_loan_amount_approved END) AS min_loan_amount_approved_12to14d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN min_loan_amount_approved END) AS min_loan_amount_approved_1week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN min_loan_amount_approved END) AS min_loan_amount_approved_2week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN min_loan_amount_approved END) AS min_loan_amount_approved_3week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN min_loan_amount_approved END) AS min_loan_amount_approved_4week,

            -- DISTINCT BANKS: LOAN REJECTED (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_loan_rejection_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_rejected_4week,

            -- NUM LOAN REJECTION SMS (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_loan_rejection_sms > 0 THEN total_loan_rejection_sms ELSE 0 END) AS num_loan_rejection_sms_4week,

            -- DISTINCT BANKS: LOAN APPLIED (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_loan_applied_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_applied_4week,

            -- NUM LOAN APPLICATION SMS (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_loan_applied_sms > 0 THEN total_loan_applied_sms ELSE 0 END) AS num_loan_application_sms_4week,

            -- DISTINCT BANKS: LOAN ENQUIRY (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_loan_enquiry_sms > 0 THEN sender_group END) AS distinct_banks_with_loan_enquiry_4week,

            -- NUM LOAN ENQUIRY SMS (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_loan_enquiry_sms > 0 THEN total_loan_enquiry_sms ELSE 0 END) AS num_loan_enquiry_sms_4week,

            -- DISTINCT BANKS: DISBURSAL READY (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_ready_for_disbursal_sms > 0 THEN sender_group END) AS distinct_banks_with_disbursal_ready_4week,

            -- DISTINCT BANKS: LOAN REPAYMENT (10 time windows)
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_last2d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_3to4d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_5to6d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_7to8d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_9to11d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_12to14d,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_1week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_2week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_3week,
            COUNT(DISTINCT CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_repayment_sms > 0 THEN sender_group END) AS distinct_banks_loan_repayment_4week,

            -- NUM LOAN REPAYMENT SMS (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_repayment_sms > 0 THEN total_repayment_sms ELSE 0 END) AS num_loan_repayment_sms_4week,

            -- TOTAL LOAN REPAYMENT AMOUNT (10 time windows)
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_last2d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_3to4d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_5to6d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_7to8d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_9to11d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_12to14d,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_1week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_2week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_3week,
            SUM(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 AND total_emi_amount_paid > 0 THEN total_emi_amount_paid ELSE 0 END) AS total_loan_repayment_amount_4week,

            -- MAX LOAN REPAYMENT AMOUNT (10 time windows)
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_last2d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_3to4d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_5to6d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_7to8d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_9to11d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_12to14d,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_1week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_2week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_3week,
            MAX(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN max_emi_amount_paid END) AS max_loan_repayment_amount_4week,

            -- MIN LOAN REPAYMENT AMOUNT (10 time windows)
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 2 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_last2d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 3 AND 4 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_3to4d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 5 AND 6 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_5to6d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 7 AND 8 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_7to8d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 9 AND 11 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_9to11d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 12 AND 14 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_12to14d,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 1 AND 7 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_1week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 8 AND 14 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_2week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 15 AND 21 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_3week,
            MIN(CASE WHEN DATEDIFF('day', dt, CUTOFF_DATE) BETWEEN 22 AND 30 THEN min_emi_amount_paid END) AS min_loan_repayment_amount_4week

        FROM ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_DAY_LEVEL_COLLECT_DPD
        GROUP BY  USER_ID, CUTOFF_DATE
    )
)
);

-- select * from ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_FEATURES_COLLECT_DPD limit 10;

describe table ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_FEATURES_COLLECT_DPD;

-- select count(*) from analytics.data_science.data_early_dpd2_sms_features_collect_dpd;     --- 1995532

-- SELECT
--     COUNT(*) AS total_rows,
--     SUM(CASE WHEN MIN_LOAN_AMOUNT_DISBURSED_12TO14D IS NULL THEN 1 ELSE 0 END) AS null_
--     SUM(CASE WHEN MIN_LOAN_AMOUNT_APPROVED_2WEEK IS NULL THEN 1 ELSE 0 END) AS null_user_id,
--     SUM(CASE WHEN MAX_LOAN_AMOUNT_APPROVED_5TO6D IS NULL THEN 1 ELSE 0 END) AS null_CUTOFF_DATE
-- FROM analytics.data_science.data_early_dpd2_sms_features_collect_dpd;


select count(*) , count (distinct user_id), count(distinct user_id, cutoff_date) from ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_FEATURES_COLLECT_DPD;


-- select * from ANALYTICS.DATA_SCIENCE.DATA_early_dpd2_SMS_FEATURES_COLLECT_DPD;
