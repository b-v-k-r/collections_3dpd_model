 set base_tbl = 'analytics.data_science.field_disposition_base';

CREATE OR REPLACE TABLE analytics.data_science.data_early_dpd2_sms_day_level_features AS (
    WITH bre_loan_base_uid AS (
        SELECT DISTINCT USER_ID, CUTOFF_DATE
        FROM identifier($base_tbl)
    ),
    -- PRE-FILTER: Reduce trillion rows to only relevant data FIRST
    filtered_sms_raw AS (
        SELECT
            a.USER_ID,
            a.DEVICE_ID,
            a.SENDER_GROUP,
            a.TEMPLATE_ID,
            DATE(a.date) AS DT,
            TO_DOUBLE(a.FACT:debit_amount) AS debit_amount,
            TO_DOUBLE(a.FACT:credit_amount) AS credit_amount,
            TO_DOUBLE(a.FACT:available_balance) AS available_balance,
            TO_DOUBLE(a.FACT:total_due_amount) AS total_due_amount,
            TO_DOUBLE(a.FACT:available_limit) AS available_limit,
            LOWER(a.FACT:mode_of_transaction::TEXT) AS mode_transac,
            a.FACT:label::TEXT AS label,
            a.FACT:category::TEXT AS category,
            a.FACT:cheque_number::STRING AS cheque_number,
            a.FACT:emi_overdue::TEXT AS emi_overdue,
            a.FACT:purpose::STRING AS purpose,
            a.FACT:bounce_reason::STRING AS bounce_reason,
            a.FACT:bounce_type::STRING AS bounce_type,
            TO_DOUBLE(a.FACT:bounce_amount) AS bounce_amount,
            a.FACT:min_balance::STRING AS min_balance,
            a.FACT:min_balance_breach::STRING AS min_balance_breach,
            a.FACT:loan_default::STRING AS loan_default,
            a.FACT:legal_action_taken::STRING AS legal_action_taken,
            a.FACT:bill_overdue AS bill_overdue,
            TO_DOUBLE(a.FACT:days_past_due) AS days_past_due,
            TO_DOUBLE(a.FACT:late_fee) AS late_fee,
            TO_DOUBLE(a.FACT:overdue_amount) AS overdue_amount
        FROM RAW.DEVICE_DATA_PROD.SMS_FACTS_VW a
        WHERE
            -- Filter 1: Only your users (pushdown filter)
            a.user_id IN (SELECT distinct USER_ID FROM bre_loan_base_uid)
            -- Filter 2: Date range (broad to cover all cutoff dates)
            AND DATE(a.date) >= (SELECT MIN(CUTOFF_DATE) - 30 FROM bre_loan_base_uid)
            AND DATE(a.date) < (SELECT MAX(CUTOFF_DATE) FROM bre_loan_base_uid)
    ),
    -- Now apply precise filters with cutoff_date logic
    sms_with_cutoff AS (
        SELECT f.*, b.CUTOFF_DATE
        FROM filtered_sms_raw f
        INNER JOIN bre_loan_base_uid b
            ON TRIM(f.user_id) = TRIM(b.user_id)
            AND f.DT < b.CUTOFF_DATE
            AND f.DT >= DATEADD('day', -30, b.CUTOFF_DATE)
    ),
    transactional_features AS (
        SELECT
            USER_ID, DEVICE_ID, DT, CUTOFF_DATE,
            COUNT(*) AS NUM_TOTAL_SMS,
            SUM(CASE WHEN template_id IS NOT NULL THEN 1 ELSE 0 END) AS NUM_READABLE_SMS,

            -- Credit Card features (compact)
            MAX(CASE WHEN category = 'credit_card' THEN total_due_amount END) AS MAX_total_due_amount_CC,
            MIN(CASE WHEN category = 'credit_card' THEN total_due_amount END) AS MIN_total_due_amount_CC,
            MAX(CASE WHEN category = 'credit_card' THEN available_limit END) AS MAX_available_limit_CC,
            MIN(CASE WHEN category = 'credit_card' THEN available_limit END) AS MIN_available_limit_CC,

            SUM(CASE WHEN category = 'credit_card' AND credit_amount > 1 AND credit_amount < 100 THEN 1 ELSE 0 END) AS NUM_credit_txns_lessThan_100_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount > 1 AND debit_amount < 100 THEN 1 ELSE 0 END) AS NUM_debit_txns_lessThan_100_CC,
            SUM(CASE WHEN category = 'credit_card' AND credit_amount >= 100 AND credit_amount < 500 THEN 1 ELSE 0 END) AS NUM_credit_txns_100_to_500_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount >= 100 AND debit_amount < 500 THEN 1 ELSE 0 END) AS NUM_debit_txns_100_to_500_CC,
            SUM(CASE WHEN category = 'credit_card' AND credit_amount >= 500 AND credit_amount < 2000 THEN 1 ELSE 0 END) AS NUM_credit_txns_500_to_2000_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount >= 500 AND debit_amount < 2000 THEN 1 ELSE 0 END) AS NUM_debit_txns_500_to_2000_CC,
            SUM(CASE WHEN category = 'credit_card' AND credit_amount >= 2000 AND credit_amount < 5000 THEN 1 ELSE 0 END) AS NUM_credit_txns_2000_to_5000_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount >= 2000 AND debit_amount < 5000 THEN 1 ELSE 0 END) AS NUM_debit_txns_2000_to_5000_CC,
            SUM(CASE WHEN category = 'credit_card' AND credit_amount >= 5000 AND credit_amount < 10000 THEN 1 ELSE 0 END) AS NUM_credit_txns_5000_to_10000_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount >= 5000 AND debit_amount < 10000 THEN 1 ELSE 0 END) AS NUM_debit_txns_5000_to_10000_CC,
            SUM(CASE WHEN category = 'credit_card' AND credit_amount >= 10000 THEN 1 ELSE 0 END) AS NUM_credit_txns_greaterThan_10000_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount >= 10000 THEN 1 ELSE 0 END) AS NUM_debit_txns_greaterThan_10000_CC,
            SUM(CASE WHEN category = 'credit_card' AND credit_amount > 1 THEN 1 ELSE 0 END) AS NUM_credit_txns_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount > 1 THEN 1 ELSE 0 END) AS NUM_debit_txns_CC,
            SUM(CASE WHEN category = 'credit_card' AND credit_amount > 1 THEN credit_amount ELSE 0 END) AS TOTAL_CREDIT_AMOUNT_CC,
            MIN(CASE WHEN category = 'credit_card' AND credit_amount > 1 THEN credit_amount END) AS MIN_cdt_amt_CC,
            MAX(CASE WHEN category = 'credit_card' AND credit_amount > 1 THEN credit_amount END) AS MAX_cdt_amt_CC,
            SUM(CASE WHEN category = 'credit_card' AND debit_amount > 1 THEN debit_amount ELSE 0 END) AS TOTAL_DEBIT_AMOUNT_CC,
            MIN(CASE WHEN category = 'credit_card' AND debit_amount > 1 THEN debit_amount END) AS MIN_debit_amount_CC,
            MAX(CASE WHEN category = 'credit_card' AND debit_amount > 1 THEN debit_amount END) AS MAX_debit_amount_CC,

            -- Bank Account features (compact)
            MAX(CASE WHEN category = 'bank_account' THEN available_balance END) AS MAX_available_balance,
            MIN(CASE WHEN category = 'bank_account' THEN available_balance END) AS MIN_available_balance,
            SUM(CASE WHEN category = 'bank_account' THEN available_balance END) AS total_available_balance,
            SUM(CASE WHEN category = 'bank_account' AND available_balance IS NOT NULL THEN 1 ELSE 0 END) AS num_available_balance,

            SUM(CASE WHEN category = 'bank_account' AND credit_amount > 1 AND credit_amount < 100 THEN 1 ELSE 0 END) AS NUM_credit_txns_lessThan_100,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount > 1 AND debit_amount < 100 THEN 1 ELSE 0 END) AS NUM_debit_txns_lessThan_100,
            SUM(CASE WHEN category = 'bank_account' AND credit_amount >= 100 AND credit_amount < 500 THEN 1 ELSE 0 END) AS NUM_credit_txns_100_to_500,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount >= 100 AND debit_amount < 500 THEN 1 ELSE 0 END) AS NUM_debit_txns_100_to_500,
            SUM(CASE WHEN category = 'bank_account' AND credit_amount >= 500 AND credit_amount < 2000 THEN 1 ELSE 0 END) AS NUM_credit_txns_500_to_2000,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount >= 500 AND debit_amount < 2000 THEN 1 ELSE 0 END) AS NUM_debit_txns_500_to_2000,
            SUM(CASE WHEN category = 'bank_account' AND credit_amount >= 2000 AND credit_amount < 5000 THEN 1 ELSE 0 END) AS NUM_credit_txns_2000_to_5000,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount >= 2000 AND debit_amount < 5000 THEN 1 ELSE 0 END) AS NUM_debit_txns_2000_to_5000,
            SUM(CASE WHEN category = 'bank_account' AND credit_amount >= 5000 AND credit_amount < 10000 THEN 1 ELSE 0 END) AS NUM_credit_txns_5000_to_10000,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount >= 5000 AND debit_amount < 10000 THEN 1 ELSE 0 END) AS NUM_debit_txns_5000_to_10000,
            SUM(CASE WHEN category = 'bank_account' AND credit_amount >= 10000 THEN 1 ELSE 0 END) AS NUM_credit_txns_greaterThan_10000,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount >= 10000 THEN 1 ELSE 0 END) AS NUM_debit_txns_greaterThan_10000,
            SUM(CASE WHEN category = 'bank_account' AND credit_amount > 1 THEN 1 ELSE 0 END) AS NUM_credit_txns,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount > 1 THEN 1 ELSE 0 END) AS NUM_debit_txns,
            SUM(CASE WHEN category = 'bank_account' AND credit_amount > 1 THEN credit_amount ELSE 0 END) AS TOTAL_CREDIT_AMOUNT,
            MIN(CASE WHEN category = 'bank_account' AND credit_amount > 1 THEN credit_amount END) AS MIN_cdt_amt,
            MAX(CASE WHEN category = 'bank_account' AND credit_amount > 1 THEN credit_amount END) AS MAX_cdt_amt,
            SUM(CASE WHEN category = 'bank_account' AND debit_amount > 1 THEN debit_amount ELSE 0 END) AS TOTAL_DEBIT_AMOUNT,
            MIN(CASE WHEN category = 'bank_account' AND debit_amount > 1 THEN debit_amount END) AS MIN_debit_amount,
            MAX(CASE WHEN category = 'bank_account' AND debit_amount > 1 THEN debit_amount END) AS MAX_debit_amount,

            -- Transaction mode features (compact)
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'upi' AND debit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_DEBIT_COUNT_UPI,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'atm' AND debit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_DEBIT_COUNT_atm,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac IN ('rtgs','neft','imps','dd') AND debit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_DEBIT_COUNT_bank_transfer,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cheque' AND debit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_DEBIT_COUNT_cheque,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cash' AND debit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_DEBIT_COUNT_cash,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'upi' AND credit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_CREDIT_COUNT_UPI,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'atm' AND credit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_CREDIT_COUNT_atm,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac IN ('rtgs','neft','imps','dd') AND credit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_CREDIT_COUNT_bank_transfer,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cheque' AND credit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_CREDIT_COUNT_cheque,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cash' AND credit_amount > 1 THEN 1 ELSE 0 END) AS TOTAL_CREDIT_COUNT_cash,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'upi' AND credit_amount > 1 THEN credit_amount ELSE 0 END) AS TOTAL_CREDIT_AMOUNT_UPI,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'atm' AND credit_amount > 1 THEN credit_amount ELSE 0 END) AS TOTAL_CREDIT_AMOUNT_atm,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac IN ('rtgs','neft','imps','dd') AND credit_amount > 1 THEN credit_amount ELSE 0 END) AS TOTAL_CREDIT_AMOUNT_bank_transfer,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cheque' AND credit_amount > 1 THEN credit_amount ELSE 0 END) AS TOTAL_CREDIT_AMOUNT_cheque,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cash' AND credit_amount > 1 THEN credit_amount ELSE 0 END) AS TOTAL_CREDIT_AMOUNT_cash,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'upi' AND debit_amount > 1 THEN debit_amount ELSE 0 END) AS TOTAL_DEBIT_AMOUNT_UPI,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'atm' AND debit_amount > 1 THEN debit_amount ELSE 0 END) AS TOTAL_DEBIT_AMOUNT_atm,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac IN ('rtgs','neft','imps','dd') AND debit_amount > 1 THEN debit_amount ELSE 0 END) AS TOTAL_DEBIT_AMOUNT_bank_transfer,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cheque' AND debit_amount > 1 THEN debit_amount ELSE 0 END) AS TOTAL_DEBIT_AMOUNT_cheque,
            SUM(CASE WHEN category = 'bank_account' AND mode_transac = 'cash' AND debit_amount > 1 THEN debit_amount ELSE 0 END) AS TOTAL_DEBIT_AMOUNT_cash
        FROM sms_with_cutoff
        GROUP BY USER_ID, DEVICE_ID, DT, CUTOFF_DATE
    ),
    cheque_bounces AS (
        WITH base2 AS (
            SELECT sender_group, user_id, device_id, DT, cheque_number, CUTOFF_DATE
            FROM sms_with_cutoff
            WHERE (category = 'bank_account' AND label = 'cheque_bounce' AND (purpose <> 'non_credit' OR purpose IS NULL) AND (bounce_reason NOT IN ('not_related_to_insufficient_balance','damaged_cheque','signature_mismatch') OR bounce_reason IS NULL))
               OR (category = 'loan' AND label = 'reminder' AND bounce_type = 'cheque_bounce' AND (bounce_reason NOT IN ('not_related_to_insufficient_balance','damaged_cheque','signature_mismatch') OR bounce_reason IS NULL))
        ),
        a2 AS (SELECT DISTINCT user_id, device_id, sender_group, dt, cheque_number, CUTOFF_DATE FROM base2 WHERE cheque_number IS NOT NULL),
        cheque_number AS (
            SELECT user_id, device_id, sender_group, dt, cheque_number, CUTOFF_DATE
            FROM (SELECT a.*, LAG(dt) OVER (PARTITION BY user_id, device_id, sender_group, cheque_number, CUTOFF_DATE ORDER BY dt) AS prev_dt FROM a2 a)
            WHERE dt <> prev_dt + 1 OR prev_dt IS NULL
        ),
        b2 AS (SELECT DISTINCT user_id, device_id, sender_group, dt, CUTOFF_DATE FROM base2 WHERE cheque_number IS NULL),
        without_cheque_number AS (
            SELECT user_id, device_id, sender_group, dt, CUTOFF_DATE
            FROM (SELECT b.*, LAG(dt) OVER (PARTITION BY user_id, device_id, sender_group, CUTOFF_DATE ORDER BY dt) AS prev_dt FROM b2 b)
            WHERE dt <> prev_dt + 1 OR prev_dt IS NULL
        ),
        overall2 AS (
            SELECT d.user_id, d.device_id, d.sender_group, d.dt, d.CUTOFF_DATE, NULL AS cheque_number
            FROM without_cheque_number d
            WHERE NOT EXISTS (SELECT 1 FROM a2 e WHERE e.user_id = d.user_id AND e.device_id = d.device_id AND e.sender_group = d.sender_group AND e.cutoff_date = d.cutoff_date AND d.dt BETWEEN e.dt - 1 AND e.dt + 1)
            UNION
            SELECT user_id, device_id, sender_group, dt, CUTOFF_DATE, cheque_number FROM cheque_number
        )
        SELECT user_id, device_id, dt, CUTOFF_DATE, COUNT(1) AS num_cheque_bounces FROM overall2 GROUP BY 1, 2, 3, 4
    ),
    nach_bounces AS (
        WITH base3 AS (
            SELECT SENDER_GROUP, user_id, device_id, DT, CUTOFF_DATE,
                CASE WHEN category = 'loan' AND label = 'reminder' THEN FLOOR(overdue_amount) WHEN category = 'bank_account' AND label = 'nach_bounce' THEN FLOOR(bounce_amount) END AS bounce_amount
            FROM sms_with_cutoff
            WHERE (category = 'bank_account' AND label = 'nach_bounce' AND (purpose <> 'non_credit' OR purpose IS NULL) AND (bounce_reason NOT IN ('not_related_to_insufficient_balance','damaged_cheque','signature_mismatch') OR bounce_reason IS NULL))
               OR (category = 'loan' AND label = 'reminder' AND bounce_type = 'nach_bounce' AND (bounce_reason NOT IN ('not_related_to_insufficient_balance','damaged_cheque','signature_mismatch') OR bounce_reason IS NULL))
        ),
        a3 AS (SELECT DISTINCT user_id, device_id, sender_group, dt, bounce_amount, CUTOFF_DATE FROM base3 WHERE bounce_amount IS NOT NULL),
        bounce_amount AS (
            SELECT user_id, device_id, sender_group, dt, bounce_amount, CUTOFF_DATE
            FROM (SELECT a.*, LAG(dt) OVER (PARTITION BY user_id, device_id, sender_group, bounce_amount, CUTOFF_DATE ORDER BY dt) AS prev_dt FROM a3 a)
            WHERE dt <> prev_dt + 1 OR prev_dt IS NULL
        ),
        b3 AS (SELECT DISTINCT user_id, device_id, sender_group, dt, CUTOFF_DATE FROM base3 WHERE bounce_amount IS NULL),
        without_bounce_amount AS (
            SELECT user_id, device_id, sender_group, dt, CUTOFF_DATE
            FROM (SELECT b.*, LAG(dt) OVER (PARTITION BY user_id, device_id, sender_group, CUTOFF_DATE ORDER BY dt) AS prev_dt FROM b3 b)
            WHERE dt <> prev_dt + 1 OR prev_dt IS NULL
        ),
        overall3 AS (
            SELECT d.user_id, d.device_id, d.sender_group, d.dt, d.CUTOFF_DATE, NULL AS bounce_amount
            FROM without_bounce_amount d
            WHERE NOT EXISTS (SELECT 1 FROM a3 e WHERE e.user_id = d.user_id AND e.device_id = d.device_id AND e.sender_group = d.sender_group AND e.cutoff_date = d.cutoff_date AND d.dt BETWEEN e.dt - 1 AND e.dt + 1)
            UNION
            SELECT user_id, device_id, sender_group, dt, CUTOFF_DATE, bounce_amount FROM bounce_amount
        )
        SELECT user_id, device_id, dt, CUTOFF_DATE, COUNT(1) AS num_nach_bounce FROM overall3 GROUP BY 1, 2, 3, 4
    ),
    loan_emi_overdue AS (
        SELECT user_id, device_id, DT, CUTOFF_DATE, COUNT(1) AS loan_emi_overdue_flag
        FROM sms_with_cutoff
        WHERE category = 'loan' AND label = 'reminder' AND (emi_overdue IS NOT NULL OR late_fee IS NOT NULL OR overdue_amount IS NOT NULL OR days_past_due IS NOT NULL)
        GROUP BY user_id, device_id, dt, cutoff_date
    ),
    loan_default AS (
        SELECT user_id, device_id, DT, CUTOFF_DATE, COUNT(1) AS loan_default_flag
        FROM sms_with_cutoff
        WHERE category = 'loan' AND (loan_default IS NOT NULL OR legal_action_taken IS NOT NULL)
        GROUP BY user_id, device_id, dt, cutoff_date
    ),
    credit_card_overdue AS (
        SELECT user_id, device_id, DT, CUTOFF_DATE, COUNT(1) AS credit_card_overdue_flag
        FROM sms_with_cutoff
        WHERE category = 'credit_card' AND label = 'reminder' AND (bill_overdue IS NOT NULL OR days_past_due IS NOT NULL)
        GROUP BY user_id, device_id, dt, cutoff_date
    ),
    credit_card_default AS (
        SELECT user_id, device_id, DT, CUTOFF_DATE, COUNT(1) AS credit_card_default_flag
        FROM sms_with_cutoff
        WHERE category = 'credit_card' AND (loan_default IS NOT NULL OR legal_action_taken IS NOT NULL)
        GROUP BY user_id, device_id, dt, cutoff_date
    ),
    min_balance_breach AS (
        SELECT user_id, device_id, DT, CUTOFF_DATE, COUNT(1) AS min_balance_breach_flag
        FROM sms_with_cutoff
        WHERE category = 'bank_account' AND (min_balance IS NOT NULL OR min_balance_breach IS NOT NULL)
        GROUP BY user_id, device_id, dt, cutoff_date
    )
    SELECT
        t.user_id, t.device_id, t.dt, t.cutoff_date,
        t.NUM_TOTAL_SMS, t.NUM_READABLE_SMS,
        t.MAX_total_due_amount_CC, t.MIN_total_due_amount_CC, t.MAX_available_limit_CC, t.MIN_available_limit_CC,
        t.NUM_credit_txns_lessThan_100_CC, t.NUM_debit_txns_lessThan_100_CC, t.NUM_credit_txns_100_to_500_CC, t.NUM_debit_txns_100_to_500_CC,
        t.NUM_credit_txns_500_to_2000_CC, t.NUM_debit_txns_500_to_2000_CC, t.NUM_credit_txns_2000_to_5000_CC, t.NUM_debit_txns_2000_to_5000_CC,
        t.NUM_credit_txns_5000_to_10000_CC, t.NUM_debit_txns_5000_to_10000_CC, t.NUM_credit_txns_greaterThan_10000_CC, t.NUM_debit_txns_greaterThan_10000_CC,
        t.NUM_credit_txns_CC, t.NUM_debit_txns_CC, t.TOTAL_CREDIT_AMOUNT_CC, t.MIN_cdt_amt_CC, t.MAX_cdt_amt_CC,
        t.TOTAL_DEBIT_AMOUNT_CC, t.MIN_debit_amount_CC, t.MAX_debit_amount_CC,
        t.MAX_available_balance, t.MIN_available_balance, t.total_available_balance, t.num_available_balance,
        t.NUM_credit_txns_lessThan_100, t.NUM_debit_txns_lessThan_100, t.NUM_credit_txns_100_to_500, t.NUM_debit_txns_100_to_500,
        t.NUM_credit_txns_500_to_2000, t.NUM_debit_txns_500_to_2000, t.NUM_credit_txns_2000_to_5000, t.NUM_debit_txns_2000_to_5000,
        t.NUM_credit_txns_5000_to_10000, t.NUM_debit_txns_5000_to_10000, t.NUM_credit_txns_greaterThan_10000, t.NUM_debit_txns_greaterThan_10000,
        t.NUM_credit_txns, t.NUM_debit_txns, t.TOTAL_CREDIT_AMOUNT, t.MIN_cdt_amt, t.MAX_cdt_amt, t.TOTAL_DEBIT_AMOUNT, t.MIN_debit_amount, t.MAX_debit_amount,
        t.TOTAL_DEBIT_COUNT_UPI, t.TOTAL_DEBIT_COUNT_atm, t.TOTAL_DEBIT_COUNT_bank_transfer, t.TOTAL_DEBIT_COUNT_cheque, t.TOTAL_DEBIT_COUNT_cash,
        t.TOTAL_CREDIT_COUNT_UPI, t.TOTAL_CREDIT_COUNT_atm, t.TOTAL_CREDIT_COUNT_bank_transfer, t.TOTAL_CREDIT_COUNT_cheque, t.TOTAL_CREDIT_COUNT_cash,
        t.TOTAL_CREDIT_AMOUNT_UPI, t.TOTAL_CREDIT_AMOUNT_atm, t.TOTAL_CREDIT_AMOUNT_bank_transfer, t.TOTAL_CREDIT_AMOUNT_cheque, t.TOTAL_CREDIT_AMOUNT_cash,
        t.TOTAL_DEBIT_AMOUNT_UPI, t.TOTAL_DEBIT_AMOUNT_atm, t.TOTAL_DEBIT_AMOUNT_bank_transfer, t.TOTAL_DEBIT_AMOUNT_cheque, t.TOTAL_DEBIT_AMOUNT_cash,
        COALESCE(b.num_cheque_bounces, 0) num_cheque_bounces,
        COALESCE(d.num_nach_bounce, 0) num_nach_bounce,
        COALESCE(e.min_balance_breach_flag, 0) min_balance_breach_flag,
        COALESCE(f.loan_default_flag, 0) loan_default_flag,
        COALESCE(h.credit_card_overdue_flag, 0) credit_card_overdue_flag,
        COALESCE(u.loan_emi_overdue_flag, 0) loan_emi_overdue_flag,
        COALESCE(g.credit_card_default_flag, 0) credit_card_default_flag
    FROM transactional_features t
        LEFT JOIN cheque_bounces b ON t.user_id = b.user_id AND t.dt = b.dt AND t.device_id = b.device_id AND t.cutoff_date = b.cutoff_date
        LEFT JOIN nach_bounces d ON t.user_id = d.user_id AND t.dt = d.dt AND t.device_id = d.device_id AND t.cutoff_date = d.cutoff_date
        LEFT JOIN min_balance_breach e ON t.user_id = e.user_id AND t.dt = e.dt AND t.device_id = e.device_id AND t.cutoff_date = e.cutoff_date
        LEFT JOIN loan_default f ON t.user_id = f.user_id AND t.dt = f.dt AND t.device_id = f.device_id AND t.cutoff_date = f.cutoff_date
        LEFT JOIN credit_card_default g ON t.user_id = g.user_id AND t.dt = g.dt AND t.device_id = g.device_id AND t.cutoff_date = g.cutoff_date
        LEFT JOIN credit_card_overdue h ON t.user_id = h.user_id AND t.dt = h.dt AND t.device_id = h.device_id AND t.cutoff_date = h.cutoff_date
        LEFT JOIN loan_emi_overdue u ON t.user_id = u.user_id AND t.dt = u.dt AND t.device_id = u.device_id AND t.cutoff_date = u.cutoff_date
    WHERE t.NUM_TOTAL_SMS > 0
);

-- Verify the results
SELECT * FROM analytics.data_science.data_early_dpd2_sms_day_level_features LIMIT 10;



-----  SMS FINAL FEATURES FROM DAY LEVEL =======================================================    =   =   =   =   =           ==  =       =   =   =

CREATE OR REPLACE TRANSIENT TABLE analytics.data_science.data_early_dpd2_sms_final_features AS (
    WITH device_counts AS (
        SELECT
            l.USER_ID, l.CUTOFF_DATE,
            s.DEVICE_ID,
            COUNT(*) AS device_count
        FROM identifier($base_tbl) l
        LEFT JOIN analytics.data_science.data_early_dpd2_sms_day_level_features s
            ON l.USER_ID = s.user_id
            AND l.CUTOFF_DATE = s.cutoff_date
            AND s.dt BETWEEN DATEADD(DAY, -30, l.CUTOFF_DATE) AND DATEADD(DAY, -1, l.CUTOFF_DATE)
        GROUP BY  l.USER_ID, l.CUTOFF_DATE, s.DEVICE_ID
    ),
    DEVICE AS (
        SELECT  USER_ID, CUTOFF_DATE as CUTOFF_DATE, DEVICE_ID
        FROM (
            SELECT  USER_ID, CUTOFF_DATE, DEVICE_ID, device_count,
                ROW_NUMBER() OVER (PARTITION BY  USER_ID, CUTOFF_DATE ORDER BY device_count DESC) AS rn
            FROM device_counts
        ) AS ranked_devices
        WHERE rn = 1
    ),

    sms_all_features AS (
        SELECT
            user_id, cutoff_date,

            -- ============================================
            -- 1. NUM_TOTAL_SMS (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_TOTAL_SMS END) AS NUM_TOTAL_SMS_last_4_week,

            -- ============================================
            -- 2. NUM_READABLE_SMS (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_READABLE_SMS END) AS NUM_READABLE_SMS_last_4_week,

            -- ============================================
            -- 3. num_cheque_bounces (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN num_cheque_bounces END) AS NUM_CHEQUE_BOUNCES_last_4_week,

            -- ============================================
            -- 4. num_nach_bounce (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN num_nach_bounce END) AS NUM_NACH_BOUNCES_last_4_week,

            -- ============================================
            -- 5. loan_emi_overdue_flag (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN loan_emi_overdue_flag END) AS NUM_LOAN_EMI_OVERDUE_last_4_week,

            -- ============================================
            -- 6. loan_default_flag (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN loan_default_flag END) AS NUM_LOAN_DEFAULT_last_4_week,

            -- ============================================
            -- 7. credit_card_overdue_flag (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN credit_card_overdue_flag END) AS NUM_CC_OVERDUE_last_4_week,

            -- ============================================
            -- 8. credit_card_default_flag (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN credit_card_default_flag END) AS NUM_CC_DEFAULT_last_4_week,

            -- ============================================
            -- 9. min_balance_breach_flag (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN min_balance_breach_flag END) AS NUM_MIN_BALANCE_BREACH_last_4_week,

            -- ============================================
            -- 10. MAX_total_due_amount_CC (10 windows)
            -- ============================================
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_1_2_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_3_4_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_5_6_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_7_8_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_9_11_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_12_14_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_1_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_2_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_3_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MAX_total_due_amount_CC END) AS MAX_TOTAL_DUE_AMOUNT_CC_last_4_week,

            -- ============================================
            -- 11. MIN_total_due_amount_CC (10 windows)
            -- ============================================
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_1_2_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_3_4_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_5_6_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_7_8_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_9_11_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_12_14_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_1_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_2_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_3_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MIN_total_due_amount_CC END) AS MIN_TOTAL_DUE_AMOUNT_CC_last_4_week,

            -- ============================================
            -- 12. MAX_available_limit_CC (10 windows)
            -- ============================================
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_1_2_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_3_4_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_5_6_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_7_8_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_9_11_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_12_14_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_1_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_2_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_3_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MAX_available_limit_CC END) AS MAX_AVAILABLE_LIMIT_CC_last_4_week,

            -- ============================================
            -- 13. MIN_available_limit_CC (10 windows)
            -- ============================================
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_1_2_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_3_4_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_5_6_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_7_8_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_9_11_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_12_14_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_1_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_2_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_3_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MIN_available_limit_CC END) AS MIN_AVAILABLE_LIMIT_CC_last_4_week,

            -- ============================================
            -- 14. NUM_credit_txns_lessThan_100_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_lessThan_100_CC END) AS NUM_CREDIT_TXNS_LT_100_CC_last_4_week,

            -- ============================================
            -- 15. NUM_debit_txns_lessThan_100_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_lessThan_100_CC END) AS NUM_DEBIT_TXNS_LT_100_CC_last_4_week,

            -- ============================================
            -- 16. NUM_credit_txns_100_to_500_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_100_to_500_CC END) AS NUM_CREDIT_TXNS_100_500_CC_last_4_week,

            -- ============================================
            -- 17. NUM_debit_txns_100_to_500_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_100_to_500_CC END) AS NUM_DEBIT_TXNS_100_500_CC_last_4_week,

            -- ============================================
            -- 18. NUM_credit_txns_500_to_2000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_500_to_2000_CC END) AS NUM_CREDIT_TXNS_500_2000_CC_last_4_week,

            -- ============================================
            -- 19. NUM_debit_txns_500_to_2000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_500_to_2000_CC END) AS NUM_DEBIT_TXNS_500_2000_CC_last_4_week,

            -- ============================================
            -- 20. NUM_credit_txns_2000_to_5000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_2000_to_5000_CC END) AS NUM_CREDIT_TXNS_2000_5000_CC_last_4_week,

            -- ============================================
            -- 21. NUM_debit_txns_2000_to_5000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_2000_to_5000_CC END) AS NUM_DEBIT_TXNS_2000_5000_CC_last_4_week,

            -- ============================================
            -- 22. NUM_credit_txns_5000_to_10000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_5000_to_10000_CC END) AS NUM_CREDIT_TXNS_5000_10000_CC_last_4_week,

            -- ============================================
            -- 23. NUM_debit_txns_5000_to_10000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_5000_to_10000_CC END) AS NUM_DEBIT_TXNS_5000_10000_CC_last_4_week,

            -- ============================================
            -- 24. NUM_credit_txns_greaterThan_10000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_greaterThan_10000_CC END) AS NUM_CREDIT_TXNS_GT_10000_CC_last_4_week,

            -- ============================================
            -- 25. NUM_debit_txns_greaterThan_10000_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_greaterThan_10000_CC END) AS NUM_DEBIT_TXNS_GT_10000_CC_last_4_week,

            -- ============================================
            -- 26. NUM_credit_txns_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_CC END) AS NUM_CREDIT_TXNS_CC_last_4_week,

            -- ============================================
            -- 27. NUM_debit_txns_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_CC END) AS NUM_DEBIT_TXNS_CC_last_4_week,

            -- ============================================
            -- 28. TOTAL_CREDIT_AMOUNT_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_CC END) AS TOTAL_CREDIT_AMOUNT_CC_last_4_week,

            -- ============================================
            -- 29. MIN_cdt_amt_CC (10 windows)
            -- ============================================
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_1_2_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_3_4_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_5_6_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_7_8_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_9_11_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_12_14_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_1_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_2_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_3_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MIN_cdt_amt_CC END) AS MIN_CDT_AMT_CC_last_4_week,

            -- ============================================
            -- 30. MAX_cdt_amt_CC (10 windows)
            -- ============================================
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_1_2_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_3_4_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_5_6_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_7_8_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_9_11_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_12_14_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_1_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_2_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_3_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MAX_cdt_amt_CC END) AS MAX_CDT_AMT_CC_last_4_week,

            -- ============================================
            -- 31. TOTAL_DEBIT_AMOUNT_CC (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_CC END) AS TOTAL_DEBIT_AMOUNT_CC_last_4_week,

            -- ============================================
            -- 32. MIN_debit_amount_CC (10 windows)
            -- ============================================
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_1_2_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_3_4_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_5_6_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_7_8_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_9_11_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_12_14_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_1_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_2_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_3_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MIN_debit_amount_CC END) AS MIN_DEBIT_AMOUNT_CC_last_4_week,

            -- ============================================
            -- 33. MAX_debit_amount_CC (10 windows)
            -- ============================================
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_1_2_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_3_4_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_5_6_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_7_8_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_9_11_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_12_14_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_1_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_2_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_3_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MAX_debit_amount_CC END) AS MAX_DEBIT_AMOUNT_CC_last_4_week,

            -- ============================================
            -- 34. MAX_available_balance (10 windows)
            -- ============================================
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_1_2_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_3_4_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_5_6_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_7_8_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_9_11_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_12_14_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_1_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_2_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_3_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MAX_available_balance END) AS MAX_AVAILABLE_BALANCE_last_4_week,

            -- ============================================
            -- 35. MIN_available_balance (10 windows)
            -- ============================================
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_1_2_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_3_4_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_5_6_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_7_8_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_9_11_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_12_14_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_1_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_2_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_3_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MIN_available_balance END) AS MIN_AVAILABLE_BALANCE_last_4_week,

            -- ============================================
            -- 36. total_available_balance (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN total_available_balance END) AS TOTAL_AVAILABLE_BALANCE_last_4_week,

            -- ============================================
            -- 37. num_available_balance (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN num_available_balance END) AS NUM_AVAILABLE_BALANCE_last_4_week,

            -- ============================================
            -- 38. NUM_credit_txns_lessThan_100 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_lessThan_100 END) AS NUM_CREDIT_TXNS_LT_100_last_4_week,

                        -- ============================================
            -- 39. NUM_debit_txns_lessThan_100 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_lessThan_100 END) AS NUM_DEBIT_TXNS_LT_100_last_4_week,

            -- ============================================
            -- 40. NUM_credit_txns_100_to_500 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_100_to_500 END) AS NUM_CREDIT_TXNS_100_500_last_4_week,

            -- ============================================
            -- 41. NUM_debit_txns_100_to_500 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_100_to_500 END) AS NUM_DEBIT_TXNS_100_500_last_4_week,

            -- ============================================
            -- 42. NUM_credit_txns_500_to_2000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_500_to_2000 END) AS NUM_CREDIT_TXNS_500_2000_last_4_week,

            -- ============================================
            -- 43. NUM_debit_txns_500_to_2000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_500_to_2000 END) AS NUM_DEBIT_TXNS_500_2000_last_4_week,

            -- ============================================
            -- 44. NUM_credit_txns_2000_to_5000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_2000_to_5000 END) AS NUM_CREDIT_TXNS_2000_5000_last_4_week,

            -- ============================================
            -- 45. NUM_debit_txns_2000_to_5000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_2000_to_5000 END) AS NUM_DEBIT_TXNS_2000_5000_last_4_week,

            -- ============================================
            -- 46. NUM_credit_txns_5000_to_10000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_5000_to_10000 END) AS NUM_CREDIT_TXNS_5000_10000_last_4_week,

            -- ============================================
            -- 47. NUM_debit_txns_5000_to_10000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_5000_to_10000 END) AS NUM_DEBIT_TXNS_5000_10000_last_4_week,

            -- ============================================
            -- 48. NUM_credit_txns_greaterThan_10000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns_greaterThan_10000 END) AS NUM_CREDIT_TXNS_GT_10000_last_4_week,

            -- ============================================
            -- 49. NUM_debit_txns_greaterThan_10000 (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns_greaterThan_10000 END) AS NUM_DEBIT_TXNS_GT_10000_last_4_week,

            -- ============================================
            -- 50. NUM_credit_txns (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_credit_txns END) AS NUM_CREDIT_TXNS_last_4_week,

            -- ============================================
            -- 51. NUM_debit_txns (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN NUM_debit_txns END) AS NUM_DEBIT_TXNS_last_4_week,

            -- ============================================
            -- 52. TOTAL_CREDIT_AMOUNT (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_AMOUNT END) AS TOTAL_CREDIT_AMOUNT_last_4_week,

            -- ============================================
            -- 53. MIN_cdt_amt (10 windows)
            -- ============================================
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_1_2_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_3_4_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_5_6_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_7_8_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_9_11_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_12_14_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_1_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_2_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_3_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MIN_cdt_amt END) AS MIN_CDT_AMT_last_4_week,

            -- ============================================
            -- 54. MAX_cdt_amt (10 windows)
            -- ============================================
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_1_2_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_3_4_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_5_6_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_7_8_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_9_11_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_12_14_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_1_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_2_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_3_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MAX_cdt_amt END) AS MAX_CDT_AMT_last_4_week,

            -- ============================================
            -- 55. TOTAL_DEBIT_AMOUNT (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_AMOUNT END) AS TOTAL_DEBIT_AMOUNT_last_4_week,

            -- ============================================
            -- 56. MIN_debit_amount (10 windows)
            -- ============================================
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_1_2_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_3_4_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_5_6_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_7_8_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_9_11_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_12_14_day,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_1_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_2_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_3_week,
            MIN(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MIN_debit_amount END) AS MIN_DEBIT_AMOUNT_last_4_week,

            -- ============================================
            -- 57. MAX_debit_amount (10 windows)
            -- ============================================
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_1_2_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_3_4_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_5_6_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_7_8_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_9_11_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_12_14_day,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_1_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_2_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_3_week,
            MAX(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN MAX_debit_amount END) AS MAX_DEBIT_AMOUNT_last_4_week,

            -- ============================================
            -- 58. TOTAL_DEBIT_COUNT_UPI (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_COUNT_UPI END) AS TOTAL_DEBIT_COUNT_UPI_last_4_week,

            -- ============================================
            -- 59. TOTAL_DEBIT_COUNT_atm (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_COUNT_atm END) AS TOTAL_DEBIT_COUNT_ATM_last_4_week,

            -- ============================================
            -- 60. TOTAL_DEBIT_COUNT_bank_transfer (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_COUNT_bank_transfer END) AS TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_4_week,

            -- ============================================
            -- 61. TOTAL_DEBIT_COUNT_cheque (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_COUNT_cheque END) AS TOTAL_DEBIT_COUNT_CHEQUE_last_4_week,

            -- ============================================
            -- 62. TOTAL_DEBIT_COUNT_cash (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_COUNT_cash END) AS TOTAL_DEBIT_COUNT_CASH_last_4_week,

            -- ============================================
            -- 63. TOTAL_CREDIT_COUNT_UPI (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_COUNT_UPI END) AS TOTAL_CREDIT_COUNT_UPI_last_4_week,

            -- ============================================
            -- 64. TOTAL_CREDIT_COUNT_atm (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_COUNT_atm END) AS TOTAL_CREDIT_COUNT_ATM_last_4_week,

            -- ============================================
            -- 65. TOTAL_CREDIT_COUNT_bank_transfer (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_COUNT_bank_transfer END) AS TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_4_week,

            -- ============================================
            -- 66. TOTAL_CREDIT_COUNT_cheque (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_COUNT_cheque END) AS TOTAL_CREDIT_COUNT_CHEQUE_last_4_week,

            -- ============================================
            -- 67. TOTAL_CREDIT_COUNT_cash (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_COUNT_cash END) AS TOTAL_CREDIT_COUNT_CASH_last_4_week,

            -- ============================================
            -- 68. TOTAL_CREDIT_AMOUNT_UPI (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_UPI END) AS TOTAL_CREDIT_AMOUNT_UPI_last_4_week,

            -- ============================================
            -- 69. TOTAL_CREDIT_AMOUNT_atm (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_atm END) AS TOTAL_CREDIT_AMOUNT_ATM_last_4_week,

            -- ============================================
            -- 70. TOTAL_CREDIT_AMOUNT_bank_transfer (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_bank_transfer END) AS TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_4_week,

            -- ============================================
            -- 71. TOTAL_CREDIT_AMOUNT_cheque (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cheque END) AS TOTAL_CREDIT_AMOUNT_CHEQUE_last_4_week,

            -- ============================================
            -- 72. TOTAL_CREDIT_AMOUNT_cash (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_CREDIT_AMOUNT_cash END) AS TOTAL_CREDIT_AMOUNT_CASH_last_4_week,

            -- ============================================
            -- 73. TOTAL_DEBIT_AMOUNT_UPI (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_UPI END) AS TOTAL_DEBIT_AMOUNT_UPI_last_4_week,

            -- ============================================
            -- 74. TOTAL_DEBIT_AMOUNT_atm (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_atm END) AS TOTAL_DEBIT_AMOUNT_ATM_last_4_week,

            -- ============================================
            -- 75. TOTAL_DEBIT_AMOUNT_bank_transfer (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_bank_transfer END) AS TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_4_week,

            -- ============================================
            -- 76. TOTAL_DEBIT_AMOUNT_cheque (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cheque END) AS TOTAL_DEBIT_AMOUNT_CHEQUE_last_4_week,

            -- ============================================
            -- 77. TOTAL_DEBIT_AMOUNT_cash (10 windows)
            -- ============================================
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -2, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_1_2_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -4, cutoff_date) AND DATEADD(DAY, -3, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_3_4_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -6, cutoff_date) AND DATEADD(DAY, -5, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_5_6_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -8, cutoff_date) AND DATEADD(DAY, -7, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_7_8_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -11, cutoff_date) AND DATEADD(DAY, -9, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_9_11_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -12, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_12_14_day,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -7, cutoff_date) AND DATEADD(DAY, -1, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_1_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -14, cutoff_date) AND DATEADD(DAY, -8, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_2_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -21, cutoff_date) AND DATEADD(DAY, -15, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_3_week,
            SUM(CASE WHEN dt BETWEEN DATEADD(DAY, -30, cutoff_date) AND DATEADD(DAY, -22, cutoff_date) THEN TOTAL_DEBIT_AMOUNT_cash END) AS TOTAL_DEBIT_AMOUNT_CASH_last_4_week

        FROM (
            SELECT d.USER_ID,  d.cutoff_date, s.* EXCLUDE (USER_ID, DEVICE_ID, CUTOFF_DATE)
            FROM DEVICE d
            LEFT JOIN analytics.data_science.data_early_dpd2_sms_day_level_features s
                ON s.USER_ID = d.USER_ID
                AND s.cutoff_date = d.cutoff_date
                AND (s.DEVICE_ID = d.DEVICE_ID OR (s.DEVICE_ID IS NULL AND d.DEVICE_ID IS NULL))
                AND s.DT BETWEEN DATEADD(DAY, -30, d.cutoff_date) AND DATEADD(DAY, -1, d.cutoff_date)
        )
        GROUP BY  user_id, cutoff_date
        ),

    -- ============================================
    -- DERIVED FEATURES: AVERAGES & RATIOS
    -- ============================================
    sms_derived_features AS (
        SELECT
             user_id, cutoff_date,

            -- All base features from sms_all_features
            NUM_TOTAL_SMS_last_1_2_day, NUM_TOTAL_SMS_last_3_4_day, NUM_TOTAL_SMS_last_5_6_day, NUM_TOTAL_SMS_last_7_8_day, NUM_TOTAL_SMS_last_9_11_day, NUM_TOTAL_SMS_last_12_14_day,
            NUM_TOTAL_SMS_last_1_week, NUM_TOTAL_SMS_last_2_week, NUM_TOTAL_SMS_last_3_week, NUM_TOTAL_SMS_last_4_week,

            NUM_READABLE_SMS_last_1_2_day, NUM_READABLE_SMS_last_3_4_day, NUM_READABLE_SMS_last_5_6_day, NUM_READABLE_SMS_last_7_8_day, NUM_READABLE_SMS_last_9_11_day, NUM_READABLE_SMS_last_12_14_day,
            NUM_READABLE_SMS_last_1_week, NUM_READABLE_SMS_last_2_week, NUM_READABLE_SMS_last_3_week, NUM_READABLE_SMS_last_4_week,

            NUM_CHEQUE_BOUNCES_last_1_2_day, NUM_CHEQUE_BOUNCES_last_3_4_day, NUM_CHEQUE_BOUNCES_last_5_6_day, NUM_CHEQUE_BOUNCES_last_7_8_day, NUM_CHEQUE_BOUNCES_last_9_11_day, NUM_CHEQUE_BOUNCES_last_12_14_day,
            NUM_CHEQUE_BOUNCES_last_1_week, NUM_CHEQUE_BOUNCES_last_2_week, NUM_CHEQUE_BOUNCES_last_3_week, NUM_CHEQUE_BOUNCES_last_4_week,

            NUM_NACH_BOUNCES_last_1_2_day, NUM_NACH_BOUNCES_last_3_4_day, NUM_NACH_BOUNCES_last_5_6_day, NUM_NACH_BOUNCES_last_7_8_day, NUM_NACH_BOUNCES_last_9_11_day, NUM_NACH_BOUNCES_last_12_14_day,
            NUM_NACH_BOUNCES_last_1_week, NUM_NACH_BOUNCES_last_2_week, NUM_NACH_BOUNCES_last_3_week, NUM_NACH_BOUNCES_last_4_week,

            NUM_LOAN_EMI_OVERDUE_last_1_2_day, NUM_LOAN_EMI_OVERDUE_last_3_4_day, NUM_LOAN_EMI_OVERDUE_last_5_6_day, NUM_LOAN_EMI_OVERDUE_last_7_8_day, NUM_LOAN_EMI_OVERDUE_last_9_11_day, NUM_LOAN_EMI_OVERDUE_last_12_14_day,
            NUM_LOAN_EMI_OVERDUE_last_1_week, NUM_LOAN_EMI_OVERDUE_last_2_week, NUM_LOAN_EMI_OVERDUE_last_3_week, NUM_LOAN_EMI_OVERDUE_last_4_week,

            NUM_LOAN_DEFAULT_last_1_2_day, NUM_LOAN_DEFAULT_last_3_4_day, NUM_LOAN_DEFAULT_last_5_6_day, NUM_LOAN_DEFAULT_last_7_8_day, NUM_LOAN_DEFAULT_last_9_11_day, NUM_LOAN_DEFAULT_last_12_14_day,
            NUM_LOAN_DEFAULT_last_1_week, NUM_LOAN_DEFAULT_last_2_week, NUM_LOAN_DEFAULT_last_3_week, NUM_LOAN_DEFAULT_last_4_week,

            NUM_CC_OVERDUE_last_1_2_day, NUM_CC_OVERDUE_last_3_4_day, NUM_CC_OVERDUE_last_5_6_day, NUM_CC_OVERDUE_last_7_8_day, NUM_CC_OVERDUE_last_9_11_day, NUM_CC_OVERDUE_last_12_14_day,
            NUM_CC_OVERDUE_last_1_week, NUM_CC_OVERDUE_last_2_week, NUM_CC_OVERDUE_last_3_week, NUM_CC_OVERDUE_last_4_week,

            NUM_CC_DEFAULT_last_1_2_day, NUM_CC_DEFAULT_last_3_4_day, NUM_CC_DEFAULT_last_5_6_day, NUM_CC_DEFAULT_last_7_8_day, NUM_CC_DEFAULT_last_9_11_day, NUM_CC_DEFAULT_last_12_14_day,
            NUM_CC_DEFAULT_last_1_week, NUM_CC_DEFAULT_last_2_week, NUM_CC_DEFAULT_last_3_week, NUM_CC_DEFAULT_last_4_week,

            NUM_MIN_BALANCE_BREACH_last_1_2_day, NUM_MIN_BALANCE_BREACH_last_3_4_day, NUM_MIN_BALANCE_BREACH_last_5_6_day, NUM_MIN_BALANCE_BREACH_last_7_8_day, NUM_MIN_BALANCE_BREACH_last_9_11_day, NUM_MIN_BALANCE_BREACH_last_12_14_day,
            NUM_MIN_BALANCE_BREACH_last_1_week, NUM_MIN_BALANCE_BREACH_last_2_week, NUM_MIN_BALANCE_BREACH_last_3_week, NUM_MIN_BALANCE_BREACH_last_4_week,

            MAX_TOTAL_DUE_AMOUNT_CC_last_1_2_day, MAX_TOTAL_DUE_AMOUNT_CC_last_3_4_day, MAX_TOTAL_DUE_AMOUNT_CC_last_5_6_day, MAX_TOTAL_DUE_AMOUNT_CC_last_7_8_day, MAX_TOTAL_DUE_AMOUNT_CC_last_9_11_day, MAX_TOTAL_DUE_AMOUNT_CC_last_12_14_day,
            MAX_TOTAL_DUE_AMOUNT_CC_last_1_week, MAX_TOTAL_DUE_AMOUNT_CC_last_2_week, MAX_TOTAL_DUE_AMOUNT_CC_last_3_week, MAX_TOTAL_DUE_AMOUNT_CC_last_4_week,

            MIN_TOTAL_DUE_AMOUNT_CC_last_1_2_day, MIN_TOTAL_DUE_AMOUNT_CC_last_3_4_day, MIN_TOTAL_DUE_AMOUNT_CC_last_5_6_day, MIN_TOTAL_DUE_AMOUNT_CC_last_7_8_day, MIN_TOTAL_DUE_AMOUNT_CC_last_9_11_day, MIN_TOTAL_DUE_AMOUNT_CC_last_12_14_day,
            MIN_TOTAL_DUE_AMOUNT_CC_last_1_week, MIN_TOTAL_DUE_AMOUNT_CC_last_2_week, MIN_TOTAL_DUE_AMOUNT_CC_last_3_week, MIN_TOTAL_DUE_AMOUNT_CC_last_4_week,

            MAX_AVAILABLE_LIMIT_CC_last_1_2_day, MAX_AVAILABLE_LIMIT_CC_last_3_4_day, MAX_AVAILABLE_LIMIT_CC_last_5_6_day, MAX_AVAILABLE_LIMIT_CC_last_7_8_day, MAX_AVAILABLE_LIMIT_CC_last_9_11_day, MAX_AVAILABLE_LIMIT_CC_last_12_14_day,
            MAX_AVAILABLE_LIMIT_CC_last_1_week, MAX_AVAILABLE_LIMIT_CC_last_2_week, MAX_AVAILABLE_LIMIT_CC_last_3_week, MAX_AVAILABLE_LIMIT_CC_last_4_week,

            MIN_AVAILABLE_LIMIT_CC_last_1_2_day, MIN_AVAILABLE_LIMIT_CC_last_3_4_day, MIN_AVAILABLE_LIMIT_CC_last_5_6_day, MIN_AVAILABLE_LIMIT_CC_last_7_8_day, MIN_AVAILABLE_LIMIT_CC_last_9_11_day, MIN_AVAILABLE_LIMIT_CC_last_12_14_day,
            MIN_AVAILABLE_LIMIT_CC_last_1_week, MIN_AVAILABLE_LIMIT_CC_last_2_week, MIN_AVAILABLE_LIMIT_CC_last_3_week, MIN_AVAILABLE_LIMIT_CC_last_4_week,

            NUM_CREDIT_TXNS_LT_100_CC_last_1_2_day, NUM_CREDIT_TXNS_LT_100_CC_last_3_4_day, NUM_CREDIT_TXNS_LT_100_CC_last_5_6_day, NUM_CREDIT_TXNS_LT_100_CC_last_7_8_day, NUM_CREDIT_TXNS_LT_100_CC_last_9_11_day, NUM_CREDIT_TXNS_LT_100_CC_last_12_14_day,
            NUM_CREDIT_TXNS_LT_100_CC_last_1_week, NUM_CREDIT_TXNS_LT_100_CC_last_2_week, NUM_CREDIT_TXNS_LT_100_CC_last_3_week, NUM_CREDIT_TXNS_LT_100_CC_last_4_week,

            NUM_DEBIT_TXNS_LT_100_CC_last_1_2_day, NUM_DEBIT_TXNS_LT_100_CC_last_3_4_day, NUM_DEBIT_TXNS_LT_100_CC_last_5_6_day, NUM_DEBIT_TXNS_LT_100_CC_last_7_8_day, NUM_DEBIT_TXNS_LT_100_CC_last_9_11_day, NUM_DEBIT_TXNS_LT_100_CC_last_12_14_day,
            NUM_DEBIT_TXNS_LT_100_CC_last_1_week, NUM_DEBIT_TXNS_LT_100_CC_last_2_week, NUM_DEBIT_TXNS_LT_100_CC_last_3_week, NUM_DEBIT_TXNS_LT_100_CC_last_4_week,

            NUM_CREDIT_TXNS_100_500_CC_last_1_2_day, NUM_CREDIT_TXNS_100_500_CC_last_3_4_day, NUM_CREDIT_TXNS_100_500_CC_last_5_6_day, NUM_CREDIT_TXNS_100_500_CC_last_7_8_day, NUM_CREDIT_TXNS_100_500_CC_last_9_11_day, NUM_CREDIT_TXNS_100_500_CC_last_12_14_day,
            NUM_CREDIT_TXNS_100_500_CC_last_1_week, NUM_CREDIT_TXNS_100_500_CC_last_2_week, NUM_CREDIT_TXNS_100_500_CC_last_3_week, NUM_CREDIT_TXNS_100_500_CC_last_4_week,

            NUM_DEBIT_TXNS_100_500_CC_last_1_2_day, NUM_DEBIT_TXNS_100_500_CC_last_3_4_day, NUM_DEBIT_TXNS_100_500_CC_last_5_6_day, NUM_DEBIT_TXNS_100_500_CC_last_7_8_day, NUM_DEBIT_TXNS_100_500_CC_last_9_11_day, NUM_DEBIT_TXNS_100_500_CC_last_12_14_day,
            NUM_DEBIT_TXNS_100_500_CC_last_1_week, NUM_DEBIT_TXNS_100_500_CC_last_2_week, NUM_DEBIT_TXNS_100_500_CC_last_3_week, NUM_DEBIT_TXNS_100_500_CC_last_4_week,

            NUM_CREDIT_TXNS_500_2000_CC_last_1_2_day, NUM_CREDIT_TXNS_500_2000_CC_last_3_4_day, NUM_CREDIT_TXNS_500_2000_CC_last_5_6_day, NUM_CREDIT_TXNS_500_2000_CC_last_7_8_day, NUM_CREDIT_TXNS_500_2000_CC_last_9_11_day, NUM_CREDIT_TXNS_500_2000_CC_last_12_14_day,
            NUM_CREDIT_TXNS_500_2000_CC_last_1_week, NUM_CREDIT_TXNS_500_2000_CC_last_2_week, NUM_CREDIT_TXNS_500_2000_CC_last_3_week, NUM_CREDIT_TXNS_500_2000_CC_last_4_week,

            NUM_DEBIT_TXNS_500_2000_CC_last_1_2_day, NUM_DEBIT_TXNS_500_2000_CC_last_3_4_day, NUM_DEBIT_TXNS_500_2000_CC_last_5_6_day, NUM_DEBIT_TXNS_500_2000_CC_last_7_8_day, NUM_DEBIT_TXNS_500_2000_CC_last_9_11_day, NUM_DEBIT_TXNS_500_2000_CC_last_12_14_day,
            NUM_DEBIT_TXNS_500_2000_CC_last_1_week, NUM_DEBIT_TXNS_500_2000_CC_last_2_week, NUM_DEBIT_TXNS_500_2000_CC_last_3_week, NUM_DEBIT_TXNS_500_2000_CC_last_4_week,

            NUM_CREDIT_TXNS_2000_5000_CC_last_1_2_day, NUM_CREDIT_TXNS_2000_5000_CC_last_3_4_day, NUM_CREDIT_TXNS_2000_5000_CC_last_5_6_day, NUM_CREDIT_TXNS_2000_5000_CC_last_7_8_day, NUM_CREDIT_TXNS_2000_5000_CC_last_9_11_day, NUM_CREDIT_TXNS_2000_5000_CC_last_12_14_day,
            NUM_CREDIT_TXNS_2000_5000_CC_last_1_week, NUM_CREDIT_TXNS_2000_5000_CC_last_2_week, NUM_CREDIT_TXNS_2000_5000_CC_last_3_week, NUM_CREDIT_TXNS_2000_5000_CC_last_4_week,

            NUM_DEBIT_TXNS_2000_5000_CC_last_1_2_day, NUM_DEBIT_TXNS_2000_5000_CC_last_3_4_day, NUM_DEBIT_TXNS_2000_5000_CC_last_5_6_day, NUM_DEBIT_TXNS_2000_5000_CC_last_7_8_day, NUM_DEBIT_TXNS_2000_5000_CC_last_9_11_day, NUM_DEBIT_TXNS_2000_5000_CC_last_12_14_day,
            NUM_DEBIT_TXNS_2000_5000_CC_last_1_week, NUM_DEBIT_TXNS_2000_5000_CC_last_2_week, NUM_DEBIT_TXNS_2000_5000_CC_last_3_week, NUM_DEBIT_TXNS_2000_5000_CC_last_4_week,

            NUM_CREDIT_TXNS_5000_10000_CC_last_1_2_day, NUM_CREDIT_TXNS_5000_10000_CC_last_3_4_day, NUM_CREDIT_TXNS_5000_10000_CC_last_5_6_day, NUM_CREDIT_TXNS_5000_10000_CC_last_7_8_day, NUM_CREDIT_TXNS_5000_10000_CC_last_9_11_day, NUM_CREDIT_TXNS_5000_10000_CC_last_12_14_day,
            NUM_CREDIT_TXNS_5000_10000_CC_last_1_week, NUM_CREDIT_TXNS_5000_10000_CC_last_2_week, NUM_CREDIT_TXNS_5000_10000_CC_last_3_week, NUM_CREDIT_TXNS_5000_10000_CC_last_4_week,

            NUM_DEBIT_TXNS_5000_10000_CC_last_1_2_day, NUM_DEBIT_TXNS_5000_10000_CC_last_3_4_day, NUM_DEBIT_TXNS_5000_10000_CC_last_5_6_day, NUM_DEBIT_TXNS_5000_10000_CC_last_7_8_day, NUM_DEBIT_TXNS_5000_10000_CC_last_9_11_day, NUM_DEBIT_TXNS_5000_10000_CC_last_12_14_day,
            NUM_DEBIT_TXNS_5000_10000_CC_last_1_week, NUM_DEBIT_TXNS_5000_10000_CC_last_2_week, NUM_DEBIT_TXNS_5000_10000_CC_last_3_week, NUM_DEBIT_TXNS_5000_10000_CC_last_4_week,

            NUM_CREDIT_TXNS_GT_10000_CC_last_1_2_day, NUM_CREDIT_TXNS_GT_10000_CC_last_3_4_day, NUM_CREDIT_TXNS_GT_10000_CC_last_5_6_day, NUM_CREDIT_TXNS_GT_10000_CC_last_7_8_day, NUM_CREDIT_TXNS_GT_10000_CC_last_9_11_day, NUM_CREDIT_TXNS_GT_10000_CC_last_12_14_day,
            NUM_CREDIT_TXNS_GT_10000_CC_last_1_week, NUM_CREDIT_TXNS_GT_10000_CC_last_2_week, NUM_CREDIT_TXNS_GT_10000_CC_last_3_week, NUM_CREDIT_TXNS_GT_10000_CC_last_4_week,

            NUM_DEBIT_TXNS_GT_10000_CC_last_1_2_day, NUM_DEBIT_TXNS_GT_10000_CC_last_3_4_day, NUM_DEBIT_TXNS_GT_10000_CC_last_5_6_day, NUM_DEBIT_TXNS_GT_10000_CC_last_7_8_day, NUM_DEBIT_TXNS_GT_10000_CC_last_9_11_day, NUM_DEBIT_TXNS_GT_10000_CC_last_12_14_day,
            NUM_DEBIT_TXNS_GT_10000_CC_last_1_week, NUM_DEBIT_TXNS_GT_10000_CC_last_2_week, NUM_DEBIT_TXNS_GT_10000_CC_last_3_week, NUM_DEBIT_TXNS_GT_10000_CC_last_4_week,

            NUM_CREDIT_TXNS_CC_last_1_2_day, NUM_CREDIT_TXNS_CC_last_3_4_day, NUM_CREDIT_TXNS_CC_last_5_6_day, NUM_CREDIT_TXNS_CC_last_7_8_day, NUM_CREDIT_TXNS_CC_last_9_11_day, NUM_CREDIT_TXNS_CC_last_12_14_day,
            NUM_CREDIT_TXNS_CC_last_1_week, NUM_CREDIT_TXNS_CC_last_2_week, NUM_CREDIT_TXNS_CC_last_3_week, NUM_CREDIT_TXNS_CC_last_4_week,

            NUM_DEBIT_TXNS_CC_last_1_2_day, NUM_DEBIT_TXNS_CC_last_3_4_day, NUM_DEBIT_TXNS_CC_last_5_6_day, NUM_DEBIT_TXNS_CC_last_7_8_day, NUM_DEBIT_TXNS_CC_last_9_11_day, NUM_DEBIT_TXNS_CC_last_12_14_day,
            NUM_DEBIT_TXNS_CC_last_1_week, NUM_DEBIT_TXNS_CC_last_2_week, NUM_DEBIT_TXNS_CC_last_3_week, NUM_DEBIT_TXNS_CC_last_4_week,

            TOTAL_CREDIT_AMOUNT_CC_last_1_2_day, TOTAL_CREDIT_AMOUNT_CC_last_3_4_day, TOTAL_CREDIT_AMOUNT_CC_last_5_6_day, TOTAL_CREDIT_AMOUNT_CC_last_7_8_day, TOTAL_CREDIT_AMOUNT_CC_last_9_11_day, TOTAL_CREDIT_AMOUNT_CC_last_12_14_day,
            TOTAL_CREDIT_AMOUNT_CC_last_1_week, TOTAL_CREDIT_AMOUNT_CC_last_2_week, TOTAL_CREDIT_AMOUNT_CC_last_3_week, TOTAL_CREDIT_AMOUNT_CC_last_4_week,

            MIN_CDT_AMT_CC_last_1_2_day, MIN_CDT_AMT_CC_last_3_4_day, MIN_CDT_AMT_CC_last_5_6_day, MIN_CDT_AMT_CC_last_7_8_day, MIN_CDT_AMT_CC_last_9_11_day, MIN_CDT_AMT_CC_last_12_14_day,
            MIN_CDT_AMT_CC_last_1_week, MIN_CDT_AMT_CC_last_2_week, MIN_CDT_AMT_CC_last_3_week, MIN_CDT_AMT_CC_last_4_week,

            MAX_CDT_AMT_CC_last_1_2_day, MAX_CDT_AMT_CC_last_3_4_day, MAX_CDT_AMT_CC_last_5_6_day, MAX_CDT_AMT_CC_last_7_8_day, MAX_CDT_AMT_CC_last_9_11_day, MAX_CDT_AMT_CC_last_12_14_day,
            MAX_CDT_AMT_CC_last_1_week, MAX_CDT_AMT_CC_last_2_week, MAX_CDT_AMT_CC_last_3_week, MAX_CDT_AMT_CC_last_4_week,

            TOTAL_DEBIT_AMOUNT_CC_last_1_2_day, TOTAL_DEBIT_AMOUNT_CC_last_3_4_day, TOTAL_DEBIT_AMOUNT_CC_last_5_6_day, TOTAL_DEBIT_AMOUNT_CC_last_7_8_day, TOTAL_DEBIT_AMOUNT_CC_last_9_11_day, TOTAL_DEBIT_AMOUNT_CC_last_12_14_day,
            TOTAL_DEBIT_AMOUNT_CC_last_1_week, TOTAL_DEBIT_AMOUNT_CC_last_2_week, TOTAL_DEBIT_AMOUNT_CC_last_3_week, TOTAL_DEBIT_AMOUNT_CC_last_4_week,

            MIN_DEBIT_AMOUNT_CC_last_1_2_day, MIN_DEBIT_AMOUNT_CC_last_3_4_day, MIN_DEBIT_AMOUNT_CC_last_5_6_day, MIN_DEBIT_AMOUNT_CC_last_7_8_day, MIN_DEBIT_AMOUNT_CC_last_9_11_day, MIN_DEBIT_AMOUNT_CC_last_12_14_day,
            MIN_DEBIT_AMOUNT_CC_last_1_week, MIN_DEBIT_AMOUNT_CC_last_2_week, MIN_DEBIT_AMOUNT_CC_last_3_week, MIN_DEBIT_AMOUNT_CC_last_4_week,

            MAX_DEBIT_AMOUNT_CC_last_1_2_day, MAX_DEBIT_AMOUNT_CC_last_3_4_day, MAX_DEBIT_AMOUNT_CC_last_5_6_day, MAX_DEBIT_AMOUNT_CC_last_7_8_day, MAX_DEBIT_AMOUNT_CC_last_9_11_day, MAX_DEBIT_AMOUNT_CC_last_12_14_day,
            MAX_DEBIT_AMOUNT_CC_last_1_week, MAX_DEBIT_AMOUNT_CC_last_2_week, MAX_DEBIT_AMOUNT_CC_last_3_week, MAX_DEBIT_AMOUNT_CC_last_4_week,

            MAX_AVAILABLE_BALANCE_last_1_2_day, MAX_AVAILABLE_BALANCE_last_3_4_day, MAX_AVAILABLE_BALANCE_last_5_6_day, MAX_AVAILABLE_BALANCE_last_7_8_day, MAX_AVAILABLE_BALANCE_last_9_11_day, MAX_AVAILABLE_BALANCE_last_12_14_day,
            MAX_AVAILABLE_BALANCE_last_1_week, MAX_AVAILABLE_BALANCE_last_2_week, MAX_AVAILABLE_BALANCE_last_3_week, MAX_AVAILABLE_BALANCE_last_4_week,

            MIN_AVAILABLE_BALANCE_last_1_2_day, MIN_AVAILABLE_BALANCE_last_3_4_day, MIN_AVAILABLE_BALANCE_last_5_6_day, MIN_AVAILABLE_BALANCE_last_7_8_day, MIN_AVAILABLE_BALANCE_last_9_11_day, MIN_AVAILABLE_BALANCE_last_12_14_day,
            MIN_AVAILABLE_BALANCE_last_1_week, MIN_AVAILABLE_BALANCE_last_2_week, MIN_AVAILABLE_BALANCE_last_3_week, MIN_AVAILABLE_BALANCE_last_4_week,

            TOTAL_AVAILABLE_BALANCE_last_1_2_day, TOTAL_AVAILABLE_BALANCE_last_3_4_day, TOTAL_AVAILABLE_BALANCE_last_5_6_day, TOTAL_AVAILABLE_BALANCE_last_7_8_day, TOTAL_AVAILABLE_BALANCE_last_9_11_day, TOTAL_AVAILABLE_BALANCE_last_12_14_day,
            TOTAL_AVAILABLE_BALANCE_last_1_week, TOTAL_AVAILABLE_BALANCE_last_2_week, TOTAL_AVAILABLE_BALANCE_last_3_week, TOTAL_AVAILABLE_BALANCE_last_4_week,

            NUM_AVAILABLE_BALANCE_last_1_2_day, NUM_AVAILABLE_BALANCE_last_3_4_day, NUM_AVAILABLE_BALANCE_last_5_6_day, NUM_AVAILABLE_BALANCE_last_7_8_day, NUM_AVAILABLE_BALANCE_last_9_11_day, NUM_AVAILABLE_BALANCE_last_12_14_day,
            NUM_AVAILABLE_BALANCE_last_1_week, NUM_AVAILABLE_BALANCE_last_2_week, NUM_AVAILABLE_BALANCE_last_3_week, NUM_AVAILABLE_BALANCE_last_4_week,

            NUM_CREDIT_TXNS_LT_100_last_1_2_day, NUM_CREDIT_TXNS_LT_100_last_3_4_day, NUM_CREDIT_TXNS_LT_100_last_5_6_day, NUM_CREDIT_TXNS_LT_100_last_7_8_day, NUM_CREDIT_TXNS_LT_100_last_9_11_day, NUM_CREDIT_TXNS_LT_100_last_12_14_day,
            NUM_CREDIT_TXNS_LT_100_last_1_week, NUM_CREDIT_TXNS_LT_100_last_2_week, NUM_CREDIT_TXNS_LT_100_last_3_week, NUM_CREDIT_TXNS_LT_100_last_4_week,

            NUM_DEBIT_TXNS_LT_100_last_1_2_day, NUM_DEBIT_TXNS_LT_100_last_3_4_day, NUM_DEBIT_TXNS_LT_100_last_5_6_day, NUM_DEBIT_TXNS_LT_100_last_7_8_day, NUM_DEBIT_TXNS_LT_100_last_9_11_day, NUM_DEBIT_TXNS_LT_100_last_12_14_day,
            NUM_DEBIT_TXNS_LT_100_last_1_week, NUM_DEBIT_TXNS_LT_100_last_2_week, NUM_DEBIT_TXNS_LT_100_last_3_week, NUM_DEBIT_TXNS_LT_100_last_4_week,

            NUM_CREDIT_TXNS_100_500_last_1_2_day, NUM_CREDIT_TXNS_100_500_last_3_4_day, NUM_CREDIT_TXNS_100_500_last_5_6_day, NUM_CREDIT_TXNS_100_500_last_7_8_day, NUM_CREDIT_TXNS_100_500_last_9_11_day, NUM_CREDIT_TXNS_100_500_last_12_14_day,
            NUM_CREDIT_TXNS_100_500_last_1_week, NUM_CREDIT_TXNS_100_500_last_2_week, NUM_CREDIT_TXNS_100_500_last_3_week, NUM_CREDIT_TXNS_100_500_last_4_week,

            NUM_DEBIT_TXNS_100_500_last_1_2_day, NUM_DEBIT_TXNS_100_500_last_3_4_day, NUM_DEBIT_TXNS_100_500_last_5_6_day, NUM_DEBIT_TXNS_100_500_last_7_8_day, NUM_DEBIT_TXNS_100_500_last_9_11_day, NUM_DEBIT_TXNS_100_500_last_12_14_day,
            NUM_DEBIT_TXNS_100_500_last_1_week, NUM_DEBIT_TXNS_100_500_last_2_week, NUM_DEBIT_TXNS_100_500_last_3_week, NUM_DEBIT_TXNS_100_500_last_4_week,

            NUM_CREDIT_TXNS_500_2000_last_1_2_day, NUM_CREDIT_TXNS_500_2000_last_3_4_day, NUM_CREDIT_TXNS_500_2000_last_5_6_day, NUM_CREDIT_TXNS_500_2000_last_7_8_day, NUM_CREDIT_TXNS_500_2000_last_9_11_day, NUM_CREDIT_TXNS_500_2000_last_12_14_day,
            NUM_CREDIT_TXNS_500_2000_last_1_week, NUM_CREDIT_TXNS_500_2000_last_2_week, NUM_CREDIT_TXNS_500_2000_last_3_week, NUM_CREDIT_TXNS_500_2000_last_4_week,

            NUM_DEBIT_TXNS_500_2000_last_1_2_day, NUM_DEBIT_TXNS_500_2000_last_3_4_day, NUM_DEBIT_TXNS_500_2000_last_5_6_day, NUM_DEBIT_TXNS_500_2000_last_7_8_day, NUM_DEBIT_TXNS_500_2000_last_9_11_day, NUM_DEBIT_TXNS_500_2000_last_12_14_day,
            NUM_DEBIT_TXNS_500_2000_last_1_week, NUM_DEBIT_TXNS_500_2000_last_2_week, NUM_DEBIT_TXNS_500_2000_last_3_week, NUM_DEBIT_TXNS_500_2000_last_4_week,

            NUM_CREDIT_TXNS_2000_5000_last_1_2_day, NUM_CREDIT_TXNS_2000_5000_last_3_4_day, NUM_CREDIT_TXNS_2000_5000_last_5_6_day, NUM_CREDIT_TXNS_2000_5000_last_7_8_day, NUM_CREDIT_TXNS_2000_5000_last_9_11_day, NUM_CREDIT_TXNS_2000_5000_last_12_14_day,
            NUM_CREDIT_TXNS_2000_5000_last_1_week, NUM_CREDIT_TXNS_2000_5000_last_2_week, NUM_CREDIT_TXNS_2000_5000_last_3_week, NUM_CREDIT_TXNS_2000_5000_last_4_week,

            NUM_DEBIT_TXNS_2000_5000_last_1_2_day, NUM_DEBIT_TXNS_2000_5000_last_3_4_day, NUM_DEBIT_TXNS_2000_5000_last_5_6_day, NUM_DEBIT_TXNS_2000_5000_last_7_8_day, NUM_DEBIT_TXNS_2000_5000_last_9_11_day, NUM_DEBIT_TXNS_2000_5000_last_12_14_day,
            NUM_DEBIT_TXNS_2000_5000_last_1_week, NUM_DEBIT_TXNS_2000_5000_last_2_week, NUM_DEBIT_TXNS_2000_5000_last_3_week, NUM_DEBIT_TXNS_2000_5000_last_4_week,

            NUM_CREDIT_TXNS_5000_10000_last_1_2_day, NUM_CREDIT_TXNS_5000_10000_last_3_4_day, NUM_CREDIT_TXNS_5000_10000_last_5_6_day, NUM_CREDIT_TXNS_5000_10000_last_7_8_day, NUM_CREDIT_TXNS_5000_10000_last_9_11_day, NUM_CREDIT_TXNS_5000_10000_last_12_14_day,
            NUM_CREDIT_TXNS_5000_10000_last_1_week, NUM_CREDIT_TXNS_5000_10000_last_2_week, NUM_CREDIT_TXNS_5000_10000_last_3_week, NUM_CREDIT_TXNS_5000_10000_last_4_week,

            NUM_DEBIT_TXNS_5000_10000_last_1_2_day, NUM_DEBIT_TXNS_5000_10000_last_3_4_day, NUM_DEBIT_TXNS_5000_10000_last_5_6_day, NUM_DEBIT_TXNS_5000_10000_last_7_8_day, NUM_DEBIT_TXNS_5000_10000_last_9_11_day, NUM_DEBIT_TXNS_5000_10000_last_12_14_day,
            NUM_DEBIT_TXNS_5000_10000_last_1_week, NUM_DEBIT_TXNS_5000_10000_last_2_week, NUM_DEBIT_TXNS_5000_10000_last_3_week, NUM_DEBIT_TXNS_5000_10000_last_4_week,

            NUM_CREDIT_TXNS_GT_10000_last_1_2_day, NUM_CREDIT_TXNS_GT_10000_last_3_4_day, NUM_CREDIT_TXNS_GT_10000_last_5_6_day, NUM_CREDIT_TXNS_GT_10000_last_7_8_day, NUM_CREDIT_TXNS_GT_10000_last_9_11_day, NUM_CREDIT_TXNS_GT_10000_last_12_14_day,
            NUM_CREDIT_TXNS_GT_10000_last_1_week, NUM_CREDIT_TXNS_GT_10000_last_2_week, NUM_CREDIT_TXNS_GT_10000_last_3_week, NUM_CREDIT_TXNS_GT_10000_last_4_week,

            NUM_DEBIT_TXNS_GT_10000_last_1_2_day, NUM_DEBIT_TXNS_GT_10000_last_3_4_day, NUM_DEBIT_TXNS_GT_10000_last_5_6_day, NUM_DEBIT_TXNS_GT_10000_last_7_8_day, NUM_DEBIT_TXNS_GT_10000_last_9_11_day, NUM_DEBIT_TXNS_GT_10000_last_12_14_day,
            NUM_DEBIT_TXNS_GT_10000_last_1_week, NUM_DEBIT_TXNS_GT_10000_last_2_week, NUM_DEBIT_TXNS_GT_10000_last_3_week, NUM_DEBIT_TXNS_GT_10000_last_4_week,

            NUM_CREDIT_TXNS_last_1_2_day, NUM_CREDIT_TXNS_last_3_4_day, NUM_CREDIT_TXNS_last_5_6_day, NUM_CREDIT_TXNS_last_7_8_day, NUM_CREDIT_TXNS_last_9_11_day, NUM_CREDIT_TXNS_last_12_14_day,
            NUM_CREDIT_TXNS_last_1_week, NUM_CREDIT_TXNS_last_2_week, NUM_CREDIT_TXNS_last_3_week, NUM_CREDIT_TXNS_last_4_week,

            NUM_DEBIT_TXNS_last_1_2_day, NUM_DEBIT_TXNS_last_3_4_day, NUM_DEBIT_TXNS_last_5_6_day, NUM_DEBIT_TXNS_last_7_8_day, NUM_DEBIT_TXNS_last_9_11_day, NUM_DEBIT_TXNS_last_12_14_day,
            NUM_DEBIT_TXNS_last_1_week, NUM_DEBIT_TXNS_last_2_week, NUM_DEBIT_TXNS_last_3_week, NUM_DEBIT_TXNS_last_4_week,

            TOTAL_CREDIT_AMOUNT_last_1_2_day, TOTAL_CREDIT_AMOUNT_last_3_4_day, TOTAL_CREDIT_AMOUNT_last_5_6_day, TOTAL_CREDIT_AMOUNT_last_7_8_day, TOTAL_CREDIT_AMOUNT_last_9_11_day, TOTAL_CREDIT_AMOUNT_last_12_14_day,
            TOTAL_CREDIT_AMOUNT_last_1_week, TOTAL_CREDIT_AMOUNT_last_2_week, TOTAL_CREDIT_AMOUNT_last_3_week, TOTAL_CREDIT_AMOUNT_last_4_week,

            MIN_CDT_AMT_last_1_2_day, MIN_CDT_AMT_last_3_4_day, MIN_CDT_AMT_last_5_6_day, MIN_CDT_AMT_last_7_8_day, MIN_CDT_AMT_last_9_11_day, MIN_CDT_AMT_last_12_14_day,
            MIN_CDT_AMT_last_1_week, MIN_CDT_AMT_last_2_week, MIN_CDT_AMT_last_3_week, MIN_CDT_AMT_last_4_week,

            MAX_CDT_AMT_last_1_2_day, MAX_CDT_AMT_last_3_4_day, MAX_CDT_AMT_last_5_6_day, MAX_CDT_AMT_last_7_8_day, MAX_CDT_AMT_last_9_11_day, MAX_CDT_AMT_last_12_14_day,
            MAX_CDT_AMT_last_1_week, MAX_CDT_AMT_last_2_week, MAX_CDT_AMT_last_3_week, MAX_CDT_AMT_last_4_week,

            TOTAL_DEBIT_AMOUNT_last_1_2_day, TOTAL_DEBIT_AMOUNT_last_3_4_day, TOTAL_DEBIT_AMOUNT_last_5_6_day, TOTAL_DEBIT_AMOUNT_last_7_8_day, TOTAL_DEBIT_AMOUNT_last_9_11_day, TOTAL_DEBIT_AMOUNT_last_12_14_day,
            TOTAL_DEBIT_AMOUNT_last_1_week, TOTAL_DEBIT_AMOUNT_last_2_week, TOTAL_DEBIT_AMOUNT_last_3_week, TOTAL_DEBIT_AMOUNT_last_4_week,

            MIN_DEBIT_AMOUNT_last_1_2_day, MIN_DEBIT_AMOUNT_last_3_4_day, MIN_DEBIT_AMOUNT_last_5_6_day, MIN_DEBIT_AMOUNT_last_7_8_day, MIN_DEBIT_AMOUNT_last_9_11_day, MIN_DEBIT_AMOUNT_last_12_14_day,
            MIN_DEBIT_AMOUNT_last_1_week, MIN_DEBIT_AMOUNT_last_2_week, MIN_DEBIT_AMOUNT_last_3_week, MIN_DEBIT_AMOUNT_last_4_week,

            MAX_DEBIT_AMOUNT_last_1_2_day, MAX_DEBIT_AMOUNT_last_3_4_day, MAX_DEBIT_AMOUNT_last_5_6_day, MAX_DEBIT_AMOUNT_last_7_8_day, MAX_DEBIT_AMOUNT_last_9_11_day, MAX_DEBIT_AMOUNT_last_12_14_day,
            MAX_DEBIT_AMOUNT_last_1_week, MAX_DEBIT_AMOUNT_last_2_week, MAX_DEBIT_AMOUNT_last_3_week, MAX_DEBIT_AMOUNT_last_4_week,

            TOTAL_DEBIT_COUNT_UPI_last_1_2_day, TOTAL_DEBIT_COUNT_UPI_last_3_4_day, TOTAL_DEBIT_COUNT_UPI_last_5_6_day, TOTAL_DEBIT_COUNT_UPI_last_7_8_day, TOTAL_DEBIT_COUNT_UPI_last_9_11_day, TOTAL_DEBIT_COUNT_UPI_last_12_14_day,
            TOTAL_DEBIT_COUNT_UPI_last_1_week, TOTAL_DEBIT_COUNT_UPI_last_2_week, TOTAL_DEBIT_COUNT_UPI_last_3_week, TOTAL_DEBIT_COUNT_UPI_last_4_week,

            TOTAL_DEBIT_COUNT_ATM_last_1_2_day, TOTAL_DEBIT_COUNT_ATM_last_3_4_day, TOTAL_DEBIT_COUNT_ATM_last_5_6_day, TOTAL_DEBIT_COUNT_ATM_last_7_8_day, TOTAL_DEBIT_COUNT_ATM_last_9_11_day, TOTAL_DEBIT_COUNT_ATM_last_12_14_day,
            TOTAL_DEBIT_COUNT_ATM_last_1_week, TOTAL_DEBIT_COUNT_ATM_last_2_week, TOTAL_DEBIT_COUNT_ATM_last_3_week, TOTAL_DEBIT_COUNT_ATM_last_4_week,

            TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_1_2_day, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_3_4_day, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_5_6_day, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_7_8_day, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_9_11_day, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_12_14_day,
            TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_1_week, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_2_week, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_3_week, TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_4_week,

            TOTAL_DEBIT_COUNT_CHEQUE_last_1_2_day, TOTAL_DEBIT_COUNT_CHEQUE_last_3_4_day, TOTAL_DEBIT_COUNT_CHEQUE_last_5_6_day, TOTAL_DEBIT_COUNT_CHEQUE_last_7_8_day, TOTAL_DEBIT_COUNT_CHEQUE_last_9_11_day, TOTAL_DEBIT_COUNT_CHEQUE_last_12_14_day,
            TOTAL_DEBIT_COUNT_CHEQUE_last_1_week, TOTAL_DEBIT_COUNT_CHEQUE_last_2_week, TOTAL_DEBIT_COUNT_CHEQUE_last_3_week, TOTAL_DEBIT_COUNT_CHEQUE_last_4_week,

            TOTAL_DEBIT_COUNT_CASH_last_1_2_day, TOTAL_DEBIT_COUNT_CASH_last_3_4_day, TOTAL_DEBIT_COUNT_CASH_last_5_6_day, TOTAL_DEBIT_COUNT_CASH_last_7_8_day, TOTAL_DEBIT_COUNT_CASH_last_9_11_day, TOTAL_DEBIT_COUNT_CASH_last_12_14_day,
            TOTAL_DEBIT_COUNT_CASH_last_1_week, TOTAL_DEBIT_COUNT_CASH_last_2_week, TOTAL_DEBIT_COUNT_CASH_last_3_week, TOTAL_DEBIT_COUNT_CASH_last_4_week,

            TOTAL_CREDIT_COUNT_UPI_last_1_2_day, TOTAL_CREDIT_COUNT_UPI_last_3_4_day, TOTAL_CREDIT_COUNT_UPI_last_5_6_day, TOTAL_CREDIT_COUNT_UPI_last_7_8_day, TOTAL_CREDIT_COUNT_UPI_last_9_11_day, TOTAL_CREDIT_COUNT_UPI_last_12_14_day,
            TOTAL_CREDIT_COUNT_UPI_last_1_week, TOTAL_CREDIT_COUNT_UPI_last_2_week, TOTAL_CREDIT_COUNT_UPI_last_3_week, TOTAL_CREDIT_COUNT_UPI_last_4_week,

            TOTAL_CREDIT_COUNT_ATM_last_1_2_day, TOTAL_CREDIT_COUNT_ATM_last_3_4_day, TOTAL_CREDIT_COUNT_ATM_last_5_6_day, TOTAL_CREDIT_COUNT_ATM_last_7_8_day, TOTAL_CREDIT_COUNT_ATM_last_9_11_day, TOTAL_CREDIT_COUNT_ATM_last_12_14_day,
            TOTAL_CREDIT_COUNT_ATM_last_1_week, TOTAL_CREDIT_COUNT_ATM_last_2_week, TOTAL_CREDIT_COUNT_ATM_last_3_week, TOTAL_CREDIT_COUNT_ATM_last_4_week,

            TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_1_2_day, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_3_4_day, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_5_6_day, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_7_8_day, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_9_11_day, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_12_14_day,
            TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_1_week, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_2_week, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_3_week, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_4_week,

            TOTAL_CREDIT_COUNT_CHEQUE_last_1_2_day, TOTAL_CREDIT_COUNT_CHEQUE_last_3_4_day, TOTAL_CREDIT_COUNT_CHEQUE_last_5_6_day, TOTAL_CREDIT_COUNT_CHEQUE_last_7_8_day, TOTAL_CREDIT_COUNT_CHEQUE_last_9_11_day, TOTAL_CREDIT_COUNT_CHEQUE_last_12_14_day,
            TOTAL_CREDIT_COUNT_CHEQUE_last_1_week, TOTAL_CREDIT_COUNT_CHEQUE_last_2_week, TOTAL_CREDIT_COUNT_CHEQUE_last_3_week, TOTAL_CREDIT_COUNT_CHEQUE_last_4_week,

            TOTAL_CREDIT_COUNT_CASH_last_1_2_day, TOTAL_CREDIT_COUNT_CASH_last_3_4_day, TOTAL_CREDIT_COUNT_CASH_last_5_6_day, TOTAL_CREDIT_COUNT_CASH_last_7_8_day, TOTAL_CREDIT_COUNT_CASH_last_9_11_day, TOTAL_CREDIT_COUNT_CASH_last_12_14_day,
            TOTAL_CREDIT_COUNT_CASH_last_1_week, TOTAL_CREDIT_COUNT_CASH_last_2_week, TOTAL_CREDIT_COUNT_CASH_last_3_week, TOTAL_CREDIT_COUNT_CASH_last_4_week,

            TOTAL_CREDIT_AMOUNT_UPI_last_1_2_day, TOTAL_CREDIT_AMOUNT_UPI_last_3_4_day, TOTAL_CREDIT_AMOUNT_UPI_last_5_6_day, TOTAL_CREDIT_AMOUNT_UPI_last_7_8_day, TOTAL_CREDIT_AMOUNT_UPI_last_9_11_day, TOTAL_CREDIT_AMOUNT_UPI_last_12_14_day,
            TOTAL_CREDIT_AMOUNT_UPI_last_1_week, TOTAL_CREDIT_AMOUNT_UPI_last_2_week, TOTAL_CREDIT_AMOUNT_UPI_last_3_week, TOTAL_CREDIT_AMOUNT_UPI_last_4_week,

            TOTAL_CREDIT_AMOUNT_ATM_last_1_2_day, TOTAL_CREDIT_AMOUNT_ATM_last_3_4_day, TOTAL_CREDIT_AMOUNT_ATM_last_5_6_day, TOTAL_CREDIT_AMOUNT_ATM_last_7_8_day, TOTAL_CREDIT_AMOUNT_ATM_last_9_11_day, TOTAL_CREDIT_AMOUNT_ATM_last_12_14_day,
            TOTAL_CREDIT_AMOUNT_ATM_last_1_week, TOTAL_CREDIT_AMOUNT_ATM_last_2_week, TOTAL_CREDIT_AMOUNT_ATM_last_3_week, TOTAL_CREDIT_AMOUNT_ATM_last_4_week,

            TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_1_2_day, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_3_4_day, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_5_6_day, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_7_8_day, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_9_11_day, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_12_14_day,
            TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_1_week, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_2_week, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_3_week, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_4_week,

            TOTAL_CREDIT_AMOUNT_CHEQUE_last_1_2_day, TOTAL_CREDIT_AMOUNT_CHEQUE_last_3_4_day, TOTAL_CREDIT_AMOUNT_CHEQUE_last_5_6_day, TOTAL_CREDIT_AMOUNT_CHEQUE_last_7_8_day, TOTAL_CREDIT_AMOUNT_CHEQUE_last_9_11_day, TOTAL_CREDIT_AMOUNT_CHEQUE_last_12_14_day,
            TOTAL_CREDIT_AMOUNT_CHEQUE_last_1_week, TOTAL_CREDIT_AMOUNT_CHEQUE_last_2_week, TOTAL_CREDIT_AMOUNT_CHEQUE_last_3_week, TOTAL_CREDIT_AMOUNT_CHEQUE_last_4_week,

            TOTAL_CREDIT_AMOUNT_CASH_last_1_2_day, TOTAL_CREDIT_AMOUNT_CASH_last_3_4_day, TOTAL_CREDIT_AMOUNT_CASH_last_5_6_day, TOTAL_CREDIT_AMOUNT_CASH_last_7_8_day, TOTAL_CREDIT_AMOUNT_CASH_last_9_11_day, TOTAL_CREDIT_AMOUNT_CASH_last_12_14_day,
            TOTAL_CREDIT_AMOUNT_CASH_last_1_week, TOTAL_CREDIT_AMOUNT_CASH_last_2_week, TOTAL_CREDIT_AMOUNT_CASH_last_3_week, TOTAL_CREDIT_AMOUNT_CASH_last_4_week,

            TOTAL_DEBIT_AMOUNT_UPI_last_1_2_day, TOTAL_DEBIT_AMOUNT_UPI_last_3_4_day, TOTAL_DEBIT_AMOUNT_UPI_last_5_6_day, TOTAL_DEBIT_AMOUNT_UPI_last_7_8_day, TOTAL_DEBIT_AMOUNT_UPI_last_9_11_day, TOTAL_DEBIT_AMOUNT_UPI_last_12_14_day,
            TOTAL_DEBIT_AMOUNT_UPI_last_1_week, TOTAL_DEBIT_AMOUNT_UPI_last_2_week, TOTAL_DEBIT_AMOUNT_UPI_last_3_week, TOTAL_DEBIT_AMOUNT_UPI_last_4_week,

            TOTAL_DEBIT_AMOUNT_ATM_last_1_2_day, TOTAL_DEBIT_AMOUNT_ATM_last_3_4_day, TOTAL_DEBIT_AMOUNT_ATM_last_5_6_day, TOTAL_DEBIT_AMOUNT_ATM_last_7_8_day, TOTAL_DEBIT_AMOUNT_ATM_last_9_11_day, TOTAL_DEBIT_AMOUNT_ATM_last_12_14_day,
            TOTAL_DEBIT_AMOUNT_ATM_last_1_week, TOTAL_DEBIT_AMOUNT_ATM_last_2_week, TOTAL_DEBIT_AMOUNT_ATM_last_3_week, TOTAL_DEBIT_AMOUNT_ATM_last_4_week,

            TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_1_2_day, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_3_4_day, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_5_6_day, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_7_8_day, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_9_11_day, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_12_14_day,
            TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_1_week, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_2_week, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_3_week, TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_4_week,

            TOTAL_DEBIT_AMOUNT_CHEQUE_last_1_2_day, TOTAL_DEBIT_AMOUNT_CHEQUE_last_3_4_day, TOTAL_DEBIT_AMOUNT_CHEQUE_last_5_6_day, TOTAL_DEBIT_AMOUNT_CHEQUE_last_7_8_day, TOTAL_DEBIT_AMOUNT_CHEQUE_last_9_11_day, TOTAL_DEBIT_AMOUNT_CHEQUE_last_12_14_day,
            TOTAL_DEBIT_AMOUNT_CHEQUE_last_1_week, TOTAL_DEBIT_AMOUNT_CHEQUE_last_2_week, TOTAL_DEBIT_AMOUNT_CHEQUE_last_3_week, TOTAL_DEBIT_AMOUNT_CHEQUE_last_4_week,

            TOTAL_DEBIT_AMOUNT_CASH_last_1_2_day, TOTAL_DEBIT_AMOUNT_CASH_last_3_4_day, TOTAL_DEBIT_AMOUNT_CASH_last_5_6_day, TOTAL_DEBIT_AMOUNT_CASH_last_7_8_day, TOTAL_DEBIT_AMOUNT_CASH_last_9_11_day, TOTAL_DEBIT_AMOUNT_CASH_last_12_14_day,
            TOTAL_DEBIT_AMOUNT_CASH_last_1_week, TOTAL_DEBIT_AMOUNT_CASH_last_2_week, TOTAL_DEBIT_AMOUNT_CASH_last_3_week, TOTAL_DEBIT_AMOUNT_CASH_last_4_week,

            -- ============================================
            -- DERIVED: AVERAGE FEATURES (per day)
            -- ============================================
            -- Bank Account Averages
            DIV0(TOTAL_CREDIT_AMOUNT_last_1_week, 7) AS AVG_CDT_AMT_last_1_week,
            DIV0(TOTAL_CREDIT_AMOUNT_last_2_week, 7) AS AVG_CDT_AMT_last_2_week,
            DIV0(TOTAL_CREDIT_AMOUNT_last_3_week, 7) AS AVG_CDT_AMT_last_3_week,
            DIV0(TOTAL_CREDIT_AMOUNT_last_4_week, 9) AS AVG_CDT_AMT_last_4_week,

            DIV0(TOTAL_DEBIT_AMOUNT_last_1_week, 7) AS AVG_DEBIT_AMOUNT_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_last_2_week, 7) AS AVG_DEBIT_AMOUNT_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_last_3_week, 7) AS AVG_DEBIT_AMOUNT_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_last_4_week, 9) AS AVG_DEBIT_AMOUNT_last_4_week,

            DIV0(NUM_CREDIT_TXNS_last_1_week, 7) AS AVG_CREDIT_TXNS_last_1_week,
            DIV0(NUM_CREDIT_TXNS_last_2_week, 7) AS AVG_CREDIT_TXNS_last_2_week,
            DIV0(NUM_CREDIT_TXNS_last_3_week, 7) AS AVG_CREDIT_TXNS_last_3_week,
            DIV0(NUM_CREDIT_TXNS_last_4_week, 9) AS AVG_CREDIT_TXNS_last_4_week,

            DIV0(NUM_DEBIT_TXNS_last_1_week, 7) AS AVG_DEBIT_TXNS_last_1_week,
            DIV0(NUM_DEBIT_TXNS_last_2_week, 7) AS AVG_DEBIT_TXNS_last_2_week,
            DIV0(NUM_DEBIT_TXNS_last_3_week, 7) AS AVG_DEBIT_TXNS_last_3_week,
            DIV0(NUM_DEBIT_TXNS_last_4_week, 9) AS AVG_DEBIT_TXNS_last_4_week,

            DIV0(TOTAL_AVAILABLE_BALANCE_last_1_week, NUM_AVAILABLE_BALANCE_last_1_week) AS AVG_AVAILABLE_BALANCE_last_1_week,
            DIV0(TOTAL_AVAILABLE_BALANCE_last_2_week, NUM_AVAILABLE_BALANCE_last_2_week) AS AVG_AVAILABLE_BALANCE_last_2_week,
            DIV0(TOTAL_AVAILABLE_BALANCE_last_3_week, NUM_AVAILABLE_BALANCE_last_3_week) AS AVG_AVAILABLE_BALANCE_last_3_week,
            DIV0(TOTAL_AVAILABLE_BALANCE_last_4_week, NUM_AVAILABLE_BALANCE_last_4_week) AS AVG_AVAILABLE_BALANCE_last_4_week,

            -- Credit Card Averages
            DIV0(TOTAL_CREDIT_AMOUNT_CC_last_1_week, 7) AS AVG_CDT_AMT_CC_last_1_week,
            DIV0(TOTAL_CREDIT_AMOUNT_CC_last_2_week, 7) AS AVG_CDT_AMT_CC_last_2_week,
            DIV0(TOTAL_CREDIT_AMOUNT_CC_last_3_week, 7) AS AVG_CDT_AMT_CC_last_3_week,
            DIV0(TOTAL_CREDIT_AMOUNT_CC_last_4_week, 9) AS AVG_CDT_AMT_CC_last_4_week,

            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_1_week, 7) AS AVG_DEBIT_AMOUNT_CC_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_2_week, 7) AS AVG_DEBIT_AMOUNT_CC_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_3_week, 7) AS AVG_DEBIT_AMOUNT_CC_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_4_week, 9) AS AVG_DEBIT_AMOUNT_CC_last_4_week,

            DIV0(NUM_CREDIT_TXNS_CC_last_1_week, 7) AS AVG_CREDIT_TXNS_CC_last_1_week,
            DIV0(NUM_CREDIT_TXNS_CC_last_2_week, 7) AS AVG_CREDIT_TXNS_CC_last_2_week,
            DIV0(NUM_CREDIT_TXNS_CC_last_3_week, 7) AS AVG_CREDIT_TXNS_CC_last_3_week,
            DIV0(NUM_CREDIT_TXNS_CC_last_4_week, 9) AS AVG_CREDIT_TXNS_CC_last_4_week,

            DIV0(NUM_DEBIT_TXNS_CC_last_1_week, 7) AS AVG_DEBIT_TXNS_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_CC_last_2_week, 7) AS AVG_DEBIT_TXNS_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_CC_last_3_week, 7) AS AVG_DEBIT_TXNS_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_CC_last_4_week, 9) AS AVG_DEBIT_TXNS_CC_last_4_week,

            -- ============================================
            -- DERIVED: RATIO FEATURES - DEBIT/CREDIT RATIOS (10 windows)
            -- ============================================
            DIV0(TOTAL_DEBIT_AMOUNT_last_1_2_day, TOTAL_CREDIT_AMOUNT_last_1_2_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_1_2_day,
            DIV0(TOTAL_DEBIT_AMOUNT_last_3_4_day, TOTAL_CREDIT_AMOUNT_last_3_4_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_3_4_day,
            DIV0(TOTAL_DEBIT_AMOUNT_last_5_6_day, TOTAL_CREDIT_AMOUNT_last_5_6_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_5_6_day,
            DIV0(TOTAL_DEBIT_AMOUNT_last_7_8_day, TOTAL_CREDIT_AMOUNT_last_7_8_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_7_8_day,
            DIV0(TOTAL_DEBIT_AMOUNT_last_9_11_day, TOTAL_CREDIT_AMOUNT_last_9_11_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_9_11_day,
            DIV0(TOTAL_DEBIT_AMOUNT_last_12_14_day, TOTAL_CREDIT_AMOUNT_last_12_14_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_12_14_day,
            DIV0(TOTAL_DEBIT_AMOUNT_last_1_week, TOTAL_CREDIT_AMOUNT_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_last_2_week, TOTAL_CREDIT_AMOUNT_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_last_3_week, TOTAL_CREDIT_AMOUNT_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_last_4_week, TOTAL_CREDIT_AMOUNT_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_last_4_week,

            -- Transaction Count Ratios (10 windows)
            DIV0(NUM_DEBIT_TXNS_last_1_2_day, NUM_CREDIT_TXNS_last_1_2_day) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_1_2_day,
            DIV0(NUM_DEBIT_TXNS_last_3_4_day, NUM_CREDIT_TXNS_last_3_4_day) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_3_4_day,
            DIV0(NUM_DEBIT_TXNS_last_5_6_day, NUM_CREDIT_TXNS_last_5_6_day) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_5_6_day,
            DIV0(NUM_DEBIT_TXNS_last_7_8_day, NUM_CREDIT_TXNS_last_7_8_day) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_7_8_day,
            DIV0(NUM_DEBIT_TXNS_last_9_11_day, NUM_CREDIT_TXNS_last_9_11_day) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_9_11_day,
            DIV0(NUM_DEBIT_TXNS_last_12_14_day, NUM_CREDIT_TXNS_last_12_14_day) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_12_14_day,
            DIV0(NUM_DEBIT_TXNS_last_1_week, NUM_CREDIT_TXNS_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_1_week,
            DIV0(NUM_DEBIT_TXNS_last_2_week, NUM_CREDIT_TXNS_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_2_week,
            DIV0(NUM_DEBIT_TXNS_last_3_week, NUM_CREDIT_TXNS_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_3_week,
            DIV0(NUM_DEBIT_TXNS_last_4_week, NUM_CREDIT_TXNS_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_last_4_week,

            -- Credit Card Debit/Credit Ratios (10 windows)
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_1_2_day, TOTAL_CREDIT_AMOUNT_CC_last_1_2_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_1_2_day,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_3_4_day, TOTAL_CREDIT_AMOUNT_CC_last_3_4_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_3_4_day,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_5_6_day, TOTAL_CREDIT_AMOUNT_CC_last_5_6_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_5_6_day,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_7_8_day, TOTAL_CREDIT_AMOUNT_CC_last_7_8_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_7_8_day,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_9_11_day, TOTAL_CREDIT_AMOUNT_CC_last_9_11_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_9_11_day,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_12_14_day, TOTAL_CREDIT_AMOUNT_CC_last_12_14_day) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_12_14_day,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_1_week, TOTAL_CREDIT_AMOUNT_CC_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_2_week, TOTAL_CREDIT_AMOUNT_CC_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_3_week, TOTAL_CREDIT_AMOUNT_CC_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_4_week, TOTAL_CREDIT_AMOUNT_CC_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CC_last_4_week,

            DIV0(NUM_DEBIT_TXNS_CC_last_1_week, NUM_CREDIT_TXNS_CC_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_CC_last_2_week, NUM_CREDIT_TXNS_CC_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_CC_last_3_week, NUM_CREDIT_TXNS_CC_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_CC_last_4_week, NUM_CREDIT_TXNS_CC_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_CC_last_4_week,

            -- ============================================
            -- DERIVED: UPI DEBIT/CREDIT RATIOS (4 weekly windows)
            -- ============================================
            DIV0(TOTAL_DEBIT_AMOUNT_UPI_last_1_week, TOTAL_CREDIT_AMOUNT_UPI_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_UPI_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_UPI_last_2_week, TOTAL_CREDIT_AMOUNT_UPI_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_UPI_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_UPI_last_3_week, TOTAL_CREDIT_AMOUNT_UPI_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_UPI_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_UPI_last_4_week, TOTAL_CREDIT_AMOUNT_UPI_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_UPI_last_4_week,

            DIV0(TOTAL_DEBIT_COUNT_UPI_last_1_week, TOTAL_CREDIT_COUNT_UPI_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_UPI_last_1_week,
            DIV0(TOTAL_DEBIT_COUNT_UPI_last_2_week, TOTAL_CREDIT_COUNT_UPI_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_UPI_last_2_week,
            DIV0(TOTAL_DEBIT_COUNT_UPI_last_3_week, TOTAL_CREDIT_COUNT_UPI_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_UPI_last_3_week,
            DIV0(TOTAL_DEBIT_COUNT_UPI_last_4_week, TOTAL_CREDIT_COUNT_UPI_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_UPI_last_4_week,

            -- ============================================
            -- DERIVED: ATM DEBIT/CREDIT RATIOS (4 weekly windows)
            -- ============================================
            DIV0(TOTAL_DEBIT_AMOUNT_ATM_last_1_week, TOTAL_CREDIT_AMOUNT_ATM_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_ATM_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_ATM_last_2_week, TOTAL_CREDIT_AMOUNT_ATM_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_ATM_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_ATM_last_3_week, TOTAL_CREDIT_AMOUNT_ATM_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_ATM_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_ATM_last_4_week, TOTAL_CREDIT_AMOUNT_ATM_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_ATM_last_4_week,

            DIV0(TOTAL_DEBIT_COUNT_ATM_last_1_week, TOTAL_CREDIT_COUNT_ATM_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_ATM_last_1_week,
            DIV0(TOTAL_DEBIT_COUNT_ATM_last_2_week, TOTAL_CREDIT_COUNT_ATM_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_ATM_last_2_week,
            DIV0(TOTAL_DEBIT_COUNT_ATM_last_3_week, TOTAL_CREDIT_COUNT_ATM_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_ATM_last_3_week,
            DIV0(TOTAL_DEBIT_COUNT_ATM_last_4_week, TOTAL_CREDIT_COUNT_ATM_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_ATM_last_4_week,

            -- ============================================
            -- DERIVED: BANK TRANSFER DEBIT/CREDIT RATIOS (4 weekly windows)
            -- ============================================
            DIV0(TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_1_week, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_BANK_TRANSFER_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_2_week, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_BANK_TRANSFER_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_3_week, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_BANK_TRANSFER_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_BANK_TRANSFER_last_4_week, TOTAL_CREDIT_AMOUNT_BANK_TRANSFER_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_BANK_TRANSFER_last_4_week,

            DIV0(TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_1_week, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_BANK_TRANSFER_last_1_week,
            DIV0(TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_2_week, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_BANK_TRANSFER_last_2_week,
            DIV0(TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_3_week, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_BANK_TRANSFER_last_3_week,
            DIV0(TOTAL_DEBIT_COUNT_BANK_TRANSFER_last_4_week, TOTAL_CREDIT_COUNT_BANK_TRANSFER_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_BANK_TRANSFER_last_4_week,

            -- ============================================
            -- DERIVED: CHEQUE DEBIT/CREDIT RATIOS (4 weekly windows)
            -- ============================================
            DIV0(TOTAL_DEBIT_AMOUNT_CHEQUE_last_1_week, TOTAL_CREDIT_AMOUNT_CHEQUE_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CHEQUE_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CHEQUE_last_2_week, TOTAL_CREDIT_AMOUNT_CHEQUE_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CHEQUE_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CHEQUE_last_3_week, TOTAL_CREDIT_AMOUNT_CHEQUE_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CHEQUE_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CHEQUE_last_4_week, TOTAL_CREDIT_AMOUNT_CHEQUE_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CHEQUE_last_4_week,

            DIV0(TOTAL_DEBIT_COUNT_CHEQUE_last_1_week, TOTAL_CREDIT_COUNT_CHEQUE_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CHEQUE_last_1_week,
            DIV0(TOTAL_DEBIT_COUNT_CHEQUE_last_2_week, TOTAL_CREDIT_COUNT_CHEQUE_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CHEQUE_last_2_week,
            DIV0(TOTAL_DEBIT_COUNT_CHEQUE_last_3_week, TOTAL_CREDIT_COUNT_CHEQUE_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CHEQUE_last_3_week,
            DIV0(TOTAL_DEBIT_COUNT_CHEQUE_last_4_week, TOTAL_CREDIT_COUNT_CHEQUE_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CHEQUE_last_4_week,

            -- ============================================
            -- DERIVED: CASH DEBIT/CREDIT RATIOS (4 weekly windows)
            -- ============================================
            DIV0(TOTAL_DEBIT_AMOUNT_CASH_last_1_week, TOTAL_CREDIT_AMOUNT_CASH_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CASH_last_1_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CASH_last_2_week, TOTAL_CREDIT_AMOUNT_CASH_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CASH_last_2_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CASH_last_3_week, TOTAL_CREDIT_AMOUNT_CASH_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CASH_last_3_week,
            DIV0(TOTAL_DEBIT_AMOUNT_CASH_last_4_week, TOTAL_CREDIT_AMOUNT_CASH_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_AMOUNT_CASH_last_4_week,

            DIV0(TOTAL_DEBIT_COUNT_CASH_last_1_week, TOTAL_CREDIT_COUNT_CASH_last_1_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CASH_last_1_week,
            DIV0(TOTAL_DEBIT_COUNT_CASH_last_2_week, TOTAL_CREDIT_COUNT_CASH_last_2_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CASH_last_2_week,
            DIV0(TOTAL_DEBIT_COUNT_CASH_last_3_week, TOTAL_CREDIT_COUNT_CASH_last_3_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CASH_last_3_week,
            DIV0(TOTAL_DEBIT_COUNT_CASH_last_4_week, TOTAL_CREDIT_COUNT_CASH_last_4_week) AS RATIO_TOTAL_DEBIT_CREDIT_COUNT_CASH_last_4_week,

            -- ============================================
            -- DERIVED: TRANSACTION BUCKET RATIOS (4 weekly windows)
            -- ============================================
            DIV0(NUM_DEBIT_TXNS_LT_100_last_1_week, NUM_CREDIT_TXNS_LT_100_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_last_1_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_last_2_week, NUM_CREDIT_TXNS_LT_100_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_last_2_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_last_3_week, NUM_CREDIT_TXNS_LT_100_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_last_3_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_last_4_week, NUM_CREDIT_TXNS_LT_100_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_last_4_week,

            DIV0(NUM_DEBIT_TXNS_100_500_last_1_week, NUM_CREDIT_TXNS_100_500_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_last_1_week,
            DIV0(NUM_DEBIT_TXNS_100_500_last_2_week, NUM_CREDIT_TXNS_100_500_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_last_2_week,
            DIV0(NUM_DEBIT_TXNS_100_500_last_3_week, NUM_CREDIT_TXNS_100_500_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_last_3_week,
            DIV0(NUM_DEBIT_TXNS_100_500_last_4_week, NUM_CREDIT_TXNS_100_500_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_last_4_week,

            DIV0(NUM_DEBIT_TXNS_500_2000_last_1_week, NUM_CREDIT_TXNS_500_2000_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_last_1_week,
            DIV0(NUM_DEBIT_TXNS_500_2000_last_2_week, NUM_CREDIT_TXNS_500_2000_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_last_2_week,
            DIV0(NUM_DEBIT_TXNS_500_2000_last_3_week, NUM_CREDIT_TXNS_500_2000_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_last_3_week,
            DIV0(NUM_DEBIT_TXNS_500_2000_last_4_week, NUM_CREDIT_TXNS_500_2000_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_last_4_week,

            DIV0(NUM_DEBIT_TXNS_2000_5000_last_1_week, NUM_CREDIT_TXNS_2000_5000_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_last_1_week,
            DIV0(NUM_DEBIT_TXNS_2000_5000_last_2_week, NUM_CREDIT_TXNS_2000_5000_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_last_2_week,
            DIV0(NUM_DEBIT_TXNS_2000_5000_last_3_week, NUM_CREDIT_TXNS_2000_5000_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_last_3_week,
            DIV0(NUM_DEBIT_TXNS_2000_5000_last_4_week, NUM_CREDIT_TXNS_2000_5000_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_last_4_week,

            DIV0(NUM_DEBIT_TXNS_5000_10000_last_1_week, NUM_CREDIT_TXNS_5000_10000_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_last_1_week,
            DIV0(NUM_DEBIT_TXNS_5000_10000_last_2_week, NUM_CREDIT_TXNS_5000_10000_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_last_2_week,
            DIV0(NUM_DEBIT_TXNS_5000_10000_last_3_week, NUM_CREDIT_TXNS_5000_10000_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_last_3_week,
            DIV0(NUM_DEBIT_TXNS_5000_10000_last_4_week, NUM_CREDIT_TXNS_5000_10000_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_last_4_week,

            DIV0(NUM_DEBIT_TXNS_GT_10000_last_1_week, NUM_CREDIT_TXNS_GT_10000_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_last_1_week,
            DIV0(NUM_DEBIT_TXNS_GT_10000_last_2_week, NUM_CREDIT_TXNS_GT_10000_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_last_2_week,
            DIV0(NUM_DEBIT_TXNS_GT_10000_last_3_week, NUM_CREDIT_TXNS_GT_10000_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_last_3_week,
            DIV0(NUM_DEBIT_TXNS_GT_10000_last_4_week, NUM_CREDIT_TXNS_GT_10000_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_last_4_week,

            -- ============================================
            -- DERIVED: CC BUCKET RATIOS (4 weekly windows)
            -- ============================================
            DIV0(NUM_DEBIT_TXNS_LT_100_CC_last_1_week, NUM_CREDIT_TXNS_LT_100_CC_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_CC_last_2_week, NUM_CREDIT_TXNS_LT_100_CC_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_CC_last_3_week, NUM_CREDIT_TXNS_LT_100_CC_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_CC_last_4_week, NUM_CREDIT_TXNS_LT_100_CC_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_LT_100_CC_last_4_week,

            DIV0(NUM_DEBIT_TXNS_100_500_CC_last_1_week, NUM_CREDIT_TXNS_100_500_CC_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_100_500_CC_last_2_week, NUM_CREDIT_TXNS_100_500_CC_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_100_500_CC_last_3_week, NUM_CREDIT_TXNS_100_500_CC_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_100_500_CC_last_4_week, NUM_CREDIT_TXNS_100_500_CC_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_100_500_CC_last_4_week,

            DIV0(NUM_DEBIT_TXNS_500_2000_CC_last_1_week, NUM_CREDIT_TXNS_500_2000_CC_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_500_2000_CC_last_2_week, NUM_CREDIT_TXNS_500_2000_CC_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_500_2000_CC_last_3_week, NUM_CREDIT_TXNS_500_2000_CC_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_500_2000_CC_last_4_week, NUM_CREDIT_TXNS_500_2000_CC_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_500_2000_CC_last_4_week,

            DIV0(NUM_DEBIT_TXNS_2000_5000_CC_last_1_week, NUM_CREDIT_TXNS_2000_5000_CC_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_2000_5000_CC_last_2_week, NUM_CREDIT_TXNS_2000_5000_CC_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_2000_5000_CC_last_3_week, NUM_CREDIT_TXNS_2000_5000_CC_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_2000_5000_CC_last_4_week, NUM_CREDIT_TXNS_2000_5000_CC_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_2000_5000_CC_last_4_week,

            DIV0(NUM_DEBIT_TXNS_5000_10000_CC_last_1_week, NUM_CREDIT_TXNS_5000_10000_CC_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_5000_10000_CC_last_2_week, NUM_CREDIT_TXNS_5000_10000_CC_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_5000_10000_CC_last_3_week, NUM_CREDIT_TXNS_5000_10000_CC_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_5000_10000_CC_last_4_week, NUM_CREDIT_TXNS_5000_10000_CC_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_5000_10000_CC_last_4_week,

            DIV0(NUM_DEBIT_TXNS_GT_10000_CC_last_1_week, NUM_CREDIT_TXNS_GT_10000_CC_last_1_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_CC_last_1_week,
            DIV0(NUM_DEBIT_TXNS_GT_10000_CC_last_2_week, NUM_CREDIT_TXNS_GT_10000_CC_last_2_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_CC_last_2_week,
            DIV0(NUM_DEBIT_TXNS_GT_10000_CC_last_3_week, NUM_CREDIT_TXNS_GT_10000_CC_last_3_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_CC_last_3_week,
            DIV0(NUM_DEBIT_TXNS_GT_10000_CC_last_4_week, NUM_CREDIT_TXNS_GT_10000_CC_last_4_week) AS RATIO_NUM_DEBIT_CREDIT_TXNS_GT_10000_CC_last_4_week,

            -- ============================================
            -- DERIVED: TREND RATIOS - LATEST VS OLD
            -- ============================================
            -- Credit Amount Trend (Week 1-2 vs Week 3-4)
            DIV0(TOTAL_CREDIT_AMOUNT_last_1_week + TOTAL_CREDIT_AMOUNT_last_2_week, TOTAL_CREDIT_AMOUNT_last_3_week + TOTAL_CREDIT_AMOUNT_last_4_week) AS RATIO_CREDIT_AMOUNT_LATEST_VS_OLD,

            -- Debit Amount Trend
            DIV0(TOTAL_DEBIT_AMOUNT_last_1_week + TOTAL_DEBIT_AMOUNT_last_2_week, TOTAL_DEBIT_AMOUNT_last_3_week + TOTAL_DEBIT_AMOUNT_last_4_week) AS RATIO_DEBIT_AMOUNT_LATEST_VS_OLD,

            -- Credit Transaction Count Trend
            DIV0(NUM_CREDIT_TXNS_last_1_week + NUM_CREDIT_TXNS_last_2_week, NUM_CREDIT_TXNS_last_3_week + NUM_CREDIT_TXNS_last_4_week) AS RATIO_CREDIT_TXNS_LATEST_VS_OLD,

            -- Debit Transaction Count Trend
            DIV0(NUM_DEBIT_TXNS_last_1_week + NUM_DEBIT_TXNS_last_2_week, NUM_DEBIT_TXNS_last_3_week + NUM_DEBIT_TXNS_last_4_week) AS RATIO_DEBIT_TXNS_LATEST_VS_OLD,

            -- Available Balance Trend
            DIV0(MAX_AVAILABLE_BALANCE_last_1_week, MAX_AVAILABLE_BALANCE_last_3_week) AS RATIO_MAX_AVAILABLE_BALANCE_LATEST_VS_OLD,
            DIV0(MIN_AVAILABLE_BALANCE_last_1_week, MIN_AVAILABLE_BALANCE_last_3_week) AS RATIO_MIN_AVAILABLE_BALANCE_LATEST_VS_OLD,
            DIV0(AVG_AVAILABLE_BALANCE_last_1_week, AVG_AVAILABLE_BALANCE_last_3_week) AS RATIO_AVG_AVAILABLE_BALANCE_LATEST_VS_OLD,

            -- Credit Card Trends
            DIV0(TOTAL_CREDIT_AMOUNT_CC_last_1_week + TOTAL_CREDIT_AMOUNT_CC_last_2_week, TOTAL_CREDIT_AMOUNT_CC_last_3_week + TOTAL_CREDIT_AMOUNT_CC_last_4_week) AS RATIO_CREDIT_AMOUNT_CC_LATEST_VS_OLD,
            DIV0(TOTAL_DEBIT_AMOUNT_CC_last_1_week + TOTAL_DEBIT_AMOUNT_CC_last_2_week, TOTAL_DEBIT_AMOUNT_CC_last_3_week + TOTAL_DEBIT_AMOUNT_CC_last_4_week) AS RATIO_DEBIT_AMOUNT_CC_LATEST_VS_OLD,
            DIV0(NUM_CREDIT_TXNS_CC_last_1_week + NUM_CREDIT_TXNS_CC_last_2_week, NUM_CREDIT_TXNS_CC_last_3_week + NUM_CREDIT_TXNS_CC_last_4_week) AS RATIO_CREDIT_TXNS_CC_LATEST_VS_OLD,
            DIV0(NUM_DEBIT_TXNS_CC_last_1_week + NUM_DEBIT_TXNS_CC_last_2_week, NUM_DEBIT_TXNS_CC_last_3_week + NUM_DEBIT_TXNS_CC_last_4_week) AS RATIO_DEBIT_TXNS_CC_LATEST_VS_OLD,

            -- CC Due Amount Trend
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_1_week, MAX_TOTAL_DUE_AMOUNT_CC_last_3_week) AS RATIO_MAX_CC_DUE_AMOUNT_LATEST_VS_OLD,

            -- CC Available Limit Trend
            DIV0(MAX_AVAILABLE_LIMIT_CC_last_1_week, MAX_AVAILABLE_LIMIT_CC_last_3_week) AS RATIO_MAX_CC_AVAILABLE_LIMIT_LATEST_VS_OLD,

            -- UPI Trends
            DIV0(TOTAL_CREDIT_AMOUNT_UPI_last_1_week + TOTAL_CREDIT_AMOUNT_UPI_last_2_week, TOTAL_CREDIT_AMOUNT_UPI_last_3_week + TOTAL_CREDIT_AMOUNT_UPI_last_4_week) AS RATIO_CREDIT_AMOUNT_UPI_LATEST_VS_OLD,
            DIV0(TOTAL_DEBIT_AMOUNT_UPI_last_1_week + TOTAL_DEBIT_AMOUNT_UPI_last_2_week, TOTAL_DEBIT_AMOUNT_UPI_last_3_week + TOTAL_DEBIT_AMOUNT_UPI_last_4_week) AS RATIO_DEBIT_AMOUNT_UPI_LATEST_VS_OLD,
            DIV0(TOTAL_CREDIT_COUNT_UPI_last_1_week + TOTAL_CREDIT_COUNT_UPI_last_2_week, TOTAL_CREDIT_COUNT_UPI_last_3_week + TOTAL_CREDIT_COUNT_UPI_last_4_week) AS RATIO_CREDIT_COUNT_UPI_LATEST_VS_OLD,
            DIV0(TOTAL_DEBIT_COUNT_UPI_last_1_week + TOTAL_DEBIT_COUNT_UPI_last_2_week, TOTAL_DEBIT_COUNT_UPI_last_3_week + TOTAL_DEBIT_COUNT_UPI_last_4_week) AS RATIO_DEBIT_COUNT_UPI_LATEST_VS_OLD,

            -- ============================================
            -- DERIVED: CREDIT CARD UTILIZATION RATIO (10 windows)
            -- ============================================
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_1_2_day, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_1_2_day, 0)) AS CC_UTILIZATION_RATIO_last_1_2_day,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_3_4_day, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_3_4_day, 0)) AS CC_UTILIZATION_RATIO_last_3_4_day,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_5_6_day, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_5_6_day, 0)) AS CC_UTILIZATION_RATIO_last_5_6_day,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_7_8_day, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_7_8_day, 0)) AS CC_UTILIZATION_RATIO_last_7_8_day,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_9_11_day, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_9_11_day, 0)) AS CC_UTILIZATION_RATIO_last_9_11_day,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_12_14_day, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_12_14_day, 0)) AS CC_UTILIZATION_RATIO_last_12_14_day,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_1_week, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_1_week, 0)) AS CC_UTILIZATION_RATIO_last_1_week,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_2_week, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_2_week, 0)) AS CC_UTILIZATION_RATIO_last_2_week,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_3_week, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_3_week, 0)) AS CC_UTILIZATION_RATIO_last_3_week,
            DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_4_week, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_4_week, 0)) AS CC_UTILIZATION_RATIO_last_4_week,

            -- ============================================
            -- DERIVED: PROPORTION OF SMALL TRANSACTIONS (4 weekly windows)
            -- ============================================
            DIV0(NUM_CREDIT_TXNS_LT_100_last_1_week, NUM_CREDIT_TXNS_last_1_week) AS PROP_CREDIT_TXNS_LT_100_last_1_week,
            DIV0(NUM_CREDIT_TXNS_LT_100_last_2_week, NUM_CREDIT_TXNS_last_2_week) AS PROP_CREDIT_TXNS_LT_100_last_2_week,
            DIV0(NUM_CREDIT_TXNS_LT_100_last_3_week, NUM_CREDIT_TXNS_last_3_week) AS PROP_CREDIT_TXNS_LT_100_last_3_week,
            DIV0(NUM_CREDIT_TXNS_LT_100_last_4_week, NUM_CREDIT_TXNS_last_4_week) AS PROP_CREDIT_TXNS_LT_100_last_4_week,

            DIV0(NUM_DEBIT_TXNS_LT_100_last_1_week, NUM_DEBIT_TXNS_last_1_week) AS PROP_DEBIT_TXNS_LT_100_last_1_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_last_2_week, NUM_DEBIT_TXNS_last_2_week) AS PROP_DEBIT_TXNS_LT_100_last_2_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_last_3_week, NUM_DEBIT_TXNS_last_3_week) AS PROP_DEBIT_TXNS_LT_100_last_3_week,
            DIV0(NUM_DEBIT_TXNS_LT_100_last_4_week, NUM_DEBIT_TXNS_last_4_week) AS PROP_DEBIT_TXNS_LT_100_last_4_week,

            -- ============================================
            -- DERIVED: RISK FLAGS
            -- ============================================
            CASE WHEN (NUM_CHEQUE_BOUNCES_last_1_week + NUM_CHEQUE_BOUNCES_last_2_week) > 0 THEN 1 ELSE 0 END AS CHEQUE_BOUNCE_RECENT_FLAG,
            CASE WHEN (NUM_NACH_BOUNCES_last_1_week + NUM_NACH_BOUNCES_last_2_week) > 0 THEN 1 ELSE 0 END AS NACH_BOUNCE_RECENT_FLAG,
            CASE WHEN (NUM_LOAN_EMI_OVERDUE_last_1_week + NUM_LOAN_EMI_OVERDUE_last_2_week) > 0 THEN 1 ELSE 0 END AS LOAN_OVERDUE_RECENT_FLAG,
            CASE WHEN (NUM_LOAN_DEFAULT_last_1_week + NUM_LOAN_DEFAULT_last_2_week) > 0 THEN 1 ELSE 0 END AS LOAN_DEFAULT_RECENT_FLAG,
            CASE WHEN (NUM_CC_OVERDUE_last_1_week + NUM_CC_OVERDUE_last_2_week) > 0 THEN 1 ELSE 0 END AS CC_OVERDUE_RECENT_FLAG,
            CASE WHEN (NUM_CC_DEFAULT_last_1_week + NUM_CC_DEFAULT_last_2_week) > 0 THEN 1 ELSE 0 END AS CC_DEFAULT_RECENT_FLAG,
            CASE WHEN (NUM_MIN_BALANCE_BREACH_last_1_week + NUM_MIN_BALANCE_BREACH_last_2_week) > 0 THEN 1 ELSE 0 END AS MIN_BALANCE_BREACH_RECENT_FLAG,

            -- Risk Trend (Increasing?)
            CASE WHEN (NUM_CHEQUE_BOUNCES_last_1_week + NUM_CHEQUE_BOUNCES_last_2_week) > (NUM_CHEQUE_BOUNCES_last_3_week + NUM_CHEQUE_BOUNCES_last_4_week) THEN 1 ELSE 0 END AS CHEQUE_BOUNCE_INCREASING_FLAG,
            CASE WHEN (NUM_NACH_BOUNCES_last_1_week + NUM_NACH_BOUNCES_last_2_week) > (NUM_NACH_BOUNCES_last_3_week + NUM_NACH_BOUNCES_last_4_week) THEN 1 ELSE 0 END AS NACH_BOUNCE_INCREASING_FLAG,
            CASE WHEN (NUM_LOAN_EMI_OVERDUE_last_1_week + NUM_LOAN_EMI_OVERDUE_last_2_week) > (NUM_LOAN_EMI_OVERDUE_last_3_week + NUM_LOAN_EMI_OVERDUE_last_4_week) THEN 1 ELSE 0 END AS LOAN_OVERDUE_INCREASING_FLAG,

            -- CC Stress Flags
            CASE WHEN DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_1_week, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_1_week, 0)) > 0.7 THEN 1 ELSE 0 END AS CC_HIGH_UTILIZATION_FLAG,
            CASE WHEN (NUM_CC_OVERDUE_last_1_week + NUM_CC_OVERDUE_last_2_week) > 0 OR DIV0(MAX_TOTAL_DUE_AMOUNT_CC_last_1_week, NULLIF(MAX_AVAILABLE_LIMIT_CC_last_1_week, 0)) > 0.7 THEN 1 ELSE 0 END AS CC_FINANCIAL_STRESS_FLAG,

            -- Balance Declining Flag
            CASE WHEN MAX_AVAILABLE_BALANCE_last_1_week < MAX_AVAILABLE_BALANCE_last_3_week THEN 1 ELSE 0 END AS BALANCE_DECLINING_FLAG,

            -- ============================================
            -- DERIVED: AVG AMOUNT TREND RATIOS (Weekly avg comparison)
            -- ============================================
            DIV0(AVG_CDT_AMT_last_1_week, AVG_CDT_AMT_last_3_week) AS RATIO_AVG_CDT_AMT_LATEST_VS_OLD,
            DIV0(AVG_DEBIT_AMOUNT_last_1_week, AVG_DEBIT_AMOUNT_last_3_week) AS RATIO_AVG_DEBIT_AMOUNT_LATEST_VS_OLD,
            DIV0(AVG_CREDIT_TXNS_last_1_week, AVG_CREDIT_TXNS_last_3_week) AS RATIO_AVG_CREDIT_TXNS_LATEST_VS_OLD,
            DIV0(AVG_DEBIT_TXNS_last_1_week, AVG_DEBIT_TXNS_last_3_week) AS RATIO_AVG_DEBIT_TXNS_LATEST_VS_OLD,

            DIV0(AVG_CDT_AMT_CC_last_1_week, AVG_CDT_AMT_CC_last_3_week) AS RATIO_AVG_CDT_AMT_CC_LATEST_VS_OLD,
            DIV0(AVG_DEBIT_AMOUNT_CC_last_1_week, AVG_DEBIT_AMOUNT_CC_last_3_week) AS RATIO_AVG_DEBIT_AMOUNT_CC_LATEST_VS_OLD,
            DIV0(AVG_CREDIT_TXNS_CC_last_1_week, AVG_CREDIT_TXNS_CC_last_3_week) AS RATIO_AVG_CREDIT_TXNS_CC_LATEST_VS_OLD,
            DIV0(AVG_DEBIT_TXNS_CC_last_1_week, AVG_DEBIT_TXNS_CC_last_3_week) AS RATIO_AVG_DEBIT_TXNS_CC_LATEST_VS_OLD,

            -- ============================================
            -- DERIVED: MAX/MIN AMOUNT TREND RATIOS
            -- ============================================
            DIV0(MAX_CDT_AMT_last_1_week, MAX_CDT_AMT_last_3_week) AS RATIO_MAX_CDT_AMT_LATEST_VS_OLD,
            DIV0(MAX_DEBIT_AMOUNT_last_1_week, MAX_DEBIT_AMOUNT_last_3_week) AS RATIO_MAX_DEBIT_AMOUNT_LATEST_VS_OLD,
            DIV0(MIN_CDT_AMT_last_1_week, MIN_CDT_AMT_last_3_week) AS RATIO_MIN_CDT_AMT_LATEST_VS_OLD,
            DIV0(MIN_DEBIT_AMOUNT_last_1_week, MIN_DEBIT_AMOUNT_last_3_week) AS RATIO_MIN_DEBIT_AMOUNT_LATEST_VS_OLD,

            DIV0(MAX_CDT_AMT_CC_last_1_week, MAX_CDT_AMT_CC_last_3_week) AS RATIO_MAX_CDT_AMT_CC_LATEST_VS_OLD,
            DIV0(MAX_DEBIT_AMOUNT_CC_last_1_week, MAX_DEBIT_AMOUNT_CC_last_3_week) AS RATIO_MAX_DEBIT_AMOUNT_CC_LATEST_VS_OLD,
            DIV0(MIN_CDT_AMT_CC_last_1_week, MIN_CDT_AMT_CC_last_3_week) AS RATIO_MIN_CDT_AMT_CC_LATEST_VS_OLD,
            DIV0(MIN_DEBIT_AMOUNT_CC_last_1_week, MIN_DEBIT_AMOUNT_CC_last_3_week) AS RATIO_MIN_DEBIT_AMOUNT_CC_LATEST_VS_OLD

        FROM sms_all_features
    )

    SELECT * FROM sms_derived_features
);


-- -- Check data
-- SELECT * FROM analytics.data_science.data_early_dpd2_sms_final_features limit 10 ;
-- select count(*) from analytics.data_science.data_early_dpd2_sms_final_features;

describe table analytics.data_science.data_early_dpd2_sms_final_features;

-- SELECT
--     COUNT(*) AS total_rows,
--     SUM(CASE WHEN USER_ID IS NULL THEN 1 ELSE 0 END) AS null_user_id,
--     SUM(CASE WHEN cutoff_date IS NULL THEN 1 ELSE 0 END) AS null_CUTOFF_DATE
-- FROM analytics.data_science.data_early_dpd2_sms_final_features;




select count(*) , count (distinct user_id), count(distinct user_id, cutoff_date) from analytics.data_science.data_early_dpd2_sms_final_features;
-- COUNT(*)	COUNT (DISTINCT USER_ID)	COUNT(DISTINCT USER_ID, CUTOFF_DATE)
-- 5238679	2586889	5238679


-- describe table analytics.data_science.data_early_dpd2_sms_final_features;
