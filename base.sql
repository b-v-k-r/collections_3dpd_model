create
or replace table analytics.data_science.early_dpd_base_3dpd as with ranked_repayments as (
    SELECT
        a.loan_id,
        b.user_id,
        collect_at,
        lender_lead_flag,
        settlement_mis_flag,
        is_lender_payment_split_enabled,
        is_manual_repayment_on_lender_mid,
        ROW_NUMBER() OVER (
            PARTITION BY a.loan_id,
            DATE(collect_at)
            ORDER BY
                CASE
                    WHEN type <> 'AUTO' THEN 1
                    ELSE 2
                END
        ) as rn
    FROM
        APP_BACKEND.LOAN_SERVICE_PROD.PUBLIC_REPAYMENTS_VW a
        left join (
            select
                distinct customer_id as user_id,
                loan_id
            from
                analytics.model.loan_origination_characteristics
        ) b on a.loan_id = b.loan_id
    WHERE
        (
            (
                collected_amount_su > 0
                AND status = 'SUCCESS'
            )
            OR ideal_amount_su > 0
        )
),
collections_repayments as (
    SELECT
        a.user_id,
        a.loan_id,
        loan_status,
        collect_date as cutoff_date,
        datediff('day', first_edi_date, collect_date) as no_of_days_since_first_edi,
        ideal_amount as edi,
        due_amount as due_amount_bod,
        collected_amount as collected_amount_eod,
        excess_collected_amount,
        collected_amount_cumulative,
        principal_overdue,
        interest_overdue,
        unpaid_amount,
        COALESCE(
            ARRAY_SIZE(OBJECT_KEYS(repayments_transactions_split)),
            0
        ) as no_of_modes_of_repayments_used,
        repayments_transactions_split,
        pos,
        ios,
        actual_dpd,
        tilldate_max_dpd,
        is_upi_autopay_primary,
        is_allocation_experiment,
        team_allocation,
        b.lender_lead_flag,
        b.settlement_mis_flag,
        b.is_lender_payment_split_enabled,
        b.is_manual_repayment_on_lender_mid,
        datediff('day', c.loan_start_date, a.collect_date) as days_since_loan_start,
        datediff('day', a.collect_date, c.loan_end_date) as days_remaining_for_loan_end
    FROM
        LOG.COLLECTIONS_REPAYMENTS_BASEFACT a
        left join (
            select
                *
            from
                ranked_repayments
            where
                rn = 1
        ) b on date(a.collect_date) = date(b.collect_at)
        and a.loan_id = b.loan_id
        left join (
            select
                distinct loan_id,
                loan_start_date,
                actual_loan_end_date as loan_end_date
            from
                analytics.log.credit_performance_daily
        ) c on a.loan_id = c.loan_id
    WHERE
        LOAN_ACTIVE_AS_ON_DATE
        and is_hcb_loan_ondate = 0
        and ideal_amount is not null
        and cutoff_date between '2025-04-01' and '2026-02-28'
        and loan_start_date >= '2025-01-01'
),
target_variable_coding as (
    select
        a.*,
        case
            when future_max_dpd is null then -1
            when future_max_dpd >= 4 then 1
            else 0
        end as target_risk_bucket_3d
    from
        collections_repayments a
        left join (
            select
                a.loan_id,
                a.cutoff_date,
                max(b.actual_dpd) as future_max_dpd
            from
                collections_repayments a
                left join collections_repayments b on a.loan_id = b.loan_id
                and b.cutoff_date > a.cutoff_date
                and b.cutoff_date <= dateadd('day', 1, a.cutoff_date)
            where
                a.actual_dpd = 3
                and b.actual_dpd is not null
            group by
                a.loan_id,
                a.cutoff_date
        ) f on a.loan_id = f.loan_id
        and a.cutoff_date = f.cutoff_date
    where
        a.actual_dpd = 3
        and future_max_dpd is not null
)
select
    *
from
    target_variable_coding;
select
    target_risk_bucket_3d,
    count(*),
    count(*) * 100.0 / sum(count(*)) over() as perc_cnt
from
    analytics.data_science.early_dpd_base_3dpd
group by
    all;
