-- Set the base table (override this variable before executing if needed)
set base_tbl = 'analytics.data_science.early_dpd_base_2dpd';

with userbase as (
    select distinct
        loan_id,
        cutoff_date,
        user_id
    from identifier($base_tbl)
),

loan_base_fact as (
    select
        a.loan_id,
        a.cutoff_date,
        a.user_id,
        b.* exclude (loan_id, user_id),
    from userbase a
    left join model.collections_loan_mandates_basefact b
        on a.loan_id = b.loan_id
        and b.mandate_created_at <= a.cutoff_date

),

mandates_base_fact as (
    select *
    from model.collections_mandates_basefact
),

latest_mandate_data as (
    select distinct
        a.loan_id,
        a.cutoff_date,
        a.user_id,
        a.loan_status,
        a.loan_disbursed_date,
        a.actual_loan_end_date,
        a.reference_id,
        a.mode,
        a.status as latest_status,
        a.payment_mandate_status as latest_payment_mandate_status,
        a.mandate_active_duration_log,
        a.loan_mandate_mode_split,
        b.mandate_created_at,
        b.mandate_end_at,
        b.mandate_last_updated_at,
        b.mandate_amount,
        b.lender,
        b.mandate_status_reason,
        b.provider_to_loans,
        b.provider_to_payments,
        b.amount_mode,
        b.mandate_type,
        b.bank_name,
        b.branch_name,
        b.is_mandate_cancellable,
        datediff('day', a.loan_disbursed_date, b.mandate_created_at) as disbursal_to_mandate_days,
        datediff('day', b.mandate_created_at, b.mandate_end_at) as mandate_planned_span_days,
        case
            when b.mandate_end_at > a.actual_loan_end_date
                and a.actual_loan_end_date is not null
            then 1 else 0
        end as mandate_outlives_loan,
        case
            when b.mandate_created_at < a.loan_disbursed_date
                and a.loan_disbursed_date is not null
            then 1 else 0
        end as mandate_pre_disbursal
    from loan_base_fact a
    left join mandates_base_fact b
        on a.reference_id = b.reference_id
    where a.reference_id is not null
),

mandate_history as (
    select
        b.*,
        a.status as history_status,
        a.updated_at as history_updated_at,
        a.raw_response,
        a.raw_data,
        hour(a.updated_at) as event_hour,
        case
            when hour(a.updated_at) between 9 and 18 then 1 else 0
        end as is_business_hours_event,
        case
            when hour(a.updated_at) < 9 or hour(a.updated_at) > 18 then 1 else 0
        end as is_off_hours_event
    from APP_BACKEND.KB_PAYMENTS_PROD.PUBLIC_MANDATE_HISTORY_VW a
    inner join (select distinct * from latest_mandate_data) b
        on a.mandate_id = b.reference_id
    where a.updated_at <= b.cutoff_date
),

public_mandates as (
    select
        a.*,
        b.meta,
        b.amount_su,
        b.provider,
        b.feature,
        b.recurring_frequency,
        b.raw_data as raw_data_public_mandate,
        try_parse_json(b.meta):raw_response:auth_sub_mode::string as auth_sub_mode,
        try_parse_json(b.meta):raw_response:umrn::string as umrn,
        try_parse_json(b.meta):raw_response:upi:vpa::string as upi_vpa,
        try_parse_json(b.meta):raw_response:upi:flow::string as upi_flow,
        try_parse_json(b.meta):activation_debit_info:amount_su::int as activation_debit_amount_su,
        try_parse_json(b.meta):activation_debit_info:is_refund_required::boolean as activation_refund_required,
        try_parse_json(b.meta):parsed_response:code::string as gateway_response_code,
        try_parse_json(b.meta):raw_response:bank_details:state::string as bank_registration_state,
        try_parse_json(b.meta):raw_response:mandate_details:sponsor_bank_name::string as sponsor_bank_name,
        try_parse_json(b.meta):raw_response:mandate_details:destination_bank_name::string as destination_bank_name,
        try_parse_json(b.meta):raw_response:mandate_details:customer_account_type::string as customer_account_type,
        try_to_timestamp(try_parse_json(b.meta):last_status_sync_at::string) as last_status_sync_at
    from (select distinct * from mandate_history) a
    left join APP_BACKEND.KB_PAYMENTS_PROD.PUBLIC_MANDATES_VW b
        on a.reference_id = b.id
        and a.history_status = b.status
)

select *
from public_mandates
order by loan_id, cutoff_date, reference_id, history_updated_at
