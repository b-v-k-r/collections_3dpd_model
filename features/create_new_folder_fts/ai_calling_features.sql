-- ai_calling_features.sql
-- ============================================================
-- Produces one row per (USER_ID, CUTOFF_DATE) with historical
-- AI-calling-related features derived from historical collections
-- allocation data in analytics.log.lending_collections_daily_allocation_ssot.
--
-- Lookback windows before cutoff:
--   1-7 days
--   8-15 days
--   16-30 days
--
-- Pattern matches the other feature SQL files in this folder:
--   * SET base_tbl = '...';
--   * identifier($base_tbl)
--   * CREATE OR REPLACE TABLE ... AS
-- ============================================================

set base_tbl = 'analytics.data_science.early_dpd3_base';

create or replace table analytics.data_science.ai_calling_features_for_early_dpd3 as

with base as (
    select distinct
        loan_id,
        user_id,
        cutoff_date
    from identifier($base_tbl)
    where loan_id is not null
      and user_id is not null
      and cutoff_date is not null
),

level1_config as (
    select
        start_dpd,
        end_dpd,
        start_date,
        end_date,
        strtok_to_array(translate(risk_bucket::varchar, '[]', ''), ',') as risk_bucket,
        strtok_to_array(translate(language, '[]', ''), ',') as language,
        strtok_to_array(translate(nps::variant, '[]', ''), ',') as nps,
        cohort_size,
        call_type
    from analytics.reporting.collections_ai_calling_level1_config
),

latest_call_type as (
    select
        l.loan_id,
        pl.raw_data:"calling_type"::varchar as calling_type_allocated
    from app_backend.io_sales_prod.public_leads_vw pl
    join (
        select id
        from app_backend.io_sales_prod.public_products_vw
        where name = 'COLLECTIONS'
    ) p
        on p.id = pl.product_id
    join model.loan_origination_characteristics l
        on pl.phone = l.phone
       and l.loan_disbursed_date <= pl.updated_at::date
       and pl.created_at::date <= coalesce(l.actual_loan_end_date, current_date)
    where pl.raw_data:"calling_type"::varchar is not null
    qualify row_number() over (partition by l.loan_id order by pl.updated_at desc) = 1
),

risk_band_ssot as (
    select
        loan_id,
        todays_date,
        dpd,
        case when dpd > 6 and risk_band is null then 'H' else risk_band end as risk_bucket
    from analytics.log.lending_collections_daily_allocation_ssot
),

pincode_language as (
    select distinct
        pincode,
        primary_language,
        secondary_language
    from app_backend.io_sales_prod.public_pincode_location_entity_mappings_vw
),

paused_loans as (
    select distinct
        id as loan_id
    from app_backend.loan_service_prod.public_loans_vw
    where metadata:"isCollectionPaused"::boolean = true
      and status = 'ACTIVE'
),

foreclosed_loans as (
    select distinct
        fl.loan_id
    from app_backend.loan_service_prod.public_loans_foreclosure_vw fl
    left join model.loan_origination_characteristics loc
        on fl.loan_id = loc.loan_id
       and loc.actual_loan_end_date is not null
    where loc.loan_id is null
      and fl.status in ('IN_PROGRESS', 'SUCCESS')
),

raw_history as (
    select
        b.loan_id as base_loan_id,
        b.user_id,
        b.cutoff_date,
        c.todays_date as collect_date,
        datediff('day', c.todays_date, b.cutoff_date) as days_before_cutoff,
        b.loan_id as loan_id,
        c.dpd as bod_dpd,
        round(loc.loan_amount) as loan_amount,
        c.risk_bucket,
        iff(
            loc.rule_version ilike 'COMMON_GEO_EXPANSION_POLICY_VARIANT_2_KB_INSIGHTS'
            or loc.rule_version ilike 'COMMON_NON_PHYSICALLY_SERVICEABLE_POLICY_VARIANT_2_KB_INSIGHTS'
            or loc.rule_version ilike '%NON_PHYSICALLY_SERVICEABLE%',
            1,
            0
        ) as nps,
        l.primary_language,
        ai.start_dpd as calling_dpd,
        ai.cohort_size,
        coalesce(al.calling_type_allocated, 'NOT_ALLOCATED') as calling_type_allocated,
        case
            when ai.start_dpd is not null
            then 1 else 0
        end as is_ai_eligible
    from base b
    join risk_band_ssot c
        on c.loan_id = b.loan_id
       and c.todays_date between dateadd(day, -30, b.cutoff_date)
                             and dateadd(day,  -1, b.cutoff_date)
    left join latest_call_type al
        on b.loan_id = al.loan_id
    left join analytics.model.loan_origination_characteristics loc
        on b.loan_id = loc.loan_id
    left join pincode_language l
        on l.pincode = loc.pincode
    left join paused_loans p
        on p.loan_id = b.loan_id
    left join foreclosed_loans f
        on f.loan_id = b.loan_id
    left join level1_config ai
        on c.dpd between ai.start_dpd and ai.end_dpd
       and c.todays_date between ai.start_date and ai.end_date
       and array_contains(c.risk_bucket::variant, ai.risk_bucket)
       and array_contains(
            lower(l.primary_language)::variant,
            split(lower(array_to_string(ai.language, ',')), ',')
       )
       and array_contains(
            iff(
                loc.rule_version ilike 'COMMON_GEO_EXPANSION_POLICY_VARIANT_2_KB_INSIGHTS'
                or loc.rule_version ilike 'COMMON_NON_PHYSICALLY_SERVICEABLE_POLICY_VARIANT_2_KB_INSIGHTS'
                or loc.rule_version ilike '%NON_PHYSICALLY_SERVICEABLE%',
                '1',
                '0'
            )::variant,
            ai.nps
       )
       and coalesce(al.calling_type_allocated, 'NOT_ALLOCATED') = ai.call_type
    where p.loan_id is null
      and f.loan_id is null
    group by all
),

day_features as (
    select
        user_id,
        cutoff_date,
        collect_date,
        count(distinct loan_id) as num_collection_loans,
        count(distinct case when calling_dpd is not null then loan_id end) as num_level1_matched_loans,
        count(distinct case when is_ai_eligible = 1 then loan_id end) as num_ai_eligible_loans,

        avg(bod_dpd::float) as avg_bod_dpd,
        max(bod_dpd) as max_bod_dpd,
        avg(case when is_ai_eligible = 1 then bod_dpd::float end) as avg_bod_dpd_ai_eligible,
        max(case when is_ai_eligible = 1 then bod_dpd end) as max_bod_dpd_ai_eligible,

        avg(loan_amount) as avg_loan_amount,
        max(loan_amount) as max_loan_amount,
        avg(case when is_ai_eligible = 1 then cohort_size end) as avg_cohort_size_ai_eligible,

        sum(case when nps = 1 then 1 else 0 end) as num_nps_loans,
        sum(case when lower(primary_language) = 'hindi' then 1 else 0 end) as num_hindi_loans,
        sum(case when lower(primary_language) = 'english' then 1 else 0 end) as num_english_loans,
        sum(case when risk_bucket = 'H' then 1 else 0 end) as num_high_risk_loans,

        sum(case when upper(calling_type_allocated) = 'MANUAL' then 1 else 0 end) as num_manual_allocated_loans,
        sum(case when upper(calling_type_allocated) = 'IVR' then 1 else 0 end) as num_ivr_allocated_loans,
        sum(case when upper(calling_type_allocated) = 'NO_CALL' then 1 else 0 end) as num_no_call_allocated_loans,
        sum(case when upper(calling_type_allocated) = 'NOT_ALLOCATED' then 1 else 0 end) as num_not_allocated_loans,

        sum(case when bod_dpd between 1 and 3 then 1 else 0 end) as num_bod_dpd_1_3_loans,
        sum(case when bod_dpd between 4 and 10 then 1 else 0 end) as num_bod_dpd_4_10_loans,
        sum(case when bod_dpd > 10 then 1 else 0 end) as num_bod_dpd_gt10_loans
    from raw_history
    group by user_id, cutoff_date, collect_date
)

select
    b.user_id,
    b.cutoff_date,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then 1 else 0 end) as num_ai_history_days_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then 1 else 0 end) as num_ai_history_days_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then 1 else 0 end) as num_ai_history_days_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_collection_loans else 0 end) as num_collection_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_collection_loans else 0 end) as num_collection_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_collection_loans else 0 end) as num_collection_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_level1_matched_loans else 0 end) as num_level1_matched_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_level1_matched_loans else 0 end) as num_level1_matched_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_level1_matched_loans else 0 end) as num_level1_matched_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_ai_eligible_loans else 0 end) as num_ai_eligible_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_ai_eligible_loans else 0 end) as num_ai_eligible_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_ai_eligible_loans else 0 end) as num_ai_eligible_loans_16_to_30_d,

    avg(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.avg_bod_dpd end) as avg_bod_dpd_1_to_7_d,
    avg(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.avg_bod_dpd end) as avg_bod_dpd_8_to_15_d,
    avg(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.avg_bod_dpd end) as avg_bod_dpd_16_to_30_d,

    max(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.max_bod_dpd end) as max_bod_dpd_1_to_7_d,
    max(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.max_bod_dpd end) as max_bod_dpd_8_to_15_d,
    max(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.max_bod_dpd end) as max_bod_dpd_16_to_30_d,

    avg(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.avg_bod_dpd_ai_eligible end) as avg_bod_dpd_ai_eligible_1_to_7_d,
    avg(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.avg_bod_dpd_ai_eligible end) as avg_bod_dpd_ai_eligible_8_to_15_d,
    avg(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.avg_bod_dpd_ai_eligible end) as avg_bod_dpd_ai_eligible_16_to_30_d,

    avg(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.avg_loan_amount end) as avg_loan_amount_1_to_7_d,
    avg(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.avg_loan_amount end) as avg_loan_amount_8_to_15_d,
    avg(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.avg_loan_amount end) as avg_loan_amount_16_to_30_d,

    max(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.max_loan_amount end) as max_loan_amount_1_to_7_d,
    max(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.max_loan_amount end) as max_loan_amount_8_to_15_d,
    max(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.max_loan_amount end) as max_loan_amount_16_to_30_d,

    avg(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.avg_cohort_size_ai_eligible end) as avg_cohort_size_ai_eligible_1_to_7_d,
    avg(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.avg_cohort_size_ai_eligible end) as avg_cohort_size_ai_eligible_8_to_15_d,
    avg(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.avg_cohort_size_ai_eligible end) as avg_cohort_size_ai_eligible_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_nps_loans else 0 end) as num_nps_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_nps_loans else 0 end) as num_nps_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_nps_loans else 0 end) as num_nps_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_hindi_loans else 0 end) as num_hindi_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_hindi_loans else 0 end) as num_hindi_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_hindi_loans else 0 end) as num_hindi_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_english_loans else 0 end) as num_english_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_english_loans else 0 end) as num_english_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_english_loans else 0 end) as num_english_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_high_risk_loans else 0 end) as num_high_risk_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_high_risk_loans else 0 end) as num_high_risk_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_high_risk_loans else 0 end) as num_high_risk_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_manual_allocated_loans else 0 end) as num_manual_allocated_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_manual_allocated_loans else 0 end) as num_manual_allocated_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_manual_allocated_loans else 0 end) as num_manual_allocated_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_ivr_allocated_loans else 0 end) as num_ivr_allocated_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_ivr_allocated_loans else 0 end) as num_ivr_allocated_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_ivr_allocated_loans else 0 end) as num_ivr_allocated_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_no_call_allocated_loans else 0 end) as num_no_call_allocated_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_no_call_allocated_loans else 0 end) as num_no_call_allocated_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_no_call_allocated_loans else 0 end) as num_no_call_allocated_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_not_allocated_loans else 0 end) as num_not_allocated_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_not_allocated_loans else 0 end) as num_not_allocated_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_not_allocated_loans else 0 end) as num_not_allocated_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_bod_dpd_1_3_loans else 0 end) as num_bod_dpd_1_3_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_bod_dpd_1_3_loans else 0 end) as num_bod_dpd_1_3_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_bod_dpd_1_3_loans else 0 end) as num_bod_dpd_1_3_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_bod_dpd_4_10_loans else 0 end) as num_bod_dpd_4_10_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_bod_dpd_4_10_loans else 0 end) as num_bod_dpd_4_10_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_bod_dpd_4_10_loans else 0 end) as num_bod_dpd_4_10_loans_16_to_30_d,

    sum(case when d.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then d.num_bod_dpd_gt10_loans else 0 end) as num_bod_dpd_gt10_loans_1_to_7_d,
    sum(case when d.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then d.num_bod_dpd_gt10_loans else 0 end) as num_bod_dpd_gt10_loans_8_to_15_d,
    sum(case when d.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then d.num_bod_dpd_gt10_loans else 0 end) as num_bod_dpd_gt10_loans_16_to_30_d,

    datediff('day', max(d.collect_date), b.cutoff_date) as days_since_last_ai_history_30d,
    datediff(
        'day',
        max(case when d.num_ai_eligible_loans > 0 then d.collect_date end),
        b.cutoff_date
    ) as days_since_last_ai_eligible_day_30d
from base b
left join day_features d
    on b.user_id = d.user_id
   and b.cutoff_date = d.cutoff_date
group by b.user_id, b.cutoff_date;
