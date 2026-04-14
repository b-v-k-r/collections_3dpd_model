-- legal_automations.sql
-- ============================================================
-- Produces one row per (USER_ID, CUTOFF_DATE) with historical
-- legal-automation-related features derived from collections
-- repayments and legal notice logs.
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

set base_tbl = 'analytics.data_science.early_dpd2_base';

create or replace table analytics.data_science.legal_automation_features_for_early_dpd2 as

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

notice_sent_log as (
    select
        data:issuance_date::date as issuance_date,
        try_to_number(data:loan_id::varchar) as loan_id,
        upper(data:notice_type::varchar) as notice_type
    from raw.google_sheets.collections_legal_notice_tracker
    where regexp_like(data:loan_id, '^[0-9]+$')

    union all

    select
        coalesce(
            try_to_date(try_parse_json(data):date::string, 'dd-mon-yy'),
            try_to_date(try_parse_json(data):date::string, 'yyyy-mm-dd')
        )::date as issuance_date,
        try_parse_json(data):loan_id::number as loan_id,
        upper(source) as notice_type
    from app_backend.lending_notices.legal_notices_tracker_all
    where regexp_like(try_parse_json(lower(data)):loan_id::string, '^[0-9]+$')

    union all

    select
        issuance_date,
        loan_id,
        upper(notice_type) as notice_type
    from analytics.model.collections_legal_notice_tracker_backup
),

repayment_history as (
    select
        b.user_id,
        b.loan_id,
        b.cutoff_date,
        r.collect_date,
        datediff('day', r.collect_date, b.cutoff_date) as days_before_cutoff,
        r.actual_dpd,
        r.loan_status,
        r.due_amount as overdue_amount,
        r.collected_amount,
        iff(coalesce(r.due_amount, 0) > 0 and div0(r.collected_amount, r.due_amount) >= 0.2, 1, 0) as paid_20per_flag
    from base b
    join analytics.log.collections_repayments_basefact r
        on r.loan_id = b.loan_id
       and r.collect_date between dateadd(day, -30, b.cutoff_date)
                              and dateadd(day,  -1, b.cutoff_date)
),

repayment_day_features as (
    select
        user_id,
        loan_id,
        cutoff_date,
        collect_date,
        max(actual_dpd) as max_actual_dpd,
        avg(actual_dpd::float) as avg_actual_dpd,
        max(case when loan_status = 'ACTIVE' then 1 else 0 end) as is_active_day,
        max(case when actual_dpd >= 15 and loan_status = 'ACTIVE' then 1 else 0 end) as is_active_dpd_15_plus_day,
        max(case when actual_dpd >= 30 and loan_status = 'ACTIVE' then 1 else 0 end) as is_active_dpd_30_plus_day,
        sum(coalesce(overdue_amount, 0)) as total_overdue_amount,
        sum(coalesce(collected_amount, 0)) as total_collected_amount,
        max(paid_20per_flag) as paid_20per_flag_day
    from repayment_history
    group by user_id, loan_id, cutoff_date, collect_date
),

notice_history as (
    select
        b.user_id,
        b.loan_id,
        b.cutoff_date,
        n.issuance_date as notice_date,
        datediff('day', n.issuance_date, b.cutoff_date) as days_before_cutoff,
        n.notice_type
    from base b
    join notice_sent_log n
        on n.loan_id = b.loan_id
       and n.issuance_date < b.cutoff_date
       and n.issuance_date >= dateadd(day, -30, b.cutoff_date)
),

notice_day_features as (
    select
        user_id,
        loan_id,
        cutoff_date,
        notice_date,
        count(*) as num_legal_notices,
        sum(case when notice_type = 'DUNNING' then 1 else 0 end) as num_dunning_notices,
        sum(case when notice_type = 'LRN' then 1 else 0 end) as num_lrn_notices,
        sum(case when notice_type = 'OTHERS' then 1 else 0 end) as num_other_notices,
        sum(case when notice_type = 'AVOID' then 1 else 0 end) as num_avoid_notices
    from notice_history
    group by user_id, loan_id, cutoff_date, notice_date
),

asof_cutoff_state as (
    select
        b.user_id,
        b.loan_id,
        b.cutoff_date,
        s.actual_dpd as latest_actual_dpd,
        s.loan_status as latest_loan_status,
        coalesce(paid.last_15_paid_20per_flag, 0) as paid_20per_in_last_15d,
        coalesce(n15.has_recent_dunning_or_lrn, 0) as has_recent_dunning_or_lrn,
        coalesce(never.has_other_notice_before_cutoff, 0) as has_other_notice_before_cutoff,
        coalesce(avoid.has_avoid_notice_before_cutoff, 0) as has_avoid_notice_before_cutoff
    from base b
    left join (
        select
            loan_id,
            cutoff_date,
            actual_dpd,
            loan_status
        from (
            select
                b.loan_id,
                b.cutoff_date,
                r.actual_dpd,
                r.loan_status,
                row_number() over (
                    partition by b.loan_id, b.cutoff_date
                    order by r.collect_date desc
                ) as rn
            from base b
            join analytics.log.collections_repayments_basefact r
                on r.loan_id = b.loan_id
               and r.collect_date < b.cutoff_date
               and r.collect_date >= dateadd(day, -15, b.cutoff_date)
        ) t
        where rn = 1
    ) s
        on b.loan_id = s.loan_id
       and b.cutoff_date = s.cutoff_date
    left join (
        select
            loan_id,
            cutoff_date,
            max(paid_20per_flag_day) as last_15_paid_20per_flag
        from repayment_day_features
        where collect_date >= dateadd(day, -15, cutoff_date)
        group by loan_id, cutoff_date
    ) paid
        on b.loan_id = paid.loan_id
       and b.cutoff_date = paid.cutoff_date
    left join (
        select
            loan_id,
            cutoff_date,
            1 as has_recent_dunning_or_lrn
        from notice_history
        where notice_date >= dateadd(day, -15, cutoff_date)
          and notice_type in ('DUNNING', 'LRN')
        group by loan_id, cutoff_date
    ) n15
        on b.loan_id = n15.loan_id
       and b.cutoff_date = n15.cutoff_date
    left join (
        select
            b.loan_id,
            b.cutoff_date,
            1 as has_other_notice_before_cutoff
        from base b
        join notice_sent_log n
            on n.loan_id = b.loan_id
           and n.issuance_date < b.cutoff_date
           and n.notice_type = 'OTHERS'
        group by b.loan_id, b.cutoff_date
    ) never
        on b.loan_id = never.loan_id
       and b.cutoff_date = never.cutoff_date
    left join (
        select
            b.loan_id,
            b.cutoff_date,
            1 as has_avoid_notice_before_cutoff
        from base b
        join notice_sent_log n
            on n.loan_id = b.loan_id
           and n.issuance_date < b.cutoff_date
           and n.notice_type = 'AVOID'
        group by b.loan_id, b.cutoff_date
    ) avoid
        on b.loan_id = avoid.loan_id
       and b.cutoff_date = avoid.cutoff_date
),

candidate_flags as (
    select
        user_id,
        loan_id,
        cutoff_date,
        case
            when latest_loan_status = 'ACTIVE'
             and latest_actual_dpd between 15 and 29
             and has_recent_dunning_or_lrn = 0
             and has_other_notice_before_cutoff = 0
             and has_avoid_notice_before_cutoff = 0
             and paid_20per_in_last_15d = 0
            then 1 else 0
        end as is_dunning_candidate_asof_cutoff,
        case
            when latest_loan_status = 'ACTIVE'
             and latest_actual_dpd >= 30
             and has_recent_dunning_or_lrn = 0
             and has_other_notice_before_cutoff = 0
             and has_avoid_notice_before_cutoff = 0
             and paid_20per_in_last_15d = 0
            then 1 else 0
        end as is_lrn_candidate_asof_cutoff
    from asof_cutoff_state
)

select
    b.user_id,
    b.cutoff_date,

    sum(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then 1 else 0 end) as num_repayment_obs_days_1_to_7_d,
    sum(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then 1 else 0 end) as num_repayment_obs_days_8_to_15_d,
    sum(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then 1 else 0 end) as num_repayment_obs_days_16_to_30_d,

    avg(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then rdf.avg_actual_dpd end) as avg_actual_dpd_1_to_7_d,
    avg(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then rdf.avg_actual_dpd end) as avg_actual_dpd_8_to_15_d,
    avg(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then rdf.avg_actual_dpd end) as avg_actual_dpd_16_to_30_d,

    max(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then rdf.max_actual_dpd end) as max_actual_dpd_1_to_7_d,
    max(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then rdf.max_actual_dpd end) as max_actual_dpd_8_to_15_d,
    max(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then rdf.max_actual_dpd end) as max_actual_dpd_16_to_30_d,

    sum(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then rdf.is_active_dpd_15_plus_day else 0 end) as num_active_dpd_15_plus_days_1_to_7_d,
    sum(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then rdf.is_active_dpd_15_plus_day else 0 end) as num_active_dpd_15_plus_days_8_to_15_d,
    sum(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then rdf.is_active_dpd_15_plus_day else 0 end) as num_active_dpd_15_plus_days_16_to_30_d,

    sum(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then rdf.is_active_dpd_30_plus_day else 0 end) as num_active_dpd_30_plus_days_1_to_7_d,
    sum(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then rdf.is_active_dpd_30_plus_day else 0 end) as num_active_dpd_30_plus_days_8_to_15_d,
    sum(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then rdf.is_active_dpd_30_plus_day else 0 end) as num_active_dpd_30_plus_days_16_to_30_d,

    sum(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then rdf.total_overdue_amount else 0 end) as total_overdue_amount_1_to_7_d,
    sum(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then rdf.total_overdue_amount else 0 end) as total_overdue_amount_8_to_15_d,
    sum(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then rdf.total_overdue_amount else 0 end) as total_overdue_amount_16_to_30_d,

    sum(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then rdf.total_collected_amount else 0 end) as total_collected_amount_1_to_7_d,
    sum(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then rdf.total_collected_amount else 0 end) as total_collected_amount_8_to_15_d,
    sum(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then rdf.total_collected_amount else 0 end) as total_collected_amount_16_to_30_d,

    sum(case when rdf.collect_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then rdf.paid_20per_flag_day else 0 end) as num_paid_20per_days_1_to_7_d,
    sum(case when rdf.collect_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then rdf.paid_20per_flag_day else 0 end) as num_paid_20per_days_8_to_15_d,
    sum(case when rdf.collect_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then rdf.paid_20per_flag_day else 0 end) as num_paid_20per_days_16_to_30_d,

    sum(case when ndf.notice_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then ndf.num_legal_notices else 0 end) as num_legal_notices_1_to_7_d,
    sum(case when ndf.notice_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then ndf.num_legal_notices else 0 end) as num_legal_notices_8_to_15_d,
    sum(case when ndf.notice_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then ndf.num_legal_notices else 0 end) as num_legal_notices_16_to_30_d,

    sum(case when ndf.notice_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then ndf.num_dunning_notices else 0 end) as num_dunning_notices_1_to_7_d,
    sum(case when ndf.notice_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then ndf.num_dunning_notices else 0 end) as num_dunning_notices_8_to_15_d,
    sum(case when ndf.notice_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then ndf.num_dunning_notices else 0 end) as num_dunning_notices_16_to_30_d,

    sum(case when ndf.notice_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then ndf.num_lrn_notices else 0 end) as num_lrn_notices_1_to_7_d,
    sum(case when ndf.notice_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then ndf.num_lrn_notices else 0 end) as num_lrn_notices_8_to_15_d,
    sum(case when ndf.notice_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then ndf.num_lrn_notices else 0 end) as num_lrn_notices_16_to_30_d,

    sum(case when ndf.notice_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then ndf.num_other_notices else 0 end) as num_other_notices_1_to_7_d,
    sum(case when ndf.notice_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then ndf.num_other_notices else 0 end) as num_other_notices_8_to_15_d,
    sum(case when ndf.notice_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then ndf.num_other_notices else 0 end) as num_other_notices_16_to_30_d,

    sum(case when ndf.notice_date between dateadd(day, -7, b.cutoff_date) and dateadd(day, -1, b.cutoff_date) then ndf.num_avoid_notices else 0 end) as num_avoid_notices_1_to_7_d,
    sum(case when ndf.notice_date between dateadd(day, -15, b.cutoff_date) and dateadd(day, -8, b.cutoff_date) then ndf.num_avoid_notices else 0 end) as num_avoid_notices_8_to_15_d,
    sum(case when ndf.notice_date between dateadd(day, -30, b.cutoff_date) and dateadd(day, -16, b.cutoff_date) then ndf.num_avoid_notices else 0 end) as num_avoid_notices_16_to_30_d,

    max(cf.is_dunning_candidate_asof_cutoff) as is_dunning_candidate_asof_cutoff,
    max(cf.is_lrn_candidate_asof_cutoff) as is_lrn_candidate_asof_cutoff,
    datediff('day', max(ndf.notice_date), b.cutoff_date) as days_since_last_legal_notice_30d
from base b
left join repayment_day_features rdf
    on b.user_id = rdf.user_id
   and b.loan_id = rdf.loan_id
   and b.cutoff_date = rdf.cutoff_date
left join notice_day_features ndf
    on b.user_id = ndf.user_id
   and b.loan_id = ndf.loan_id
   and b.cutoff_date = ndf.cutoff_date
left join candidate_flags cf
    on b.user_id = cf.user_id
   and b.loan_id = cf.loan_id
   and b.cutoff_date = cf.cutoff_date
group by b.user_id, b.cutoff_date;
