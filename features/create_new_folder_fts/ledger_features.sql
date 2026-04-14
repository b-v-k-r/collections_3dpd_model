set base_tbl = 'analytics.data_science.field_disposition_base';

create or replace table analytics.data_science.all_ledger_features_for_early_dpd2 as
with bre_base as (
    select user_id, cutoff_date
    from identifier($base_tbl)
    where user_id is not null
      and cutoff_date is not null
),
raw_txns as (
    select
        b.owner_id as user_id,
        ts.customer_id,
        ts.transaction_id,
        ts.amount,
        len(ts.description) as description_length,
        to_timestamp_ntz(ts.created_at::bigint / 1000) as trxn_time,
        cast(ts.create_date as date) as txn_date,
        bb.cutoff_date
    from analytics.kb_curated.transactions_snap ts
    join analytics.kb_curated.books_snap b
      on ts.book_id = b.book_id
    join bre_base bb
      on b.owner_id = bb.user_id
     and cast(ts.create_date as date) between dateadd(day, -30, bb.cutoff_date)
                                         and dateadd(day, -1, bb.cutoff_date)
    where ts.deleted = 0
      and ts.amount <> 0
),
ranked_txns as (
    select
        user_id,
        cutoff_date,
        customer_id,
        transaction_id,
        amount,
        description_length,
        trxn_time,
        row_number() over (
            partition by user_id, customer_id, cutoff_date
            order by trxn_time, transaction_id
        ) as rk
    from raw_txns
),
ledger_txns as (
    select
        user_id,
        cutoff_date,
        cast(trxn_time as date) as dt,
        description_length,
        case when amount > 0 then amount else 0 end as credit_amount,
        case when amount < 0 then abs(amount) else 0 end as debit_amount,
        sum(amount) over (
            partition by user_id, customer_id, cutoff_date
            order by rk
            rows between unbounded preceding and current row
        ) as available_balance
    from ranked_txns
),
day_features as (
    select
        user_id,
        cutoff_date,
        dt,
        count(*) as num_total_trxns,
        sum(case when description_length is not null then 1 else 0 end) as num_trxns_with_description,
        sum(case when description_length = 0 then 1 else 0 end) as num_trxns_with_description_length_0,
        sum(case when credit_amount > 0 then 1 else 0 end) as num_credit_txns,
        sum(case when debit_amount > 0 then 1 else 0 end) as num_debit_txns,
        sum(credit_amount) as total_credit_amount,
        sum(debit_amount) as total_debit_amount,
        avg(nullif(credit_amount, 0)) as avg_credit_amount,
        avg(nullif(debit_amount, 0)) as avg_debit_amount,
        min(available_balance) as min_available_balance,
        max(available_balance) as max_available_balance,
        avg(available_balance) as avg_available_balance
    from ledger_txns
    group by user_id, cutoff_date, dt
)
select
    user_id,
    cutoff_date,

    sum(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then num_total_trxns else 0 end) as num_total_trxns_1_to_7_d,
    sum(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then num_total_trxns else 0 end) as num_total_trxns_8_to_15_d,
    sum(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_total_trxns else 0 end) as num_total_trxns_16_to_30_d,

    sum(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then num_credit_txns else 0 end) as num_credit_txns_1_to_7_d,
    sum(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then num_credit_txns else 0 end) as num_credit_txns_8_to_15_d,
    sum(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_credit_txns else 0 end) as num_credit_txns_16_to_30_d,

    sum(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then num_debit_txns else 0 end) as num_debit_txns_1_to_7_d,
    sum(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then num_debit_txns else 0 end) as num_debit_txns_8_to_15_d,
    sum(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_debit_txns else 0 end) as num_debit_txns_16_to_30_d,

    sum(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then total_credit_amount else 0 end) as total_credit_amount_1_to_7_d,
    sum(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then total_credit_amount else 0 end) as total_credit_amount_8_to_15_d,
    sum(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_credit_amount else 0 end) as total_credit_amount_16_to_30_d,

    sum(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then total_debit_amount else 0 end) as total_debit_amount_1_to_7_d,
    sum(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then total_debit_amount else 0 end) as total_debit_amount_8_to_15_d,
    sum(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then total_debit_amount else 0 end) as total_debit_amount_16_to_30_d,

    avg(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then avg_credit_amount end) as avg_credit_amount_1_to_7_d,
    avg(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then avg_credit_amount end) as avg_credit_amount_8_to_15_d,
    avg(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then avg_credit_amount end) as avg_credit_amount_16_to_30_d,

    avg(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then avg_debit_amount end) as avg_debit_amount_1_to_7_d,
    avg(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then avg_debit_amount end) as avg_debit_amount_8_to_15_d,
    avg(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then avg_debit_amount end) as avg_debit_amount_16_to_30_d,

    min(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then min_available_balance end) as min_available_balance_1_to_7_d,
    min(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then min_available_balance end) as min_available_balance_8_to_15_d,
    min(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then min_available_balance end) as min_available_balance_16_to_30_d,

    max(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then max_available_balance end) as max_available_balance_1_to_7_d,
    max(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then max_available_balance end) as max_available_balance_8_to_15_d,
    max(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then max_available_balance end) as max_available_balance_16_to_30_d,

    avg(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then avg_available_balance end) as avg_available_balance_1_to_7_d,
    avg(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then avg_available_balance end) as avg_available_balance_8_to_15_d,
    avg(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then avg_available_balance end) as avg_available_balance_16_to_30_d,

    sum(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then num_trxns_with_description else 0 end) as num_trxns_with_description_1_to_7_d,
    sum(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then num_trxns_with_description else 0 end) as num_trxns_with_description_8_to_15_d,
    sum(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_trxns_with_description else 0 end) as num_trxns_with_description_16_to_30_d,

    sum(case when dt between dateadd(day, -7, cutoff_date) and dateadd(day, -1, cutoff_date) then num_trxns_with_description_length_0 else 0 end) as num_trxns_with_description_length_0_1_to_7_d,
    sum(case when dt between dateadd(day, -15, cutoff_date) and dateadd(day, -8, cutoff_date) then num_trxns_with_description_length_0 else 0 end) as num_trxns_with_description_length_0_8_to_15_d,
    sum(case when dt between dateadd(day, -30, cutoff_date) and dateadd(day, -16, cutoff_date) then num_trxns_with_description_length_0 else 0 end) as num_trxns_with_description_length_0_16_to_30_d
from day_features
group by user_id, cutoff_date;
