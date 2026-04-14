-- select * from ANALYTICS.DATA_SCIENCE.DATA_early_dpd3_BASE_FINAL limit 5;

-- 1) Define the base table once
set base_tbl = 'analytics.data_science.field_disposition_base';



---=============
-----       --  - - 1.  DEBIT CREDIT RATIO FEATURES  =========================
--==========

create or replace transient table analytics.data_science.data_early_dpd3_features_debit_credit_ratio as
with base as (
  select
    a.user_id,
    a.cutoff_date,

    -- Credit Transaction Values by time windows
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 1 and 2 and amount > 0 then amount else 0 end) as cr_txu_value_last_1_2_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 3 and 4 and amount > 0 then amount else 0 end) as cr_txu_value_last_3_4_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 5 and 6 and amount > 0 then amount else 0 end) as cr_txu_value_last_5_6_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 7 and 8 and amount > 0 then amount else 0 end) as cr_txu_value_last_7_8_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 9 and 11 and amount > 0 then amount else 0 end) as cr_txu_value_last_9_11_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 12 and 14 and amount > 0 then amount else 0 end) as cr_txu_value_last_12_14_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 1 and 7 and amount > 0 then amount else 0 end) as cr_txu_value_last_1_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 8 and 14 and amount > 0 then amount else 0 end) as cr_txu_value_last_2_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 15 and 21 and amount > 0 then amount else 0 end) as cr_txu_value_last_3_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 22 and 30 and amount > 0 then amount else 0 end) as cr_txu_value_last_4_week,

    -- Debit Transaction Values by time windows
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 1 and 2 and amount < 0 then amount else 0 end) as db_txu_value_last_1_2_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 3 and 4 and amount < 0 then amount else 0 end) as db_txu_value_last_3_4_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 5 and 6 and amount < 0 then amount else 0 end) as db_txu_value_last_5_6_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 7 and 8 and amount < 0 then amount else 0 end) as db_txu_value_last_7_8_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 9 and 11 and amount < 0 then amount else 0 end) as db_txu_value_last_9_11_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 12 and 14 and amount < 0 then amount else 0 end) as db_txu_value_last_12_14_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 1 and 7 and amount < 0 then amount else 0 end) as db_txu_value_last_1_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 8 and 14 and amount < 0 then amount else 0 end) as db_txu_value_last_2_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 15 and 21 and amount < 0 then amount else 0 end) as db_txu_value_last_3_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 22 and 30 and amount < 0 then amount else 0 end) as db_txu_value_last_4_week,

    -- Total Transaction Values by time windows
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 1 and 2 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_1_2_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 3 and 4 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_3_4_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 5 and 6 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_5_6_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 7 and 8 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_7_8_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 9 and 11 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_9_11_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 12 and 14 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_12_14_day,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 1 and 7 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_1_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 8 and 14 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_2_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 15 and 21 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_3_week,
    sum(case when datediff('day', b.create_date, a.cutoff_date) between 22 and 30 and amount <> 0 then abs(amount) else 0 end) as txu_value_last_4_week,

    -- Transaction Days by time windows
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 1 and 2 and amount <> 0 then b.create_date end) as txng_days_last_1_2_day,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 3 and 4 and amount <> 0 then b.create_date end) as txng_days_last_3_4_day,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 5 and 6 and amount <> 0 then b.create_date end) as txng_days_last_5_6_day,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 7 and 8 and amount <> 0 then b.create_date end) as txng_days_last_7_8_day,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 9 and 11 and amount <> 0 then b.create_date end) as txng_days_last_9_11_day,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 12 and 14 and amount <> 0 then b.create_date end) as txng_days_last_12_14_day,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 1 and 7 and amount <> 0 then b.create_date end) as txng_days_last_1_week,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 8 and 14 and amount <> 0 then b.create_date end) as txng_days_last_2_week,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 15 and 21 and amount <> 0 then b.create_date end) as txng_days_last_3_week,
    count(distinct case when datediff('day', b.create_date, a.cutoff_date) between 22 and 30 and amount <> 0 then b.create_date end) as txng_days_last_4_week

  from identifier($base_tbl) a
  left join analytics.kb_curated.transactions_snap b
    on a.user_id = b.created_by_user
   and b.created_at_ts::date < a.cutoff_date
   and b.created_at_ts::date >= dateadd('day', -30, a.cutoff_date)
   and deleted <> 1
  group by 1,2
)
select
  user_id,
  cutoff_date,

  cr_txu_value_last_1_2_day,
  cr_txu_value_last_3_4_day,
  cr_txu_value_last_5_6_day,
  cr_txu_value_last_7_8_day,
  cr_txu_value_last_9_11_day,
  cr_txu_value_last_12_14_day,
  cr_txu_value_last_1_week,
  cr_txu_value_last_2_week,
  cr_txu_value_last_3_week,
  cr_txu_value_last_4_week,

  db_txu_value_last_1_2_day,
  db_txu_value_last_3_4_day,
  db_txu_value_last_5_6_day,
  db_txu_value_last_7_8_day,
  db_txu_value_last_9_11_day,
  db_txu_value_last_12_14_day,
  db_txu_value_last_1_week,
  db_txu_value_last_2_week,
  db_txu_value_last_3_week,
  db_txu_value_last_4_week,

  txu_value_last_1_2_day,
  txu_value_last_3_4_day,
  txu_value_last_5_6_day,
  txu_value_last_7_8_day,
  txu_value_last_9_11_day,
  txu_value_last_12_14_day,
  txu_value_last_1_week,
  txu_value_last_2_week,
  txu_value_last_3_week,
  txu_value_last_4_week,

  txng_days_last_1_2_day,
  txng_days_last_3_4_day,
  txng_days_last_5_6_day,
  txng_days_last_7_8_day,
  txng_days_last_9_11_day,
  txng_days_last_12_14_day,
  txng_days_last_1_week,
  txng_days_last_2_week,
  txng_days_last_3_week,
  txng_days_last_4_week,

  db_txu_value_last_1_2_day / nullif(cr_txu_value_last_1_2_day, 0)::float as db_cr_ratio_last_1_2_day,
  db_txu_value_last_3_4_day / nullif(cr_txu_value_last_3_4_day, 0)::float as db_cr_ratio_last_3_4_day,
  db_txu_value_last_5_6_day / nullif(cr_txu_value_last_5_6_day, 0)::float as db_cr_ratio_last_5_6_day,
  db_txu_value_last_7_8_day / nullif(cr_txu_value_last_7_8_day, 0)::float as db_cr_ratio_last_7_8_day,
  db_txu_value_last_9_11_day / nullif(cr_txu_value_last_9_11_day, 0)::float as db_cr_ratio_last_9_11_day,
  db_txu_value_last_12_14_day / nullif(cr_txu_value_last_12_14_day, 0)::float as db_cr_ratio_last_12_14_day,
  db_txu_value_last_1_week / nullif(cr_txu_value_last_1_week, 0)::float as db_cr_ratio_last_1_week,
  db_txu_value_last_2_week / nullif(cr_txu_value_last_2_week, 0)::float as db_cr_ratio_last_2_week,
  db_txu_value_last_3_week / nullif(cr_txu_value_last_3_week, 0)::float as db_cr_ratio_last_3_week,
  db_txu_value_last_4_week / nullif(cr_txu_value_last_4_week, 0)::float as db_cr_ratio_last_4_week
from base;

-- select * from analytics.data_science.data_early_dpd3_features_debit_credit_ratio;

---===========
---------  2) ACTIVE USER FEATUERS ---  -   --  -   -   -   -   -
---===========

create or replace transient table analytics.data_science.data_early_dpd3_features_active_users as (
Select  a.USER_ID, a.cutoff_date
    -- App Open Counts by time windows
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 2 then b.event_time end) as app_open_cnt_last_1_2_day
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 3 and 4 then b.event_time end) as app_open_cnt_last_3_4_day
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 5 and 6 then b.event_time end) as app_open_cnt_last_5_6_day
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 7 and 8 then b.event_time end) as app_open_cnt_last_7_8_day
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 9 and 11 then b.event_time end) as app_open_cnt_last_9_11_day
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 12 and 14 then b.event_time end) as app_open_cnt_last_12_14_day
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 7 then b.event_time end) as app_open_cnt_last_1_week
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 8 and 14 then b.event_time end) as app_open_cnt_last_2_week
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 15 and 21 then b.event_time end) as app_open_cnt_last_3_week
    ,count(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 22 and 30 then b.event_time end) as app_open_cnt_last_4_week

    -- App Open Days by time windows
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 2 then b.event_dt_ist end) as app_open_days_last_1_2_day
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 3 and 4 then b.event_dt_ist end) as app_open_days_last_3_4_day
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 5 and 6 then b.event_dt_ist end) as app_open_days_last_5_6_day
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 7 and 8 then b.event_dt_ist end) as app_open_days_last_7_8_day
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 9 and 11 then b.event_dt_ist end) as app_open_days_last_9_11_day
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 12 and 14 then b.event_dt_ist end) as app_open_days_last_12_14_day
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 7 then b.event_dt_ist end) as app_open_days_last_1_week
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 8 and 14 then b.event_dt_ist end) as app_open_days_last_2_week
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 15 and 21 then b.event_dt_ist end) as app_open_days_last_3_week
    ,count(distinct case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 22 and 30 then b.event_dt_ist end) as app_open_days_last_4_week

from identifier($base_tbl) a
left join (
    Select user_id, event_name, event_time,
        dateadd('minute',330, event_time) event_time_ist,
        event_time_ist::date event_dt_ist,
        user_properties
    from analytics.kb_curated.amplitude_raw_logs_new_unbounded
    where event_name in ('Application Opened')
) b
    on a.USER_ID = b.user_id
    and b.event_dt_ist < a.CUTOFF_DATE
    and b.event_dt_ist >= dateadd('day',-30, a.CUTOFF_DATE)
group by 1,2
);



------ 3) OTHER FEATUERS -- -   --  --  --      -   -       -
create or replace transient table analytics.data_science.data_early_dpd3_features_other_features as (
Select  a.USER_ID, a.CUTOFF_DATE
    -- Explored Other Features by time windows
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 2
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_1_2_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 3 and 4
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_3_4_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 5 and 6
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_5_6_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 7 and 8
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_7_8_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 9 and 11
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_9_11_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 12 and 14
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_12_14_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 7
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_1_week
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 8 and 14
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_2_week
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 15 and 21
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_3_week
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 22 and 30
        and event_name in ('MainBillsClick','MainInventoryClick','HomeCashBookClick',
            'BookProfileCashRegisterClick','CustomerKhataCashbookClick')
        then 1 else 0 end) as Explored_other_features_last_4_week

    -- Used Other Features by time windows
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 2
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_1_2_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 3 and 4
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_3_4_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 5 and 6
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_5_6_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 7 and 8
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_7_8_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 9 and 11
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_9_11_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 12 and 14
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_12_14_day
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 1 and 7
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_1_week
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 8 and 14
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_2_week
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 15 and 21
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_3_week
    ,max(case when datediff('day',event_dt_ist,a.CUTOFF_DATE) between 22 and 30
        and event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
            'InventoryManagementSaveItemClick','BillsInvoiceSaveClicked','CashRegisterSaveClick')
        then 1 else 0 end) as Used_other_features_last_4_week

from identifier($base_tbl) a
left join (
    Select user_id, event_name, event_time,
        dateadd('minute',330, event_time) event_time_ist,
        event_time_ist::date event_dt_ist,
        user_properties
    from analytics.kb_curated.amplitude_raw_logs_new_unbounded
    where event_name in ('InventoryManagementStockInEntryClick','InventoryManagementStockOutEntryClick',
        'InventoryManagementSaveItemClick','MainBillsClick','BillsInvoiceSaveClicked','MainInventoryClick',
        'HomeCashBookClick','BookProfileCashRegisterClick','CustomerKhataCashbookClick','CashRegisterSaveClick')
) b
    on a.USER_ID = b.user_id
    and b.event_dt_ist < a.CUTOFF_DATE
    and b.event_dt_ist >= dateadd('day',-30, a.CUTOFF_DATE)
group by 1,2
);



----  4) CUSTOMER ADDITON       -   --  -   -   -   --  -
create or replace transient table analytics.data_science.data_early_dpd3_features_customer_addition as (
with cx as (
    Select created_by_user user_id, phone added_cx_ph, min(create_date) first_addition_dt
    from ANALYTICS.KB_CURATED.CUSTOMERS_SNAP c
    group by 1,2
)
Select  a.USER_ID, a.CUTOFF_DATE
    -- Customer Added Count by time windows
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 1 and 2 then c.added_cx_ph end) cx_added_cnt_last_1_2_day
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 3 and 4 then c.added_cx_ph end) cx_added_cnt_last_3_4_day
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 5 and 6 then c.added_cx_ph end) cx_added_cnt_last_5_6_day
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 7 and 8 then c.added_cx_ph end) cx_added_cnt_last_7_8_day
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 9 and 11 then c.added_cx_ph end) cx_added_cnt_last_9_11_day
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 12 and 14 then c.added_cx_ph end) cx_added_cnt_last_12_14_day
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 1 and 7 then c.added_cx_ph end) cx_added_cnt_last_1_week
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 8 and 14 then c.added_cx_ph end) cx_added_cnt_last_2_week
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 15 and 21 then c.added_cx_ph end) cx_added_cnt_last_3_week
    ,count(case when datediff('day',first_addition_dt,a.CUTOFF_DATE) between 22 and 30 then c.added_cx_ph end) cx_added_cnt_last_4_week
    ,count(c.added_cx_ph) cx_added_till_date

from identifier($base_tbl) a
left join cx c
    on a.USER_ID = c.user_id
    and datediff('day',c.first_addition_dt, a.CUTOFF_DATE) between 1 and 30
group by 1,2
);



------  5) LOAN COMMUNICAITON FEATUERS ____
create or replace transient table analytics.data_science.data_early_dpd3_features_loan_communications as (
with upload_date as (
    Select user_id, upload_date::date upload_dt
    from (
        select a.*, b.upload_date
        from
            (select user_id, lead_type, marketing_stage, renewal_flag, version_above_606000
            from analytics.model.lending_marketing_totf_basefact) a
            left join
            (select user_id, FIRST_WHITELISTED_DATE upload_date
             from analytics.model.whitelisted_user_base) b
            on a.user_id = b.user_id
    )
    where (1=1)
        and upload_date::date >= '2023-01-01'
        and user_id not in (
            select user_id from (
                Select u.user_id, u.meta:tags as tags, u.meta:tags[0]:tag tag_d
                from APP_BACKEND.LOAN_SERVICE_PROD.PUBLIC_USERS_VW u
                where u.meta:tags[0]:tag is not null
                and tags ilike '%qr%'
            )
        )
)
,campaign_journey_internal as (
    SELECT A.CAMPAIGN_NAME, A.PHONE, A.event_time,
        A.event_time::date as unq_date,
        A.CHANNEL, A.CAMPAIGN_ID, A.CAMPAIGN_TYPE,
        A.EVENT_NAME, A.COMPLETE_EVENT,
        B.JOURNEY_NAME,
        a.event_time::date event_dt
    FROM analytics.kb_analytics.clevertap_kb A
    LEFT JOIN (
        Select distinct journey_name, campaign_id
        from analytics.kb_analytics.clevertap_kb_journey
    ) B
        ON to_varchar(B.CAMPAIGN_ID)=A.CAMPAIGN_ID
)
,lc as (
    Select b.USER_ID, b.CUTOFF_DATE
        -- Loan Communications by time windows
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 1 and 2
            then a.event_time end) loan_comm_cnt_last_1_2_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 3 and 4
            then a.event_time end) loan_comm_cnt_last_3_4_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 5 and 6
            then a.event_time end) loan_comm_cnt_last_5_6_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 7 and 8
            then a.event_time end) loan_comm_cnt_last_7_8_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 9 and 11
            then a.event_time end) loan_comm_cnt_last_9_11_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 12 and 14
            then a.event_time end) loan_comm_cnt_last_12_14_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 1 and 7
            then a.event_time end) loan_comm_cnt_last_1_week
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 8 and 14
            then a.event_time end) loan_comm_cnt_last_2_week
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 15 and 21
            then a.event_time end) loan_comm_cnt_last_3_week
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 22 and 30
            then a.event_time end) loan_comm_cnt_last_4_week

        -- Loan WhatsApp Communications by time windows
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 1 and 2
            then a.event_time end) loan_WA_comm_cnt_last_1_2_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 3 and 4
            then a.event_time end) loan_WA_comm_cnt_last_3_4_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 5 and 6
            then a.event_time end) loan_WA_comm_cnt_last_5_6_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 7 and 8
            then a.event_time end) loan_WA_comm_cnt_last_7_8_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 9 and 11
            then a.event_time end) loan_WA_comm_cnt_last_9_11_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 12 and 14
            then a.event_time end) loan_WA_comm_cnt_last_12_14_day
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 1 and 7
            then a.event_time end) loan_WA_comm_cnt_last_1_week
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 8 and 14
            then a.event_time end) loan_WA_comm_cnt_last_2_week
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 15 and 21
            then a.event_time end) loan_WA_comm_cnt_last_3_week
        ,count(distinct case when (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
            and coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp'
            and datediff('day',event_time::date,b.CUTOFF_DATE) between 22 and 30
            then a.event_time end) loan_WA_comm_cnt_last_4_week

    from identifier($base_tbl) b
    inner join upload_date u on b.USER_ID = u.user_id
    left join (
        Select kb_id user_id, event_name, event_time, a.channel,
            journey_name, campaign_name, campaign_type
        from campaign_journey_internal a
        inner join (
            select kb_id, phone ph
            from ANALYTICS.MODEL.USER_BASE_FACT_LATEST
        ) u
            on right(replace(replace(a.phone,'+91'),' '),10) = right(replace(replace(u.ph,'+91'),' '),10)
        where ((coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%InApp%' and EVENT_NAME='Notification Viewed')
            OR (coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Push%' and EVENT_NAME='Push Impressions')
            OR (coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%SMS%' and EVENT_NAME='Notification Sent')
            OR (coalesce(CHANNEL,CAMPAIGN_TYPE) ilike '%Whatsapp' and EVENT_NAME='Notification Sent'))
            and unq_date::date >= '2022-09-01'
            and (journey_name ilike '%loan%' or campaign_name ilike '%loan%')
    ) a
        on b.USER_ID = a.user_id
        and a.event_time::date >= u.upload_dt
        and a.event_time::date < b.CUTOFF_DATE
        and datediff('day',a.event_time::date, b.CUTOFF_DATE) between 0 and 30
    group by 1,2
)
Select * from lc
);





----- 6) LOAN EXPLORED CHANNEL FEATURE ==================================

create or replace transient table analytics.data_science.data_early_dpd3_features_loan_explored_channel as (
with upload_date as (
    Select user_id, upload_date::date upload_dt
    from (
        select a.*, b.upload_date
        from
            (select user_id, lead_type, marketing_stage, renewal_flag, version_above_606000
            from analytics.model.lending_marketing_totf_basefact) a
            left join
            (select user_id, FIRST_WHITELISTED_DATE upload_date
             from analytics.model.whitelisted_user_base) b
            on a.user_id = b.user_id
    )
    where (1=1)
        and upload_date::date >= '2023-01-01'
        and user_id not in (
            select user_id from (
                Select u.user_id, u.meta:tags as tags, u.meta:tags[0]:tag tag_d
                from APP_BACKEND.LOAN_SERVICE_PROD.PUBLIC_USERS_VW u
                where u.meta:tags[0]:tag is not null
                and tags ilike '%qr%'
            )
        )
)
,le as (
    Select  b.USER_ID, b.CUTOFF_DATE
        -- Overall Loan Explored metrics by time windows
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 1 and 2 then a.event_time_ist end) loan_explored_cnt_last_1_2_day
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 3 and 4 then a.event_time_ist end) loan_explored_cnt_last_3_4_day
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 5 and 6 then a.event_time_ist end) loan_explored_cnt_last_5_6_day
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 7 and 8 then a.event_time_ist end) loan_explored_cnt_last_7_8_day
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 9 and 11 then a.event_time_ist end) loan_explored_cnt_last_9_11_day
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 12 and 14 then a.event_time_ist end) loan_explored_cnt_last_12_14_day
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 1 and 7 then a.event_time_ist end) loan_explored_cnt_last_1_week
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 8 and 14 then a.event_time_ist end) loan_explored_cnt_last_2_week
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 15 and 21 then a.event_time_ist end) loan_explored_cnt_last_3_week
        ,count(distinct case when datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 22 and 30 then a.event_time_ist end) loan_explored_cnt_last_4_week

        -- Marketing Loan Explored metrics by time windows
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 1 and 2
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_1_2_day
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 3 and 4
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_3_4_day
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 5 and 6
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_5_6_day
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 7 and 8
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_7_8_day
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 9 and 11
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_9_11_day
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 12 and 14
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_12_14_day
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 1 and 7
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_1_week
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 8 and 14
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_2_week
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 15 and 21
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_3_week
        ,count(distinct case when EVENT_NAME in ('Background Event : Deep Linking')
            and event_properties:ScreenName::text = 'KhataLoan'
            and len(event_properties:src::text)>2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 22 and 30
            then a.event_time_ist end) mtkng_loan_explored_cnt_last_4_week

        -- Product Loan Explored metrics by time windows
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 1 and 2
            then a.event_time_ist end) product_loan_explored_cnt_last_1_2_day
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 3 and 4
            then a.event_time_ist end) product_loan_explored_cnt_last_3_4_day
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 5 and 6
            then a.event_time_ist end) product_loan_explored_cnt_last_5_6_day
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 7 and 8
            then a.event_time_ist end) product_loan_explored_cnt_last_7_8_day
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 9 and 11
            then a.event_time_ist end) product_loan_explored_cnt_last_9_11_day
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 12 and 14
            then a.event_time_ist end) product_loan_explored_cnt_last_12_14_day
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 1 and 7
            then a.event_time_ist end) product_loan_explored_cnt_last_1_week
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 8 and 14
            then a.event_time_ist end) product_loan_explored_cnt_last_2_week
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 15 and 21
            then a.event_time_ist end) product_loan_explored_cnt_last_3_week
        ,count(distinct case when EVENT_NAME not in ('Background Event : Deep Linking')
            or event_properties:ScreenName::text != 'KhataLoan'
            or len(event_properties:src::text)<=2
            and datediff('day',a.event_time_ist::date,b.CUTOFF_DATE) between 22 and 30
            then a.event_time_ist end) product_loan_explored_cnt_last_4_week

    from identifier($base_tbl) b
    inner join upload_date u on b.USER_ID = u.user_id
    left join (
        Select user_id, event_name, event_time,
            dateadd('minute',330, event_time) event_time_ist,
            event_time_ist::date event_dt_ist,
            user_properties, event_properties
        from analytics.kb_curated.amplitude_raw_logs_new
        where ((EVENT_NAME in ('BookProfileApplyLoanClick','CustomerKhataMicroBannerLoanClick',
                'CreditScoreApplyLoanClick')
            OR (EVENT_NAME in ('Background Event : Deep Linking')
                and event_properties:ScreenName::text = 'KhataLoan')
            or (event_name in ('AdvertisementOnAdClick')
                and event_properties:campaignId::text = 'loan_khata_18_may_2022_campaign')
            or (event_name in ('MoneyWidgetBannerClick')
                and event_properties:banner::text ilike '%loan%')
            OR (EVENT_NAME = 'BannerWidgetClicked'
                and event_properties:JourneyType::string = 'LOAN_BANNER'
                and event_properties:state::string = 'NO_APPLICATION')
        ))
    ) a
        on b.USER_ID = a.user_id
        and a.event_time_ist::date >= u.upload_dt
        and a.event_time_ist::date < b.CUTOFF_DATE
        and datediff('day',a.event_time_ist::date, b.CUTOFF_DATE) <= 30
    group by 1,2
)
Select * from le
);



--------  7) PAYMENT COLLECION REMINDER ========

create or replace transient table analytics.data_science.data_early_dpd3_features_payment_collection_reminder as (
Select  a.USER_ID, a.CUTOFF_DATE
    -- Collection Reminders Sent Count by time windows
    ,count(distinct case when b.event_time between dateadd('day',-2, a.CUTOFF_DATE)
        and dateadd('day',-1, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_1_2_day
    ,count(distinct case when b.event_time between dateadd('day',-4, a.CUTOFF_DATE)
        and dateadd('day',-3, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_3_4_day
    ,count(distinct case when b.event_time between dateadd('day',-6, a.CUTOFF_DATE)
        and dateadd('day',-5, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_5_6_day
    ,count(distinct case when b.event_time between dateadd('day',-8, a.CUTOFF_DATE)
        and dateadd('day',-7, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_7_8_day
    ,count(distinct case when b.event_time between dateadd('day',-11, a.CUTOFF_DATE)
        and dateadd('day',-9, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_9_11_day
    ,count(distinct case when b.event_time between dateadd('day',-14, a.CUTOFF_DATE)
        and dateadd('day',-12, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_12_14_day
    ,count(distinct case when b.event_time between dateadd('day',-7, a.CUTOFF_DATE)
        and dateadd('day',-1, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_1_week
    ,count(distinct case when b.event_time between dateadd('day',-14, a.CUTOFF_DATE)
        and dateadd('day',-8, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_2_week
    ,count(distinct case when b.event_time between dateadd('day',-21, a.CUTOFF_DATE)
        and dateadd('day',-15, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_3_week
    ,count(distinct case when b.event_time between dateadd('day',-30, a.CUTOFF_DATE)
        and dateadd('day',-22, a.CUTOFF_DATE)
        then event_time end) collection_reminders_sent_last_4_week

    -- Collection Reminders Sent Dates Count by time windows
    ,count(distinct case when b.event_time between dateadd('day',-2, a.CUTOFF_DATE)
        and dateadd('day',-1, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_1_2_day
    ,count(distinct case when b.event_time between dateadd('day',-4, a.CUTOFF_DATE)
        and dateadd('day',-3, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_3_4_day
    ,count(distinct case when b.event_time between dateadd('day',-6, a.CUTOFF_DATE)
        and dateadd('day',-5, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_5_6_day
    ,count(distinct case when b.event_time between dateadd('day',-8, a.CUTOFF_DATE)
        and dateadd('day',-7, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_7_8_day
    ,count(distinct case when b.event_time between dateadd('day',-11, a.CUTOFF_DATE)
        and dateadd('day',-9, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_9_11_day
    ,count(distinct case when b.event_time between dateadd('day',-14, a.CUTOFF_DATE)
        and dateadd('day',-12, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_12_14_day
    ,count(distinct case when b.event_time between dateadd('day',-7, a.CUTOFF_DATE)
        and dateadd('day',-1, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_1_week
    ,count(distinct case when b.event_time between dateadd('day',-14, a.CUTOFF_DATE)
        and dateadd('day',-8, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_2_week
    ,count(distinct case when b.event_time between dateadd('day',-21, a.CUTOFF_DATE)
        and dateadd('day',-15, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_3_week
    ,count(distinct case when b.event_time between dateadd('day',-30, a.CUTOFF_DATE)
        and dateadd('day',-22, a.CUTOFF_DATE)
        then event_time::date end) collection_reminders_sent_dts_last_4_week

from identifier($base_tbl) a
left join (
    Select user_id, event_name, event_time,
        dateadd('minute',330, event_time) event_time_ist,
        event_time_ist::date event_dt_ist,
        user_properties
    from analytics.kb_curated.amplitude_raw_logs_new
    where event_name in ('PaymentReminderShareOnSms', 'PaymentReminderShareOnWhatsApp',
        'PaymentSharePaymentLink', 'PaymentShareWithoutLink', 'BulkReminderSuccessfullySent')
) b
    on a.USER_ID = b.user_id
    and event_time_ist::date < a.CUTOFF_DATE
    and event_time_ist::date >= dateadd('day',-30, a.CUTOFF_DATE)
group by 1,2
);




---- COMPILED DATA TABLE --- JOINING PREVIOUS 7 FEATURES GROUPS INTO 1 TABLE  =========================================
create or replace transient table analytics.data_science.data_early_dpd3_features_compiled_data as (
Select distinct
    a.USER_ID,
    a.CUTOFF_DATE

    -- Payment Collection Reminder Features
    ,b.collection_reminders_sent_last_1_2_day
    ,b.collection_reminders_sent_last_3_4_day
    ,b.collection_reminders_sent_last_5_6_day
    ,b.collection_reminders_sent_last_7_8_day
    ,b.collection_reminders_sent_last_9_11_day
    ,b.collection_reminders_sent_last_12_14_day
    ,b.collection_reminders_sent_last_1_week
    ,b.collection_reminders_sent_last_2_week
    ,b.collection_reminders_sent_last_3_week
    ,b.collection_reminders_sent_last_4_week
    ,b.collection_reminders_sent_dts_last_1_2_day
    ,b.collection_reminders_sent_dts_last_3_4_day
    ,b.collection_reminders_sent_dts_last_5_6_day
    ,b.collection_reminders_sent_dts_last_7_8_day
    ,b.collection_reminders_sent_dts_last_9_11_day
    ,b.collection_reminders_sent_dts_last_12_14_day
    ,b.collection_reminders_sent_dts_last_1_week
    ,b.collection_reminders_sent_dts_last_2_week
    ,b.collection_reminders_sent_dts_last_3_week
    ,b.collection_reminders_sent_dts_last_4_week

    -- Debit Credit Ratio Features
    ,c.CR_TXU_VALUE_last_1_2_day
    ,c.CR_TXU_VALUE_last_3_4_day
    ,c.CR_TXU_VALUE_last_5_6_day
    ,c.CR_TXU_VALUE_last_7_8_day
    ,c.CR_TXU_VALUE_last_9_11_day
    ,c.CR_TXU_VALUE_last_12_14_day
    ,c.CR_TXU_VALUE_last_1_week
    ,c.CR_TXU_VALUE_last_2_week
    ,c.CR_TXU_VALUE_last_3_week
    ,c.CR_TXU_VALUE_last_4_week
    ,c.DB_TXU_VALUE_last_1_2_day
    ,c.DB_TXU_VALUE_last_3_4_day
    ,c.DB_TXU_VALUE_last_5_6_day
    ,c.DB_TXU_VALUE_last_7_8_day
    ,c.DB_TXU_VALUE_last_9_11_day
    ,c.DB_TXU_VALUE_last_12_14_day
    ,c.DB_TXU_VALUE_last_1_week
    ,c.DB_TXU_VALUE_last_2_week
    ,c.DB_TXU_VALUE_last_3_week
    ,c.DB_TXU_VALUE_last_4_week
    ,c.TXU_VALUE_last_1_2_day
    ,c.TXU_VALUE_last_3_4_day
    ,c.TXU_VALUE_last_5_6_day
    ,c.TXU_VALUE_last_7_8_day
    ,c.TXU_VALUE_last_9_11_day
    ,c.TXU_VALUE_last_12_14_day
    ,c.TXU_VALUE_last_1_week
    ,c.TXU_VALUE_last_2_week
    ,c.TXU_VALUE_last_3_week
    ,c.TXU_VALUE_last_4_week
    ,c.txng_days_last_1_2_day
    ,c.txng_days_last_3_4_day
    ,c.txng_days_last_5_6_day
    ,c.txng_days_last_7_8_day
    ,c.txng_days_last_9_11_day
    ,c.txng_days_last_12_14_day
    ,c.txng_days_last_1_week
    ,c.txng_days_last_2_week
    ,c.txng_days_last_3_week
    ,c.txng_days_last_4_week
    ,c.db_cr_ratio_last_1_2_day
    ,c.db_cr_ratio_last_3_4_day
    ,c.db_cr_ratio_last_5_6_day
    ,c.db_cr_ratio_last_7_8_day
    ,c.db_cr_ratio_last_9_11_day
    ,c.db_cr_ratio_last_12_14_day
    ,c.db_cr_ratio_last_1_week
    ,c.db_cr_ratio_last_2_week
    ,c.db_cr_ratio_last_3_week
    ,c.db_cr_ratio_last_4_week

    -- Loan Communications Features
    ,d.loan_comm_cnt_last_1_2_day
    ,d.loan_comm_cnt_last_3_4_day
    ,d.loan_comm_cnt_last_5_6_day
    ,d.loan_comm_cnt_last_7_8_day
    ,d.loan_comm_cnt_last_9_11_day
    ,d.loan_comm_cnt_last_12_14_day
    ,d.loan_comm_cnt_last_1_week
    ,d.loan_comm_cnt_last_2_week
    ,d.loan_comm_cnt_last_3_week
    ,d.loan_comm_cnt_last_4_week
    ,d.loan_WA_comm_cnt_last_1_2_day
    ,d.loan_WA_comm_cnt_last_3_4_day
    ,d.loan_WA_comm_cnt_last_5_6_day
    ,d.loan_WA_comm_cnt_last_7_8_day
    ,d.loan_WA_comm_cnt_last_9_11_day
    ,d.loan_WA_comm_cnt_last_12_14_day
    ,d.loan_WA_comm_cnt_last_1_week
    ,d.loan_WA_comm_cnt_last_2_week
    ,d.loan_WA_comm_cnt_last_3_week
    ,d.loan_WA_comm_cnt_last_4_week

    -- Loan Explored Channel Features
    ,e.loan_explored_cnt_last_1_2_day
    ,e.loan_explored_cnt_last_3_4_day
    ,e.loan_explored_cnt_last_5_6_day
    ,e.loan_explored_cnt_last_7_8_day
    ,e.loan_explored_cnt_last_9_11_day
    ,e.loan_explored_cnt_last_12_14_day
    ,e.loan_explored_cnt_last_1_week
    ,e.loan_explored_cnt_last_2_week
    ,e.loan_explored_cnt_last_3_week
    ,e.loan_explored_cnt_last_4_week
    ,e.mtkng_loan_explored_cnt_last_1_2_day
    ,e.mtkng_loan_explored_cnt_last_3_4_day
    ,e.mtkng_loan_explored_cnt_last_5_6_day
    ,e.mtkng_loan_explored_cnt_last_7_8_day
    ,e.mtkng_loan_explored_cnt_last_9_11_day
    ,e.mtkng_loan_explored_cnt_last_12_14_day
    ,e.mtkng_loan_explored_cnt_last_1_week
    ,e.mtkng_loan_explored_cnt_last_2_week
    ,e.mtkng_loan_explored_cnt_last_3_week
    ,e.mtkng_loan_explored_cnt_last_4_week
    ,e.product_loan_explored_cnt_last_1_2_day
    ,e.product_loan_explored_cnt_last_3_4_day
    ,e.product_loan_explored_cnt_last_5_6_day
    ,e.product_loan_explored_cnt_last_7_8_day
    ,e.product_loan_explored_cnt_last_9_11_day
    ,e.product_loan_explored_cnt_last_12_14_day
    ,e.product_loan_explored_cnt_last_1_week
    ,e.product_loan_explored_cnt_last_2_week
    ,e.product_loan_explored_cnt_last_3_week
    ,e.product_loan_explored_cnt_last_4_week

    -- Other Features
    ,g.Explored_other_features_last_1_2_day
    ,g.Explored_other_features_last_3_4_day
    ,g.Explored_other_features_last_5_6_day
    ,g.Explored_other_features_last_7_8_day
    ,g.Explored_other_features_last_9_11_day
    ,g.Explored_other_features_last_12_14_day
    ,g.Explored_other_features_last_1_week
    ,g.Explored_other_features_last_2_week
    ,g.Explored_other_features_last_3_week
    ,g.Explored_other_features_last_4_week
    ,g.Used_other_features_last_1_2_day
    ,g.Used_other_features_last_3_4_day
    ,g.Used_other_features_last_5_6_day
    ,g.Used_other_features_last_7_8_day
    ,g.Used_other_features_last_9_11_day
    ,g.Used_other_features_last_12_14_day
    ,g.Used_other_features_last_1_week
    ,g.Used_other_features_last_2_week
    ,g.Used_other_features_last_3_week
    ,g.Used_other_features_last_4_week

    -- Active Users Features
    ,h.app_open_cnt_last_1_2_day
    ,h.app_open_cnt_last_3_4_day
    ,h.app_open_cnt_last_5_6_day
    ,h.app_open_cnt_last_7_8_day
    ,h.app_open_cnt_last_9_11_day
    ,h.app_open_cnt_last_12_14_day
    ,h.app_open_cnt_last_1_week
    ,h.app_open_cnt_last_2_week
    ,h.app_open_cnt_last_3_week
    ,h.app_open_cnt_last_4_week
    ,h.app_open_days_last_1_2_day
    ,h.app_open_days_last_3_4_day
    ,h.app_open_days_last_5_6_day
    ,h.app_open_days_last_7_8_day
    ,h.app_open_days_last_9_11_day
    ,h.app_open_days_last_12_14_day
    ,h.app_open_days_last_1_week
    ,h.app_open_days_last_2_week
    ,h.app_open_days_last_3_week
    ,h.app_open_days_last_4_week

    -- User Age on App
    ,datediff('day', u.login_dt, a.CUTOFF_DATE) age_on_app

    -- Customer Addition Features
    ,cx.cx_added_cnt_last_1_2_day
    ,cx.cx_added_cnt_last_3_4_day
    ,cx.cx_added_cnt_last_5_6_day
    ,cx.cx_added_cnt_last_7_8_day
    ,cx.cx_added_cnt_last_9_11_day
    ,cx.cx_added_cnt_last_12_14_day
    ,cx.cx_added_cnt_last_1_week
    ,cx.cx_added_cnt_last_2_week
    ,cx.cx_added_cnt_last_3_week
    ,cx.cx_added_cnt_last_4_week
    ,cx.cx_added_till_date

from identifier($base_tbl) a

left join analytics.data_science.data_early_dpd3_features_payment_collection_reminder b
    on  a.USER_ID = b.USER_ID and a.CUTOFF_DATE = b.CUTOFF_DATE

left join analytics.data_science.data_early_dpd3_features_debit_credit_ratio c
    on  a.USER_ID = c.USER_ID and a.CUTOFF_DATE = c.CUTOFF_DATE

left join analytics.data_science.data_early_dpd3_features_loan_communications d
    on  a.USER_ID = d.USER_ID and a.CUTOFF_DATE = d.CUTOFF_DATE

left join analytics.data_science.data_early_dpd3_features_loan_explored_channel e
    on  a.USER_ID = e.USER_ID and a.CUTOFF_DATE = e.CUTOFF_DATE

left join analytics.data_science.data_early_dpd3_features_other_features g
    on  a.USER_ID = g.USER_ID and a.CUTOFF_DATE = g.CUTOFF_DATE

left join analytics.data_science.data_early_dpd3_features_active_users h
    on  a.USER_ID = h.USER_ID and a.CUTOFF_DATE = h.CUTOFF_DATE

left join (Select kb_id, login_time::date login_dt from analytics.model.user_base_fact_latest) u
    on u.kb_id = a.USER_ID

left join analytics.data_science.data_early_dpd3_features_customer_addition cx
    on a.USER_ID = cx.USER_ID and a.CUTOFF_DATE = cx.CUTOFF_DATE
);

-- Check the compiled data
select count(*) as total_rows, count(distinct USER_ID) as distinct_USERS
from analytics.data_science.data_early_dpd3_features_compiled_data;

describe table analytics.data_science.data_early_dpd3_features_compiled_data;



--------  FINAL APP FEATURES  ===========================
create or replace transient table analytics.data_science.data_early_dpd3_final_app_features as (
with
-- Step 1: Collection Reminder Derived Features
collection_reminder_features as (
    select
         USER_ID, CUTOFF_DATE,

        -- Original features (keep all)
        collection_reminders_sent_last_1_2_day,
        collection_reminders_sent_last_3_4_day,
        collection_reminders_sent_last_5_6_day,
        collection_reminders_sent_last_7_8_day,
        collection_reminders_sent_last_9_11_day,
        collection_reminders_sent_last_12_14_day,
        collection_reminders_sent_last_1_week,
        collection_reminders_sent_last_2_week,
        collection_reminders_sent_last_3_week,
        collection_reminders_sent_last_4_week,
        collection_reminders_sent_dts_last_1_2_day,
        collection_reminders_sent_dts_last_3_4_day,
        collection_reminders_sent_dts_last_5_6_day,
        collection_reminders_sent_dts_last_7_8_day,
        collection_reminders_sent_dts_last_9_11_day,
        collection_reminders_sent_dts_last_12_14_day,
        collection_reminders_sent_dts_last_1_week,
        collection_reminders_sent_dts_last_2_week,
        collection_reminders_sent_dts_last_3_week,
        collection_reminders_sent_dts_last_4_week,

        -- NEW DERIVED: Find minimum collection reminder rate
        LEAST(
            case when (collection_reminders_sent_last_1_week is null or collection_reminders_sent_last_1_week=0)
                then 99999999 else collection_reminders_sent_last_1_week/7 end,
            case when (collection_reminders_sent_last_2_week is null or collection_reminders_sent_last_2_week=0)
                then 99999999 else collection_reminders_sent_last_2_week/7 end,
            case when (collection_reminders_sent_last_3_week is null or collection_reminders_sent_last_3_week=0)
                then 99999999 else collection_reminders_sent_last_3_week/7 end,
            case when (collection_reminders_sent_last_4_week is null or collection_reminders_sent_last_4_week=0)
                then 99999999 else collection_reminders_sent_last_4_week/9 end
        ) as least_collection_reminder,

        LEAST(
            case when (collection_reminders_sent_dts_last_1_week is null or collection_reminders_sent_dts_last_1_week=0)
                then 99999999 else collection_reminders_sent_dts_last_1_week/7 end,
            case when (collection_reminders_sent_dts_last_2_week is null or collection_reminders_sent_dts_last_2_week=0)
                then 99999999 else collection_reminders_sent_dts_last_2_week/7 end,
            case when (collection_reminders_sent_dts_last_3_week is null or collection_reminders_sent_dts_last_3_week=0)
                then 99999999 else collection_reminders_sent_dts_last_3_week/7 end,
            case when (collection_reminders_sent_dts_last_4_week is null or collection_reminders_sent_dts_last_4_week=0)
                then 99999999 else collection_reminders_sent_dts_last_4_week/9 end
        ) as least_collection_dts_reminder

    from analytics.data_science.data_early_dpd3_features_compiled_data
),

-- Step 2: Collection Reminder Indexes
collection_indexes as (
    select
         USER_ID, CUTOFF_DATE,

        -- Keep all original features
        collection_reminders_sent_last_1_2_day,
        collection_reminders_sent_last_3_4_day,
        collection_reminders_sent_last_5_6_day,
        collection_reminders_sent_last_7_8_day,
        collection_reminders_sent_last_9_11_day,
        collection_reminders_sent_last_12_14_day,
        collection_reminders_sent_last_1_week,
        collection_reminders_sent_last_2_week,
        collection_reminders_sent_last_3_week,
        collection_reminders_sent_last_4_week,
        collection_reminders_sent_dts_last_1_2_day,
        collection_reminders_sent_dts_last_3_4_day,
        collection_reminders_sent_dts_last_5_6_day,
        collection_reminders_sent_dts_last_7_8_day,
        collection_reminders_sent_dts_last_9_11_day,
        collection_reminders_sent_dts_last_12_14_day,
        collection_reminders_sent_dts_last_1_week,
        collection_reminders_sent_dts_last_2_week,
        collection_reminders_sent_dts_last_3_week,
        collection_reminders_sent_dts_last_4_week,

        -- Clean least values
        case when least_collection_reminder=99999999 then 0 else least_collection_reminder end as least_collection_reminder_clean,
        case when least_collection_dts_reminder=99999999 then 0 else least_collection_dts_reminder end as least_collection_dts_reminder_clean,

        -- NEW DERIVED: Latest collection reminder index (week 1-2)
        DIV0(collection_reminders_sent_last_1_week, 7 * case when least_collection_reminder=99999999 then 0 else least_collection_reminder end) +
        DIV0(collection_reminders_sent_last_2_week, 7 * case when least_collection_reminder=99999999 then 0 else least_collection_reminder end)
        as LATEST_COLLECTION_REMINDER_INDEX,

        -- NEW DERIVED: Old collection reminder index (week 3-4)
        DIV0(collection_reminders_sent_last_3_week, 7 * case when least_collection_reminder=99999999 then 0 else least_collection_reminder end) +
        DIV0(collection_reminders_sent_last_4_week, 9 * case when least_collection_reminder=99999999 then 0 else least_collection_reminder end)
        as OLD_COLLECTION_REMINDER_INDEX,

        -- NEW DERIVED: Latest collection reminder days index
        DIV0(collection_reminders_sent_dts_last_1_week, 7 * case when least_collection_dts_reminder=99999999 then 0 else least_collection_dts_reminder end) +
        DIV0(collection_reminders_sent_dts_last_2_week, 7 * case when least_collection_dts_reminder=99999999 then 0 else least_collection_dts_reminder end)
        as LATEST_COLLECTION_REMINDER_DAYS_INDEX,

        -- NEW DERIVED: Old collection reminder days index
        DIV0(collection_reminders_sent_dts_last_3_week, 7 * case when least_collection_dts_reminder=99999999 then 0 else least_collection_dts_reminder end) +
        DIV0(collection_reminders_sent_dts_last_4_week, 9 * case when least_collection_dts_reminder=99999999 then 0 else least_collection_dts_reminder end)
        as OLD_COLLECTION_REMINDER_DAYS_INDEX

    from collection_reminder_features
),

-- Step 3: Add all other app features with derived metrics
other_app_features as (
    select
         c.USER_ID, c.CUTOFF_DATE,

        -- ALL Transaction Features
        c.CR_TXU_VALUE_last_1_2_day, c.CR_TXU_VALUE_last_3_4_day, c.CR_TXU_VALUE_last_5_6_day,
        c.CR_TXU_VALUE_last_7_8_day, c.CR_TXU_VALUE_last_9_11_day, c.CR_TXU_VALUE_last_12_14_day,
        c.CR_TXU_VALUE_last_1_week, c.CR_TXU_VALUE_last_2_week, c.CR_TXU_VALUE_last_3_week, c.CR_TXU_VALUE_last_4_week,

        c.DB_TXU_VALUE_last_1_2_day, c.DB_TXU_VALUE_last_3_4_day, c.DB_TXU_VALUE_last_5_6_day,
        c.DB_TXU_VALUE_last_7_8_day, c.DB_TXU_VALUE_last_9_11_day, c.DB_TXU_VALUE_last_12_14_day,
        c.DB_TXU_VALUE_last_1_week, c.DB_TXU_VALUE_last_2_week, c.DB_TXU_VALUE_last_3_week, c.DB_TXU_VALUE_last_4_week,

        c.TXU_VALUE_last_1_2_day, c.TXU_VALUE_last_3_4_day, c.TXU_VALUE_last_5_6_day,
        c.TXU_VALUE_last_7_8_day, c.TXU_VALUE_last_9_11_day, c.TXU_VALUE_last_12_14_day,
        c.TXU_VALUE_last_1_week, c.TXU_VALUE_last_2_week, c.TXU_VALUE_last_3_week, c.TXU_VALUE_last_4_week,

        c.txng_days_last_1_2_day, c.txng_days_last_3_4_day, c.txng_days_last_5_6_day,
        c.txng_days_last_7_8_day, c.txng_days_last_9_11_day, c.txng_days_last_12_14_day,
        c.txng_days_last_1_week, c.txng_days_last_2_week, c.txng_days_last_3_week, c.txng_days_last_4_week,

        -- DB/CR Ratios (cleaned)
        -- DB/CR Ratios (cleaned)
        abs(c.db_cr_ratio_last_1_2_day)  as db_cr_ratio_last_1_2_day,
        abs(c.db_cr_ratio_last_3_4_day)  as db_cr_ratio_last_3_4_day,
        abs(c.db_cr_ratio_last_5_6_day)  as db_cr_ratio_last_5_6_day,
        abs(c.db_cr_ratio_last_7_8_day)  as db_cr_ratio_last_7_8_day,
        abs(c.db_cr_ratio_last_9_11_day) as db_cr_ratio_last_9_11_day,
        abs(c.db_cr_ratio_last_12_14_day) as db_cr_ratio_last_12_14_day,
        abs(c.db_cr_ratio_last_1_week)   as db_cr_ratio_last_1_week,
        abs(c.db_cr_ratio_last_2_week)   as db_cr_ratio_last_2_week,
        abs(c.db_cr_ratio_last_3_week)   as db_cr_ratio_last_3_week,
        abs(c.db_cr_ratio_last_4_week)   as db_cr_ratio_last_4_week,
        -- ALL App Open Features
        c.app_open_cnt_last_1_2_day, c.app_open_cnt_last_3_4_day, c.app_open_cnt_last_5_6_day,
        c.app_open_cnt_last_7_8_day, c.app_open_cnt_last_9_11_day, c.app_open_cnt_last_12_14_day,
        c.app_open_cnt_last_1_week, c.app_open_cnt_last_2_week, c.app_open_cnt_last_3_week, c.app_open_cnt_last_4_week,

        c.app_open_days_last_1_2_day, c.app_open_days_last_3_4_day, c.app_open_days_last_5_6_day,
        c.app_open_days_last_7_8_day, c.app_open_days_last_9_11_day, c.app_open_days_last_12_14_day,
        c.app_open_days_last_1_week, c.app_open_days_last_2_week, c.app_open_days_last_3_week, c.app_open_days_last_4_week,

        -- NEW DERIVED: App Open Ratios (opens per day)
        DIV0(c.app_open_cnt_last_1_2_day, c.app_open_days_last_1_2_day) as app_open_ratio_last_1_2_day,
        DIV0(c.app_open_cnt_last_3_4_day, c.app_open_days_last_3_4_day) as app_open_ratio_last_3_4_day,
        DIV0(c.app_open_cnt_last_5_6_day, c.app_open_days_last_5_6_day) as app_open_ratio_last_5_6_day,
        DIV0(c.app_open_cnt_last_7_8_day, c.app_open_days_last_7_8_day) as app_open_ratio_last_7_8_day,
        DIV0(c.app_open_cnt_last_1_week, c.app_open_days_last_1_week) as app_open_ratio_last_1_week,
        DIV0(c.app_open_cnt_last_2_week, c.app_open_days_last_2_week) as app_open_ratio_last_2_week,
        DIV0(c.app_open_cnt_last_3_week, c.app_open_days_last_3_week) as app_open_ratio_last_3_week,
        DIV0(c.app_open_cnt_last_4_week, c.app_open_days_last_4_week) as app_open_ratio_last_4_week,

        -- ALL Other Features
        c.Explored_other_features_last_1_2_day, c.Explored_other_features_last_3_4_day, c.Explored_other_features_last_5_6_day,
        c.Explored_other_features_last_7_8_day, c.Explored_other_features_last_9_11_day, c.Explored_other_features_last_12_14_day,
        c.Explored_other_features_last_1_week, c.Explored_other_features_last_2_week,
        c.Explored_other_features_last_3_week, c.Explored_other_features_last_4_week,

        c.Used_other_features_last_1_2_day, c.Used_other_features_last_3_4_day, c.Used_other_features_last_5_6_day,
        c.Used_other_features_last_7_8_day, c.Used_other_features_last_9_11_day, c.Used_other_features_last_12_14_day,
        c.Used_other_features_last_1_week, c.Used_other_features_last_2_week,
        c.Used_other_features_last_3_week, c.Used_other_features_last_4_week,

        -- ALL Loan Communication Features
        c.loan_comm_cnt_last_1_2_day, c.loan_comm_cnt_last_3_4_day, c.loan_comm_cnt_last_5_6_day,
        c.loan_comm_cnt_last_7_8_day, c.loan_comm_cnt_last_9_11_day, c.loan_comm_cnt_last_12_14_day,
        c.loan_comm_cnt_last_1_week, c.loan_comm_cnt_last_2_week, c.loan_comm_cnt_last_3_week, c.loan_comm_cnt_last_4_week,

        c.loan_WA_comm_cnt_last_1_2_day, c.loan_WA_comm_cnt_last_3_4_day, c.loan_WA_comm_cnt_last_5_6_day,
        c.loan_WA_comm_cnt_last_7_8_day, c.loan_WA_comm_cnt_last_9_11_day, c.loan_WA_comm_cnt_last_12_14_day,
        c.loan_WA_comm_cnt_last_1_week, c.loan_WA_comm_cnt_last_2_week, c.loan_WA_comm_cnt_last_3_week, c.loan_WA_comm_cnt_last_4_week,

        -- ALL Loan Explored Features
        c.loan_explored_cnt_last_1_2_day, c.loan_explored_cnt_last_3_4_day, c.loan_explored_cnt_last_5_6_day,
        c.loan_explored_cnt_last_7_8_day, c.loan_explored_cnt_last_9_11_day, c.loan_explored_cnt_last_12_14_day,
        c.loan_explored_cnt_last_1_week, c.loan_explored_cnt_last_2_week, c.loan_explored_cnt_last_3_week, c.loan_explored_cnt_last_4_week,

        c.mtkng_loan_explored_cnt_last_1_2_day, c.mtkng_loan_explored_cnt_last_3_4_day, c.mtkng_loan_explored_cnt_last_5_6_day,
        c.mtkng_loan_explored_cnt_last_7_8_day, c.mtkng_loan_explored_cnt_last_9_11_day, c.mtkng_loan_explored_cnt_last_12_14_day,
        c.mtkng_loan_explored_cnt_last_1_week, c.mtkng_loan_explored_cnt_last_2_week,
        c.mtkng_loan_explored_cnt_last_3_week, c.mtkng_loan_explored_cnt_last_4_week,

        c.product_loan_explored_cnt_last_1_2_day, c.product_loan_explored_cnt_last_3_4_day, c.product_loan_explored_cnt_last_5_6_day,
        c.product_loan_explored_cnt_last_7_8_day, c.product_loan_explored_cnt_last_9_11_day, c.product_loan_explored_cnt_last_12_14_day,
        c.product_loan_explored_cnt_last_1_week, c.product_loan_explored_cnt_last_2_week,
        c.product_loan_explored_cnt_last_3_week, c.product_loan_explored_cnt_last_4_week,

        -- ALL Customer Addition Features
        c.cx_added_cnt_last_1_2_day, c.cx_added_cnt_last_3_4_day, c.cx_added_cnt_last_5_6_day,
        c.cx_added_cnt_last_7_8_day, c.cx_added_cnt_last_9_11_day, c.cx_added_cnt_last_12_14_day,
        c.cx_added_cnt_last_1_week, c.cx_added_cnt_last_2_week, c.cx_added_cnt_last_3_week, c.cx_added_cnt_last_4_week,
        c.cx_added_till_date,

        -- User Age
        c.age_on_app,

        -- Collection Reminder Features from previous CTE
        cr.collection_reminders_sent_last_1_2_day,
        cr.collection_reminders_sent_last_3_4_day,
        cr.collection_reminders_sent_last_5_6_day,
        cr.collection_reminders_sent_last_7_8_day,
        cr.collection_reminders_sent_last_9_11_day,
        cr.collection_reminders_sent_last_12_14_day,
        cr.collection_reminders_sent_last_1_week,
        cr.collection_reminders_sent_last_2_week,
        cr.collection_reminders_sent_last_3_week,
        cr.collection_reminders_sent_last_4_week,
        cr.collection_reminders_sent_dts_last_1_2_day,
        cr.collection_reminders_sent_dts_last_3_4_day,
        cr.collection_reminders_sent_dts_last_5_6_day,
        cr.collection_reminders_sent_dts_last_7_8_day,
        cr.collection_reminders_sent_dts_last_9_11_day,
        cr.collection_reminders_sent_dts_last_12_14_day,
        cr.collection_reminders_sent_dts_last_1_week,
        cr.collection_reminders_sent_dts_last_2_week,
        cr.collection_reminders_sent_dts_last_3_week,
        cr.collection_reminders_sent_dts_last_4_week,
        cr.LATEST_COLLECTION_REMINDER_INDEX,
        cr.OLD_COLLECTION_REMINDER_INDEX,
        cr.LATEST_COLLECTION_REMINDER_DAYS_INDEX,
        cr.OLD_COLLECTION_REMINDER_DAYS_INDEX

    from analytics.data_science.data_early_dpd3_features_compiled_data c
    left join collection_indexes cr
        on  c.USER_ID = cr.USER_ID and c.CUTOFF_DATE = cr.CUTOFF_DATE
),

-- Step 4: Calculate Latest vs Old Trends
trend_features as (
    select
        *,

        -- NEW DERIVED: App Open Ratio Latest (week 1-2 max)
        GREATEST(
            COALESCE(app_open_ratio_last_1_week, -999999),
            COALESCE(app_open_ratio_last_2_week, -999999)
        ) as app_open_ratio_latest_raw,

        -- NEW DERIVED: App Open Ratio Old (week 3-4 min)
        LEAST(
            COALESCE(app_open_ratio_last_3_week, 999999),
            COALESCE(app_open_ratio_last_4_week, 999999)
        ) as app_open_ratio_old_raw,

        -- NEW DERIVED: DB/CR Ratio Latest
        GREATEST(
            COALESCE(db_cr_ratio_last_1_week, -999999),
            COALESCE(db_cr_ratio_last_2_week, -999999)
        ) as db_cr_ratio_latest_raw,

        -- NEW DERIVED: DB/CR Ratio Old
        LEAST(
            COALESCE(db_cr_ratio_last_3_week, 999999),
            COALESCE(db_cr_ratio_last_4_week, 999999)
        ) as db_cr_ratio_old_raw

    from other_app_features
)

-- Final select with all features + derived trend ratios
select
    *,

    -- Clean up placeholder values
    case when app_open_ratio_latest_raw = -999999 then NULL else app_open_ratio_latest_raw end as APP_OPEN_RATIO_LATEST,
    case when app_open_ratio_old_raw = 999999 then NULL else app_open_ratio_old_raw end as APP_OPEN_RATIO_OLD,
    case when db_cr_ratio_latest_raw = -999999 then NULL else db_cr_ratio_latest_raw end as DB_CR_RATIO_LATEST,
    case when db_cr_ratio_old_raw = 999999 then NULL else db_cr_ratio_old_raw end as DB_CR_RATIO_OLD,

    -- NEW DERIVED: Latest vs Old Trend Ratios
    DIV0(
        case when app_open_ratio_latest_raw = -999999 then NULL else app_open_ratio_latest_raw end,
        case when app_open_ratio_old_raw = 999999 then NULL else app_open_ratio_old_raw end
    ) as APP_OPEN_RATIO_LATEST_VS_OLD,

    DIV0(
        case when db_cr_ratio_latest_raw = -999999 then NULL else db_cr_ratio_latest_raw end,
        case when db_cr_ratio_old_raw = 999999 then NULL else db_cr_ratio_old_raw end
    ) as DB_CR_RATIO_LATEST_VS_OLD,

    -- NEW DERIVED: Collection Reminder Trend
    DIV0(LATEST_COLLECTION_REMINDER_INDEX, OLD_COLLECTION_REMINDER_INDEX) as COLLECTION_REMINDER_INDEX_LATEST_VS_OLD,
    DIV0(LATEST_COLLECTION_REMINDER_DAYS_INDEX, OLD_COLLECTION_REMINDER_DAYS_INDEX) as COLLECTION_REMINDER_DAYS_INDEX_LATEST_VS_OLD

from trend_features
);


-- -- Verify: Count features
describe table analytics.data_science.data_early_dpd3_final_app_features;

-- -- Check data
-- select * from analytics.data_science.data_early_dpd3_final_app_features limit 5;


-- SELECT
--     COUNT(*) AS total_rows,
--     SUM(CASE WHEN USER_ID IS NULL THEN 1 ELSE 0 END) AS null_user_id,
--     SUM(CASE WHEN CUTOFF_DATE IS NULL THEN 1 ELSE 0 END) AS null_CUTOFF_DATE
-- FROM analytics.data_science.data_early_dpd3_final_app_features;


select count(*) , count (distinct user_id), count(distinct user_id, cutoff_date) from analytics.data_science.data_early_dpd3_final_app_features;
-- COUNT(*)	COUNT (DISTINCT USER_ID)	COUNT(DISTINCT USER_ID, CUTOFF_DATE)
-- 5238679	2586889	5238679



select * from analytics.data_science.data_early_dpd3_final_app_features limit 100;
