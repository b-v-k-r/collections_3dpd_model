set base_tbl = 'analytics.data_science.field_disposition_base';
create
or replace table analytics.data_science.all_activity_features_for_early_dpd3 as
with base_activity as (
select
    user_id,
    cutoff_date,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_AddAddressOnGSTINClick
            ELSE 0
        END
    ) AS num_AddAddressOnGSTINClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_AddAddressOnGSTINClick
            ELSE 0
        END
    ) AS num_AddAddressOnGSTINClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_AddAddressOnGSTINClick
            ELSE 0
        END
    ) AS num_AddAddressOnGSTINClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_AddBankAccount
            ELSE 0
        END
    ) AS num_AddBankAccount_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_AddBankAccount
            ELSE 0
        END
    ) AS num_AddBankAccount_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_AddBankAccount
            ELSE 0
        END
    ) AS num_AddBankAccount_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_AddNewCustomer
            ELSE 0
        END
    ) AS num_AddNewCustomer_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_AddNewCustomer
            ELSE 0
        END
    ) AS num_AddNewCustomer_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_AddNewCustomer
            ELSE 0
        END
    ) AS num_AddNewCustomer_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_BillsExpenseAddExpense
            ELSE 0
        END
    ) AS num_BillsExpenseAddExpense_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_BillsExpenseAddExpense
            ELSE 0
        END
    ) AS num_BillsExpenseAddExpense_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_BillsExpenseAddExpense
            ELSE 0
        END
    ) AS num_BillsExpenseAddExpense_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_BillsExpenseAddNewCategoryClicked
            ELSE 0
        END
    ) AS num_BillsExpenseAddNewCategoryClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_BillsExpenseAddNewCategoryClicked
            ELSE 0
        END
    ) AS num_BillsExpenseAddNewCategoryClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_BillsExpenseAddNewCategoryClicked
            ELSE 0
        END
    ) AS num_BillsExpenseAddNewCategoryClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_BillsExpenseAmountClicked
            ELSE 0
        END
    ) AS num_BillsExpenseAmountClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_BillsExpenseAmountClicked
            ELSE 0
        END
    ) AS num_BillsExpenseAmountClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_BillsExpenseAmountClicked
            ELSE 0
        END
    ) AS num_BillsExpenseAmountClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_BillsExpenseCategorySelected
            ELSE 0
        END
    ) AS num_BillsExpenseCategorySelected_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_BillsExpenseCategorySelected
            ELSE 0
        END
    ) AS num_BillsExpenseCategorySelected_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_BillsExpenseCategorySelected
            ELSE 0
        END
    ) AS num_BillsExpenseCategorySelected_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_BillsExpenseSaveClicked
            ELSE 0
        END
    ) AS num_BillsExpenseSaveClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_BillsExpenseSaveClicked
            ELSE 0
        END
    ) AS num_BillsExpenseSaveClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_BillsExpenseSaveClicked
            ELSE 0
        END
    ) AS num_BillsExpenseSaveClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_BillsInvoiceViewPDFClicked
            ELSE 0
        END
    ) AS num_BillsInvoiceViewPDFClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_BillsInvoiceViewPDFClicked
            ELSE 0
        END
    ) AS num_BillsInvoiceViewPDFClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_BillsInvoiceViewPDFClicked
            ELSE 0
        END
    ) AS num_BillsInvoiceViewPDFClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_BulkReminderSuccessfullySent
            ELSE 0
        END
    ) AS num_BulkReminderSuccessfullySent_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_BulkReminderSuccessfullySent
            ELSE 0
        END
    ) AS num_BulkReminderSuccessfullySent_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_BulkReminderSuccessfullySent
            ELSE 0
        END
    ) AS num_BulkReminderSuccessfullySent_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CashRegisterAddEntry
            ELSE 0
        END
    ) AS num_CashRegisterAddEntry_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CashRegisterAddEntry
            ELSE 0
        END
    ) AS num_CashRegisterAddEntry_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CashRegisterAddEntry
            ELSE 0
        END
    ) AS num_CashRegisterAddEntry_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CashRegisterAttachmentClick
            ELSE 0
        END
    ) AS num_CashRegisterAttachmentClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CashRegisterAttachmentClick
            ELSE 0
        END
    ) AS num_CashRegisterAttachmentClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CashRegisterAttachmentClick
            ELSE 0
        END
    ) AS num_CashRegisterAttachmentClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CashRegisterDailySummaryClick
            ELSE 0
        END
    ) AS num_CashRegisterDailySummaryClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CashRegisterDailySummaryClick
            ELSE 0
        END
    ) AS num_CashRegisterDailySummaryClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CashRegisterDailySummaryClick
            ELSE 0
        END
    ) AS num_CashRegisterDailySummaryClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CashRegisterSaveClick
            ELSE 0
        END
    ) AS num_CashRegisterSaveClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CashRegisterSaveClick
            ELSE 0
        END
    ) AS num_CashRegisterSaveClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CashRegisterSaveClick
            ELSE 0
        END
    ) AS num_CashRegisterSaveClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_Clicked_Button_Save_New_Customer
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_Customer_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_Clicked_Button_Save_New_Customer
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_Customer_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_Clicked_Button_Save_New_Customer
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_Customer_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_Clicked_Button_Save_New_KhataBook
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_KhataBook_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_Clicked_Button_Save_New_KhataBook
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_KhataBook_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_Clicked_Button_Save_New_KhataBook
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_KhataBook_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_Clicked_Button_Save_New_Transaction
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_Transaction_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_Clicked_Button_Save_New_Transaction
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_Transaction_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_Clicked_Button_Save_New_Transaction
            ELSE 0
        END
    ) AS num_Clicked_Button_Save_New_Transaction_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CreditScoreCheckNowClick
            ELSE 0
        END
    ) AS num_CreditScoreCheckNowClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CreditScoreCheckNowClick
            ELSE 0
        END
    ) AS num_CreditScoreCheckNowClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CreditScoreCheckNowClick
            ELSE 0
        END
    ) AS num_CreditScoreCheckNowClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CreditScoreFetchSuccess
            ELSE 0
        END
    ) AS num_CreditScoreFetchSuccess_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CreditScoreFetchSuccess
            ELSE 0
        END
    ) AS num_CreditScoreFetchSuccess_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CreditScoreFetchSuccess
            ELSE 0
        END
    ) AS num_CreditScoreFetchSuccess_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataMakeCall
            ELSE 0
        END
    ) AS num_CustomerKhataMakeCall_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataMakeCall
            ELSE 0
        END
    ) AS num_CustomerKhataMakeCall_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataMakeCall
            ELSE 0
        END
    ) AS num_CustomerKhataMakeCall_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataMobileClick
            ELSE 0
        END
    ) AS num_CustomerKhataMobileClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataMobileClick
            ELSE 0
        END
    ) AS num_CustomerKhataMobileClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataMobileClick
            ELSE 0
        END
    ) AS num_CustomerKhataMobileClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOk
            ELSE 0
        END
    ) AS num_CustomerKhataOk_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOk
            ELSE 0
        END
    ) AS num_CustomerKhataOk_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOk
            ELSE 0
        END
    ) AS num_CustomerKhataOk_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOnGaveClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnGaveClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOnGaveClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnGaveClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOnGaveClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnGaveClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOnGotClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnGotClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOnGotClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnGotClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOnGotClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnGotClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOnPaymentClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnPaymentClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOnPaymentClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnPaymentClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOnPaymentClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnPaymentClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOnReportsClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnReportsClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOnReportsClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnReportsClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOnReportsClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnReportsClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOnSmsClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnSmsClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOnSmsClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnSmsClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOnSmsClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnSmsClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOnTransactionClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnTransactionClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOnTransactionClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnTransactionClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOnTransactionClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnTransactionClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataOnWhatsAppReminderClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnWhatsAppReminderClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataOnWhatsAppReminderClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnWhatsAppReminderClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataOnWhatsAppReminderClicked
            ELSE 0
        END
    ) AS num_CustomerKhataOnWhatsAppReminderClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataSave
            ELSE 0
        END
    ) AS num_CustomerKhataSave_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataSave
            ELSE 0
        END
    ) AS num_CustomerKhataSave_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataSave
            ELSE 0
        END
    ) AS num_CustomerKhataSave_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataSendFreeSMSClick
            ELSE 0
        END
    ) AS num_CustomerKhataSendFreeSMSClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataSendFreeSMSClick
            ELSE 0
        END
    ) AS num_CustomerKhataSendFreeSMSClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataSendFreeSMSClick
            ELSE 0
        END
    ) AS num_CustomerKhataSendFreeSMSClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataSendSms
            ELSE 0
        END
    ) AS num_CustomerKhataSendSms_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataSendSms
            ELSE 0
        END
    ) AS num_CustomerKhataSendSms_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataSendSms
            ELSE 0
        END
    ) AS num_CustomerKhataSendSms_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_CustomerKhataShareEntry
            ELSE 0
        END
    ) AS num_CustomerKhataShareEntry_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_CustomerKhataShareEntry
            ELSE 0
        END
    ) AS num_CustomerKhataShareEntry_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_CustomerKhataShareEntry
            ELSE 0
        END
    ) AS num_CustomerKhataShareEntry_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementAddItemHomeClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddItemHomeClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementAddItemHomeClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddItemHomeClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementAddItemHomeClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddItemHomeClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementAddSalesPriceClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddSalesPriceClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementAddSalesPriceClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddSalesPriceClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementAddSalesPriceClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddSalesPriceClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementAddStockInstructionsClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddStockInstructionsClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementAddStockInstructionsClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddStockInstructionsClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementAddStockInstructionsClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddStockInstructionsClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementAddUnitClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddUnitClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementAddUnitClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddUnitClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementAddUnitClick
            ELSE 0
        END
    ) AS num_InventoryManagementAddUnitClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementDateClick
            ELSE 0
        END
    ) AS num_InventoryManagementDateClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementDateClick
            ELSE 0
        END
    ) AS num_InventoryManagementDateClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementDateClick
            ELSE 0
        END
    ) AS num_InventoryManagementDateClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementEditItemDetail
            ELSE 0
        END
    ) AS num_InventoryManagementEditItemDetail_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementEditItemDetail
            ELSE 0
        END
    ) AS num_InventoryManagementEditItemDetail_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementEditItemDetail
            ELSE 0
        END
    ) AS num_InventoryManagementEditItemDetail_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementGstFieldClick
            ELSE 0
        END
    ) AS num_InventoryManagementGstFieldClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementGstFieldClick
            ELSE 0
        END
    ) AS num_InventoryManagementGstFieldClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementGstFieldClick
            ELSE 0
        END
    ) AS num_InventoryManagementGstFieldClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementGstSelect
            ELSE 0
        END
    ) AS num_InventoryManagementGstSelect_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementGstSelect
            ELSE 0
        END
    ) AS num_InventoryManagementGstSelect_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementGstSelect
            ELSE 0
        END
    ) AS num_InventoryManagementGstSelect_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementItemStockInClick
            ELSE 0
        END
    ) AS num_InventoryManagementItemStockInClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementItemStockInClick
            ELSE 0
        END
    ) AS num_InventoryManagementItemStockInClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementItemStockInClick
            ELSE 0
        END
    ) AS num_InventoryManagementItemStockInClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementItemStockOutClick
            ELSE 0
        END
    ) AS num_InventoryManagementItemStockOutClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementItemStockOutClick
            ELSE 0
        END
    ) AS num_InventoryManagementItemStockOutClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementItemStockOutClick
            ELSE 0
        END
    ) AS num_InventoryManagementItemStockOutClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementLowStockClick
            ELSE 0
        END
    ) AS num_InventoryManagementLowStockClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementLowStockClick
            ELSE 0
        END
    ) AS num_InventoryManagementLowStockClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementLowStockClick
            ELSE 0
        END
    ) AS num_InventoryManagementLowStockClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementOnEditItemSaveClick
            ELSE 0
        END
    ) AS num_InventoryManagementOnEditItemSaveClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementOnEditItemSaveClick
            ELSE 0
        END
    ) AS num_InventoryManagementOnEditItemSaveClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementOnEditItemSaveClick
            ELSE 0
        END
    ) AS num_InventoryManagementOnEditItemSaveClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementOnEditStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementOnEditStockClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementOnEditStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementOnEditStockClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementOnEditStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementOnEditStockClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementOnInventoryHomeListItemClick
            ELSE 0
        END
    ) AS num_InventoryManagementOnInventoryHomeListItemClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementOnInventoryHomeListItemClick
            ELSE 0
        END
    ) AS num_InventoryManagementOnInventoryHomeListItemClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementOnInventoryHomeListItemClick
            ELSE 0
        END
    ) AS num_InventoryManagementOnInventoryHomeListItemClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementSaveItemClick
            ELSE 0
        END
    ) AS num_InventoryManagementSaveItemClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementSaveItemClick
            ELSE 0
        END
    ) AS num_InventoryManagementSaveItemClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementSaveItemClick
            ELSE 0
        END
    ) AS num_InventoryManagementSaveItemClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementSelectedDate
            ELSE 0
        END
    ) AS num_InventoryManagementSelectedDate_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementSelectedDate
            ELSE 0
        END
    ) AS num_InventoryManagementSelectedDate_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementSelectedDate
            ELSE 0
        END
    ) AS num_InventoryManagementSelectedDate_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementStockInEntryClick
            ELSE 0
        END
    ) AS num_InventoryManagementStockInEntryClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementStockInEntryClick
            ELSE 0
        END
    ) AS num_InventoryManagementStockInEntryClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementStockInEntryClick
            ELSE 0
        END
    ) AS num_InventoryManagementStockInEntryClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementStockOutEntryClick
            ELSE 0
        END
    ) AS num_InventoryManagementStockOutEntryClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementStockOutEntryClick
            ELSE 0
        END
    ) AS num_InventoryManagementStockOutEntryClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementStockOutEntryClick
            ELSE 0
        END
    ) AS num_InventoryManagementStockOutEntryClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementUnitSelected
            ELSE 0
        END
    ) AS num_InventoryManagementUnitSelected_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementUnitSelected
            ELSE 0
        END
    ) AS num_InventoryManagementUnitSelected_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementUnitSelected
            ELSE 0
        END
    ) AS num_InventoryManagementUnitSelected_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementonAddStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementonAddStockClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementonAddStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementonAddStockClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementonAddStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementonAddStockClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventoryManagementonDeStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementonDeStockClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventoryManagementonDeStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementonDeStockClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventoryManagementonDeStockClicked
            ELSE 0
        END
    ) AS num_InventoryManagementonDeStockClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_InventorySearchHsnCodeClick
            ELSE 0
        END
    ) AS num_InventorySearchHsnCodeClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_InventorySearchHsnCodeClick
            ELSE 0
        END
    ) AS num_InventorySearchHsnCodeClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_InventorySearchHsnCodeClick
            ELSE 0
        END
    ) AS num_InventorySearchHsnCodeClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_KBCoinsBuyCoinsClick
            ELSE 0
        END
    ) AS num_KBCoinsBuyCoinsClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_KBCoinsBuyCoinsClick
            ELSE 0
        END
    ) AS num_KBCoinsBuyCoinsClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_KBCoinsBuyCoinsClick
            ELSE 0
        END
    ) AS num_KBCoinsBuyCoinsClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_KBCoinsHistoryTabChanged
            ELSE 0
        END
    ) AS num_KBCoinsHistoryTabChanged_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_KBCoinsHistoryTabChanged
            ELSE 0
        END
    ) AS num_KBCoinsHistoryTabChanged_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_KBCoinsHistoryTabChanged
            ELSE 0
        END
    ) AS num_KBCoinsHistoryTabChanged_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_KYUOnLocationPermissionGranted
            ELSE 0
        END
    ) AS num_KYUOnLocationPermissionGranted_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_KYUOnLocationPermissionGranted
            ELSE 0
        END
    ) AS num_KYUOnLocationPermissionGranted_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_KYUOnLocationPermissionGranted
            ELSE 0
        END
    ) AS num_KYUOnLocationPermissionGranted_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_KhataAddressSaveClick
            ELSE 0
        END
    ) AS num_KhataAddressSaveClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_KhataAddressSaveClick
            ELSE 0
        END
    ) AS num_KhataAddressSaveClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_KhataAddressSaveClick
            ELSE 0
        END
    ) AS num_KhataAddressSaveClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_KhataBulkReminderClick
            ELSE 0
        END
    ) AS num_KhataBulkReminderClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_KhataBulkReminderClick
            ELSE 0
        END
    ) AS num_KhataBulkReminderClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_KhataBulkReminderClick
            ELSE 0
        END
    ) AS num_KhataBulkReminderClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_KhataRequestMoneyClick
            ELSE 0
        END
    ) AS num_KhataRequestMoneyClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_KhataRequestMoneyClick
            ELSE 0
        END
    ) AS num_KhataRequestMoneyClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_KhataRequestMoneyClick
            ELSE 0
        END
    ) AS num_KhataRequestMoneyClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_KhataRequestMoneyCloseClick
            ELSE 0
        END
    ) AS num_KhataRequestMoneyCloseClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_KhataRequestMoneyCloseClick
            ELSE 0
        END
    ) AS num_KhataRequestMoneyCloseClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_KhataRequestMoneyCloseClick
            ELSE 0
        END
    ) AS num_KhataRequestMoneyCloseClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_LoanKhataAgreementGenerated
            ELSE 0
        END
    ) AS num_LoanKhataAgreementGenerated_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_LoanKhataAgreementGenerated
            ELSE 0
        END
    ) AS num_LoanKhataAgreementGenerated_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_LoanKhataAgreementGenerated
            ELSE 0
        END
    ) AS num_LoanKhataAgreementGenerated_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainBillsClick
            ELSE 0
        END
    ) AS num_MainBillsClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainBillsClick
            ELSE 0
        END
    ) AS num_MainBillsClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainBillsClick
            ELSE 0
        END
    ) AS num_MainBillsClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainInventoryClick
            ELSE 0
        END
    ) AS num_MainInventoryClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainInventoryClick
            ELSE 0
        END
    ) AS num_MainInventoryClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainInventoryClick
            ELSE 0
        END
    ) AS num_MainInventoryClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainMoneyClick
            ELSE 0
        END
    ) AS num_MainMoneyClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainMoneyClick
            ELSE 0
        END
    ) AS num_MainMoneyClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainMoneyClick
            ELSE 0
        END
    ) AS num_MainMoneyClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainMoreClick
            ELSE 0
        END
    ) AS num_MainMoreClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainMoreClick
            ELSE 0
        END
    ) AS num_MainMoreClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainMoreClick
            ELSE 0
        END
    ) AS num_MainMoreClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainNewKhataClick
            ELSE 0
        END
    ) AS num_MainNewKhataClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainNewKhataClick
            ELSE 0
        END
    ) AS num_MainNewKhataClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainNewKhataClick
            ELSE 0
        END
    ) AS num_MainNewKhataClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainSetBusinessNameClick
            ELSE 0
        END
    ) AS num_MainSetBusinessNameClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainSetBusinessNameClick
            ELSE 0
        END
    ) AS num_MainSetBusinessNameClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainSetBusinessNameClick
            ELSE 0
        END
    ) AS num_MainSetBusinessNameClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainShowBooks
            ELSE 0
        END
    ) AS num_MainShowBooks_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainShowBooks
            ELSE 0
        END
    ) AS num_MainShowBooks_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainShowBooks
            ELSE 0
        END
    ) AS num_MainShowBooks_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_MainShowHomeScreenKYUBottomsheet
            ELSE 0
        END
    ) AS num_MainShowHomeScreenKYUBottomsheet_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_MainShowHomeScreenKYUBottomsheet
            ELSE 0
        END
    ) AS num_MainShowHomeScreenKYUBottomsheet_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_MainShowHomeScreenKYUBottomsheet
            ELSE 0
        END
    ) AS num_MainShowHomeScreenKYUBottomsheet_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_NewCustomerAdded
            ELSE 0
        END
    ) AS num_NewCustomerAdded_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_NewCustomerAdded
            ELSE 0
        END
    ) AS num_NewCustomerAdded_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_NewCustomerAdded
            ELSE 0
        END
    ) AS num_NewCustomerAdded_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_NoCounterPartyPaymentCompleted
            ELSE 0
        END
    ) AS num_NoCounterPartyPaymentCompleted_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_NoCounterPartyPaymentCompleted
            ELSE 0
        END
    ) AS num_NoCounterPartyPaymentCompleted_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_NoCounterPartyPaymentCompleted
            ELSE 0
        END
    ) AS num_NoCounterPartyPaymentCompleted_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingAllowed
            ELSE 0
        END
    ) AS num_OnboardingAllowed_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingAllowed
            ELSE 0
        END
    ) AS num_OnboardingAllowed_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingAllowed
            ELSE 0
        END
    ) AS num_OnboardingAllowed_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingDenied
            ELSE 0
        END
    ) AS num_OnboardingDenied_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingDenied
            ELSE 0
        END
    ) AS num_OnboardingDenied_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingDenied
            ELSE 0
        END
    ) AS num_OnboardingDenied_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingError
            ELSE 0
        END
    ) AS num_OnboardingError_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingError
            ELSE 0
        END
    ) AS num_OnboardingError_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingError
            ELSE 0
        END
    ) AS num_OnboardingError_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingPermanentDenied
            ELSE 0
        END
    ) AS num_OnboardingPermanentDenied_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingPermanentDenied
            ELSE 0
        END
    ) AS num_OnboardingPermanentDenied_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingPermanentDenied
            ELSE 0
        END
    ) AS num_OnboardingPermanentDenied_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingPhoneHintReceived
            ELSE 0
        END
    ) AS num_OnboardingPhoneHintReceived_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingPhoneHintReceived
            ELSE 0
        END
    ) AS num_OnboardingPhoneHintReceived_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingPhoneHintReceived
            ELSE 0
        END
    ) AS num_OnboardingPhoneHintReceived_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingProminentDisclosureAccepted
            ELSE 0
        END
    ) AS num_OnboardingProminentDisclosureAccepted_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingProminentDisclosureAccepted
            ELSE 0
        END
    ) AS num_OnboardingProminentDisclosureAccepted_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingProminentDisclosureAccepted
            ELSE 0
        END
    ) AS num_OnboardingProminentDisclosureAccepted_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingSettingsAccepted
            ELSE 0
        END
    ) AS num_OnboardingSettingsAccepted_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingSettingsAccepted
            ELSE 0
        END
    ) AS num_OnboardingSettingsAccepted_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingSettingsAccepted
            ELSE 0
        END
    ) AS num_OnboardingSettingsAccepted_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingSkipKhataClick
            ELSE 0
        END
    ) AS num_OnboardingSkipKhataClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingSkipKhataClick
            ELSE 0
        END
    ) AS num_OnboardingSkipKhataClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingSkipKhataClick
            ELSE 0
        END
    ) AS num_OnboardingSkipKhataClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingStartKhataClick
            ELSE 0
        END
    ) AS num_OnboardingStartKhataClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingStartKhataClick
            ELSE 0
        END
    ) AS num_OnboardingStartKhataClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingStartKhataClick
            ELSE 0
        END
    ) AS num_OnboardingStartKhataClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingSwitchLanguageClick
            ELSE 0
        END
    ) AS num_OnboardingSwitchLanguageClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingSwitchLanguageClick
            ELSE 0
        END
    ) AS num_OnboardingSwitchLanguageClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingSwitchLanguageClick
            ELSE 0
        END
    ) AS num_OnboardingSwitchLanguageClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingTrueCallerError
            ELSE 0
        END
    ) AS num_OnboardingTrueCallerError_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingTrueCallerError
            ELSE 0
        END
    ) AS num_OnboardingTrueCallerError_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingTrueCallerError
            ELSE 0
        END
    ) AS num_OnboardingTrueCallerError_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingVerifyOtp
            ELSE 0
        END
    ) AS num_OnboardingVerifyOtp_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingVerifyOtp
            ELSE 0
        END
    ) AS num_OnboardingVerifyOtp_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingVerifyOtp
            ELSE 0
        END
    ) AS num_OnboardingVerifyOtp_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_OnboardingVerifyTruecallerLogin
            ELSE 0
        END
    ) AS num_OnboardingVerifyTruecallerLogin_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_OnboardingVerifyTruecallerLogin
            ELSE 0
        END
    ) AS num_OnboardingVerifyTruecallerLogin_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_OnboardingVerifyTruecallerLogin
            ELSE 0
        END
    ) AS num_OnboardingVerifyTruecallerLogin_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentAddBankAccount
            ELSE 0
        END
    ) AS num_PaymentAddBankAccount_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentAddBankAccount
            ELSE 0
        END
    ) AS num_PaymentAddBankAccount_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentAddBankAccount
            ELSE 0
        END
    ) AS num_PaymentAddBankAccount_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentKycSuccess
            ELSE 0
        END
    ) AS num_PaymentKycSuccess_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentKycSuccess
            ELSE 0
        END
    ) AS num_PaymentKycSuccess_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentKycSuccess
            ELSE 0
        END
    ) AS num_PaymentKycSuccess_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentKycUnderReview
            ELSE 0
        END
    ) AS num_PaymentKycUnderReview_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentKycUnderReview
            ELSE 0
        END
    ) AS num_PaymentKycUnderReview_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentKycUnderReview
            ELSE 0
        END
    ) AS num_PaymentKycUnderReview_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentPayNowClick
            ELSE 0
        END
    ) AS num_PaymentPayNowClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentPayNowClick
            ELSE 0
        END
    ) AS num_PaymentPayNowClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentPayNowClick
            ELSE 0
        END
    ) AS num_PaymentPayNowClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentPendingClick
            ELSE 0
        END
    ) AS num_PaymentPendingClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentPendingClick
            ELSE 0
        END
    ) AS num_PaymentPendingClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentPendingClick
            ELSE 0
        END
    ) AS num_PaymentPendingClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentReminderShareOnSms
            ELSE 0
        END
    ) AS num_PaymentReminderShareOnSms_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentReminderShareOnSms
            ELSE 0
        END
    ) AS num_PaymentReminderShareOnSms_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentReminderShareOnSms
            ELSE 0
        END
    ) AS num_PaymentReminderShareOnSms_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentReminderShareOnWhatsApp
            ELSE 0
        END
    ) AS num_PaymentReminderShareOnWhatsApp_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentReminderShareOnWhatsApp
            ELSE 0
        END
    ) AS num_PaymentReminderShareOnWhatsApp_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentReminderShareOnWhatsApp
            ELSE 0
        END
    ) AS num_PaymentReminderShareOnWhatsApp_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentRequestMoneyClick
            ELSE 0
        END
    ) AS num_PaymentRequestMoneyClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentRequestMoneyClick
            ELSE 0
        END
    ) AS num_PaymentRequestMoneyClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentRequestMoneyClick
            ELSE 0
        END
    ) AS num_PaymentRequestMoneyClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentRequestPermission
            ELSE 0
        END
    ) AS num_PaymentRequestPermission_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentRequestPermission
            ELSE 0
        END
    ) AS num_PaymentRequestPermission_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentRequestPermission
            ELSE 0
        END
    ) AS num_PaymentRequestPermission_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PaymentSaveClick
            ELSE 0
        END
    ) AS num_PaymentSaveClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PaymentSaveClick
            ELSE 0
        END
    ) AS num_PaymentSaveClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PaymentSaveClick
            ELSE 0
        END
    ) AS num_PaymentSaveClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PayoutCreated
            ELSE 0
        END
    ) AS num_PayoutCreated_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PayoutCreated
            ELSE 0
        END
    ) AS num_PayoutCreated_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PayoutCreated
            ELSE 0
        END
    ) AS num_PayoutCreated_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_PayoutSettled
            ELSE 0
        END
    ) AS num_PayoutSettled_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_PayoutSettled
            ELSE 0
        END
    ) AS num_PayoutSettled_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_PayoutSettled
            ELSE 0
        END
    ) AS num_PayoutSettled_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_SaveNewTransaction
            ELSE 0
        END
    ) AS num_SaveNewTransaction_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_SaveNewTransaction
            ELSE 0
        END
    ) AS num_SaveNewTransaction_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_SaveNewTransaction
            ELSE 0
        END
    ) AS num_SaveNewTransaction_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffAddPermissionClick
            ELSE 0
        END
    ) AS num_StaffAddPermissionClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffAddPermissionClick
            ELSE 0
        END
    ) AS num_StaffAddPermissionClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffAddPermissionClick
            ELSE 0
        END
    ) AS num_StaffAddPermissionClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffDetailAddPayment
            ELSE 0
        END
    ) AS num_StaffDetailAddPayment_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffDetailAddPayment
            ELSE 0
        END
    ) AS num_StaffDetailAddPayment_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffDetailAddPayment
            ELSE 0
        END
    ) AS num_StaffDetailAddPayment_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffDetailAttendanceMarked
            ELSE 0
        END
    ) AS num_StaffDetailAttendanceMarked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffDetailAttendanceMarked
            ELSE 0
        END
    ) AS num_StaffDetailAttendanceMarked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffDetailAttendanceMarked
            ELSE 0
        END
    ) AS num_StaffDetailAttendanceMarked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffItemClicked
            ELSE 0
        END
    ) AS num_StaffItemClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffItemClicked
            ELSE 0
        END
    ) AS num_StaffItemClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffItemClicked
            ELSE 0
        END
    ) AS num_StaffItemClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffMenuClicked
            ELSE 0
        END
    ) AS num_StaffMenuClicked_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffMenuClicked
            ELSE 0
        END
    ) AS num_StaffMenuClicked_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffMenuClicked
            ELSE 0
        END
    ) AS num_StaffMenuClicked_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffOpenInternetSettings
            ELSE 0
        END
    ) AS num_StaffOpenInternetSettings_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffOpenInternetSettings
            ELSE 0
        END
    ) AS num_StaffOpenInternetSettings_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffOpenInternetSettings
            ELSE 0
        END
    ) AS num_StaffOpenInternetSettings_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffTabPaymentSaved
            ELSE 0
        END
    ) AS num_StaffTabPaymentSaved_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffTabPaymentSaved
            ELSE 0
        END
    ) AS num_StaffTabPaymentSaved_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffTabPaymentSaved
            ELSE 0
        END
    ) AS num_StaffTabPaymentSaved_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffTabSalaryClick
            ELSE 0
        END
    ) AS num_StaffTabSalaryClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffTabSalaryClick
            ELSE 0
        END
    ) AS num_StaffTabSalaryClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffTabSalaryClick
            ELSE 0
        END
    ) AS num_StaffTabSalaryClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffTabSalaryDateSubmitClick
            ELSE 0
        END
    ) AS num_StaffTabSalaryDateSubmitClick_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffTabSalaryDateSubmitClick
            ELSE 0
        END
    ) AS num_StaffTabSalaryDateSubmitClick_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffTabSalaryDateSubmitClick
            ELSE 0
        END
    ) AS num_StaffTabSalaryDateSubmitClick_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StaffTabStaffAdded
            ELSE 0
        END
    ) AS num_StaffTabStaffAdded_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StaffTabStaffAdded
            ELSE 0
        END
    ) AS num_StaffTabStaffAdded_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StaffTabStaffAdded
            ELSE 0
        END
    ) AS num_StaffTabStaffAdded_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_StartUsingKhatabook
            ELSE 0
        END
    ) AS num_StartUsingKhatabook_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_StartUsingKhatabook
            ELSE 0
        END
    ) AS num_StartUsingKhatabook_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_StartUsingKhatabook
            ELSE 0
        END
    ) AS num_StartUsingKhatabook_16_to_30_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -7, cutoff_date)
            AND DATEADD(DAY, -1, cutoff_date) THEN num_SuccessfulPaymentCollected
            ELSE 0
        END
    ) AS num_SuccessfulPaymentCollected_1_to_7_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -15, cutoff_date)
            AND DATEADD(DAY, -8, cutoff_date) THEN num_SuccessfulPaymentCollected
            ELSE 0
        END
    ) AS num_SuccessfulPaymentCollected_8_to_15_d,
    sum(
        CASE
            WHEN event_dt BETWEEN DATEADD(DAY, -30, cutoff_date)
            AND DATEADD(DAY, -16, cutoff_date) THEN num_SuccessfulPaymentCollected
            ELSE 0
        END
    ) AS num_SuccessfulPaymentCollected_16_to_30_d
from
    (
        select
            b.*,
            a.cutoff_date
        from
            identifier($base_tbl) a
            INNER JOIN analytics.model.events_data_day_level b on a.user_id = b.user_id
            AND b.event_dt BETWEEN DATEADD(DAY, -30, a.cutoff_date)
            AND DATEADD(DAY, -1, a.cutoff_date)
    )
group by
    1,
    2
),

-- ── Level-2 CTE: composite high-signal features from raw event counts ────────
derived as (
    select *,

        -- Payment intent: direct engagement with payment screens / outgoing actions
        COALESCE(num_PaymentPayNowClick_1_to_7_d, 0)
            + COALESCE(num_PaymentPendingClick_1_to_7_d, 0)
            + COALESCE(num_PaymentSaveClick_1_to_7_d, 0)
            + COALESCE(num_PayoutCreated_1_to_7_d, 0)
            + COALESCE(num_PayoutSettled_1_to_7_d, 0)
            + COALESCE(num_SuccessfulPaymentCollected_1_to_7_d, 0)
            + COALESCE(num_KhataRequestMoneyClick_1_to_7_d, 0)
            + COALESCE(num_CustomerKhataOnPaymentClicked_1_to_7_d, 0)
            + COALESCE(num_StaffDetailAddPayment_1_to_7_d, 0)
            + COALESCE(num_StaffTabPaymentSaved_1_to_7_d, 0)
            as payment_intent_events_1_to_7_d,

        COALESCE(num_PaymentPayNowClick_8_to_15_d, 0)
            + COALESCE(num_PaymentPendingClick_8_to_15_d, 0)
            + COALESCE(num_PaymentSaveClick_8_to_15_d, 0)
            + COALESCE(num_PayoutCreated_8_to_15_d, 0)
            + COALESCE(num_PayoutSettled_8_to_15_d, 0)
            + COALESCE(num_SuccessfulPaymentCollected_8_to_15_d, 0)
            + COALESCE(num_KhataRequestMoneyClick_8_to_15_d, 0)
            + COALESCE(num_CustomerKhataOnPaymentClicked_8_to_15_d, 0)
            + COALESCE(num_StaffDetailAddPayment_8_to_15_d, 0)
            + COALESCE(num_StaffTabPaymentSaved_8_to_15_d, 0)
            as payment_intent_events_8_to_15_d,

        COALESCE(num_PaymentPayNowClick_16_to_30_d, 0)
            + COALESCE(num_PaymentPendingClick_16_to_30_d, 0)
            + COALESCE(num_PaymentSaveClick_16_to_30_d, 0)
            + COALESCE(num_PayoutCreated_16_to_30_d, 0)
            + COALESCE(num_PayoutSettled_16_to_30_d, 0)
            + COALESCE(num_SuccessfulPaymentCollected_16_to_30_d, 0)
            + COALESCE(num_KhataRequestMoneyClick_16_to_30_d, 0)
            + COALESCE(num_CustomerKhataOnPaymentClicked_16_to_30_d, 0)
            + COALESCE(num_StaffDetailAddPayment_16_to_30_d, 0)
            + COALESCE(num_StaffTabPaymentSaved_16_to_30_d, 0)
            as payment_intent_events_16_to_30_d,

        -- Business health: core khata/transaction activity (measures business liquidity)
        COALESCE(num_SaveNewTransaction_1_to_7_d, 0)
            + COALESCE(num_NewCustomerAdded_1_to_7_d, 0)
            + COALESCE(num_SuccessfulPaymentCollected_1_to_7_d, 0)
            + COALESCE(num_PayoutCreated_1_to_7_d, 0)
            + COALESCE(num_CustomerKhataOnTransactionClicked_1_to_7_d, 0)
            + COALESCE(num_CustomerKhataOnGaveClicked_1_to_7_d, 0)
            + COALESCE(num_CustomerKhataOnGotClicked_1_to_7_d, 0)
            as business_health_events_1_to_7_d,

        COALESCE(num_SaveNewTransaction_16_to_30_d, 0)
            + COALESCE(num_NewCustomerAdded_16_to_30_d, 0)
            + COALESCE(num_SuccessfulPaymentCollected_16_to_30_d, 0)
            + COALESCE(num_PayoutCreated_16_to_30_d, 0)
            + COALESCE(num_CustomerKhataOnTransactionClicked_16_to_30_d, 0)
            + COALESCE(num_CustomerKhataOnGaveClicked_16_to_30_d, 0)
            + COALESCE(num_CustomerKhataOnGotClicked_16_to_30_d, 0)
            as business_health_events_16_to_30_d,

        -- App engagement: core navigation clicks (strongest proxy for daily app opens)
        COALESCE(num_MainBillsClick_1_to_7_d, 0)
            + COALESCE(num_MainMoneyClick_1_to_7_d, 0)
            + COALESCE(num_MainMoreClick_1_to_7_d, 0)
            + COALESCE(num_MainNewKhataClick_1_to_7_d, 0)
            + COALESCE(num_MainShowBooks_1_to_7_d, 0)
            + COALESCE(num_MainInventoryClick_1_to_7_d, 0)
            as app_engagement_events_1_to_7_d,

        COALESCE(num_MainBillsClick_16_to_30_d, 0)
            + COALESCE(num_MainMoneyClick_16_to_30_d, 0)
            + COALESCE(num_MainMoreClick_16_to_30_d, 0)
            + COALESCE(num_MainNewKhataClick_16_to_30_d, 0)
            + COALESCE(num_MainShowBooks_16_to_30_d, 0)
            + COALESCE(num_MainInventoryClick_16_to_30_d, 0)
            as app_engagement_events_16_to_30_d,

        -- Financial stress awareness: checking credit score or balance screen
        COALESCE(num_CreditScoreCheckNowClick_1_to_7_d, 0)
            + COALESCE(num_CreditScoreFetchSuccess_1_to_7_d, 0)
            + COALESCE(num_MainMoneyClick_1_to_7_d, 0)
            as financial_awareness_events_1_to_7_d

    from base_activity
)

-- ── Final SELECT: add ratio / flag features that reference the composites ─────
select *,

    -- Trend ratios (recent 1-7d vs older 16-30d): value > 1 = improving engagement
    case
        when (payment_intent_events_16_to_30_d + 1) > 0
        then payment_intent_events_1_to_7_d::float / (payment_intent_events_16_to_30_d + 1)
        else null
    end as payment_intent_trend_ratio,

    case
        when (app_engagement_events_16_to_30_d + 1) > 0
        then app_engagement_events_1_to_7_d::float / (app_engagement_events_16_to_30_d + 1)
        else null
    end as app_engagement_trend_ratio,

    case
        when (business_health_events_16_to_30_d + 1) > 0
        then business_health_events_1_to_7_d::float / (business_health_events_16_to_30_d + 1)
        else null
    end as business_health_trend_ratio,

    -- Repayment focus: fraction of recent app engagement that is payment-directed
    case
        when (app_engagement_events_1_to_7_d + 1) > 0
        then payment_intent_events_1_to_7_d::float / (app_engagement_events_1_to_7_d + 1)
        else null
    end as repayment_focus_ratio_1_to_7_d,

    -- Hard negative: zero payment intent events in last 7 days
    case when payment_intent_events_1_to_7_d = 0 then 1 else 0 end
        as no_payment_activity_last_7d_flag,

    -- Hard negative: was active 16-30d ago but completely dark in last 7 days
    case
        when app_engagement_events_16_to_30_d > 3
         and app_engagement_events_1_to_7_d   = 0
        then 1 else 0
    end as business_activity_dropoff_flag,

    -- Hard positive: any direct payment action (PayNow / Save / PayoutCreated) in last 7d
    case
        when COALESCE(num_PaymentPayNowClick_1_to_7_d, 0)
           + COALESCE(num_PaymentSaveClick_1_to_7_d, 0)
           + COALESCE(num_PayoutCreated_1_to_7_d, 0) > 0
        then 1 else 0
    end as has_direct_payment_action_last_7d_flag

from derived;

select
    count(*)
from
    analytics.data_science.all_activity_features_for_early_dpd3
limit
    100;
