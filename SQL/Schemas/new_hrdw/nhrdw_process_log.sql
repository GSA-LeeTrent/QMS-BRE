SELECT * FROM new_hrdw.nhrdw_process_log 
WHERE CREATION_DATE > '2022-12-01'
AND CREATION_DATE < '2022-12-02';

-- Refresh QMS Staffing Error Checking SQL State: 42S22 Error Number : 1054 Error Text : Unknown column 'l_transaction_control_id' in 'field list'
-- nhrdw_run_snapshot SQL State: 42S22 Error Number : 1054 Error Text : Unknown column 'l_transaction_control_id' in 'field list'

SELECT * FROM new_hrdw.nhrdw_process_log 
WHERE CREATION_DATE > '2022-12-15';

-- Refresh QMS Staffing Error Checking SQL State: 42S22 Error Number : 1054 Error Text : Unknown column 'l_transaction_control_id' in 'field list'
=======
WHERE CREATION_DATE > '2022-12-13';
