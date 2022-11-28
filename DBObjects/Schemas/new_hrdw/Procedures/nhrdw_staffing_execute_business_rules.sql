USE new_hrdw;
DROP PROCEDURE new_hrdw.nhrdw_staffing_execute_business_rules;
DELIMITER $$
CREATE PROCEDURE new_hrdw.nhrdw_staffing_execute_business_rules (
	   in p_transaction_control_id            bigint
	 )
begin
	  
	   call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'BEGIN: nhrdw_staffing_execute_business_rules');
	   
	   delete from nhrdw_qms_notifications where transaction_control_id = p_transaction_control_id;
	  
	   call nhrdw_uss_mstr_requests_validate(p_transaction_control_id);
	   call nhrdw_mgs_adhoc_g_vacancy_validate(p_transaction_control_id);
	   call nhrdw_mgs_adhoc_g_cert_application_validate(p_transaction_control_id);
	   call nhrdw_star_recruit_action_status_validate(p_transaction_control_id);
	   
	   call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'END: nhrdw_staffing_execute_business_rules');
	      
	end$$
DELIMITER ;
