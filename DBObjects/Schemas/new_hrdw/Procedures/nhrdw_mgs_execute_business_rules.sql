DELIMITER $$
CREATE DEFINER=`HRDWCORPDATA`@`%` PROCEDURE `nhrdw_mgs_execute_business_rules`(
   in p_transaction_control_id            bigint
 )
begin

   delete from nhrdw_qms_notifications where transaction_control_id = p_transaction_control_id;
  
   call nhrdw_mgs_adhoc_g_vacancy_validate(p_transaction_control_id);
   call nhrdw_mgs_adhoc_g_cert_application_validate(p_transaction_control_id);
      
end$$
DELIMITER ;
