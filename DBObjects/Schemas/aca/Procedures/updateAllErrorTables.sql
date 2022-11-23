DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` PROCEDURE `updateAllErrorTables`()
BEGIN
 declare emp_count int(10);
 set emp_count = (select count(*) T from hrlinks_employee);
 if emp_count > 11000 THEN
     -- REFERENCE TABLES
	 call update_qms_Employee();
	 call Update_qms_data_items();
	 call Update_master_error_list();
	 
	 -- EHRI DATA ERRORS
	 call Update_data_errors();
	 call Update_qms_data_error_stats();
     
	 -- POSITION CATEGORIZATION
 	 call epc_InsertNewEmployees();
     call epc_UpdateEmployeesDueToPositionChange();
     
	 -- STAFFING ERRORS
     call update_stfacq_sys_users();-- adding these sprocs to the update process
     call update_sftacq_errors(); 
 END IF;
END$$
DELIMITER ;
