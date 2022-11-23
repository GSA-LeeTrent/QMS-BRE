DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` PROCEDURE `update_stfacq_sys_users`()
BEGIN
	SET SQL_SAFE_UPDATES=0;
	delete from stfacq_sys_user;

	insert into stfacq_sys_user
	select user_id
		  ,email
		  ,last_name
		  ,first_name
		  ,user_status
		  ,'MGS' System_Name
	FROM hiring.mgs_mstr_adhoc_g_user_management
	WHERE email IS NOT NULL;
END$$
DELIMITER ;
