DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` PROCEDURE `update_sftacq_errors`()
BEGIN
    declare finished integer default 0;
    declare unassigned_user_id int default 0;
    declare records_inserted integer default 0;
    declare records_closed integer default 0;
	declare rec_count_match integer default 0;
    declare reactivate_age integer default 3;
    
    declare hrdw_qms_key varchar(255);
    declare hrdw_data_item_id integer;
    declare hrdw_error_list_id integer;
    declare hrdw_system_name varchar(15);
    declare hrdw_qms_error_code varchar(255);
    declare hrdw_short_error_description varchar(255);
    declare hrdw_summary varchar(2000);
    declare hrdw_details varchar(4000);
    declare hrdw_assigned_to_user_id integer;
    declare hrdw_assigned_to_user_org_id integer;
    declare hrdw_user_exists integer;
    declare hrdw_assigned_org integer;
  
	DECLARE cur_onehr_errors CURSOR FOR 
		select qms_key 													hrdw_qms_key
			  ,data_item_id 											hrdw_data_item_id
			  ,error_list_id 											hrdw_error_list_id 
			  ,system_name 												hrdw_system_name
			  ,qms_error_code 											hrdw_qms_error_code
			  ,qms_key_text 											hrdw_summary
			  ,qms_notification_text 									hrdw_details
			  ,getUserIdByEmailOrName(notification_email_address)		hrdw_user_id
              ,checkIfUserExist(notification_email_address)  			hrdw_user_exists
              ,aca.getOrgByRoutingKey(qms_routing_key_field_3)          hrdw_assigned_org
              ,qms_short_description 									hrdw_short_error_description
		FROM new_hrdw.nhrdw_qms_notifications_current_v;

	DECLARE CONTINUE HANDLER 
			FOR NOT FOUND SET finished = 1;   
   
		set unassigned_user_id = (select getUserId('no-reply@gsa.gov'));
        set @eventid = (Select notificationevent_id from ntf_notificationevent where notificationevent_code = 'SA_Assigned');
   
		open cur_onehr_errors;
       
        getDataElements : LOOP
			FETCH cur_onehr_errors INTO hrdw_qms_key,hrdw_data_item_id,hrdw_error_list_id,hrdw_system_name,hrdw_qms_error_code,hrdw_summary,hrdw_details,hrdw_assigned_to_user_id, hrdw_user_exists,hrdw_assigned_org,hrdw_short_error_description;

            if finished = 1 then 
				leave getDataElements;
            end if;
			-- set rec_count_match = (select count(*) from qms_stfacq_error where qms_key = hrdw_qms_key and ((resolved_at < now() - reactivate_age) or resolved_at is null) and deleted_at is null);
            set rec_count_match = (select count(*) from qms_stfacq_error where qms_key = hrdw_qms_key and ((resolved_at + INTERVAL @reactivate_age DAY > now()) or resolved_at is null) and deleted_at is null);
			IF rec_count_match = 0 THEN -- create record
				set records_inserted = records_inserted + 1;
                if (hrdw_assigned_to_user_id = unassigned_user_id) THEN 
					insert into qms_stfacq_error (qms_key, data_item_id, error_list_id, system_name, qms_error_code, error_summary, error_details,assigned_to_user_id, assigned_to_org_id, assigned_by_user_id, created_at, row_version,status_id,short_error_description) 
						values (hrdw_qms_key,hrdw_data_item_id,hrdw_error_list_id,hrdw_system_name,hrdw_qms_error_code,hrdw_summary,hrdw_details,hrdw_assigned_to_user_id, hrdw_assigned_org,0,now(),1,1,hrdw_short_error_description);
				else
					   insert into qms_stfacq_error (qms_key, data_item_id, error_list_id, system_name, qms_error_code, error_summary, error_details,assigned_to_user_id, assigned_to_org_id, assigned_by_user_id, created_at, assigned_at, row_version,status_id,short_error_description) 
							values (hrdw_qms_key,hrdw_data_item_id,hrdw_error_list_id,hrdw_system_name,hrdw_qms_error_code,hrdw_summary,hrdw_details,hrdw_assigned_to_user_id, hrdw_assigned_org,0,now(),now(),1,2,hrdw_short_error_description);
                      
					   set @new_id = (select last_insert_id());
                       insert into ntf_notification(user_id, NotificationEvent_Id, workitem_id, WorkItemType_Code, Title, Message, sendasemail, CreatedAt, hasbeenread)
							values(hrdw_assigned_to_user_id, @eventid, @new_id, 'StaffAcquisition', 'Staffing Error Assigned (QMS)', concat('You have been assigned Staffing Error ', @new_id), 1, NOW(), 0);			    
                end if;	
            END IF;
		END LOOP getDataElements;
    CLOSE cur_onehr_errors;

	SET SQL_SAFE_UPDATES=0;
	update qms_stfacq_error set
		   resolved_at = now() 
		  -- ,deleted_at = now()
		  ,status_id = 4
	where qms_key not in (SELECT qms_key 
                          FROM new_hrdw.nhrdw_qms_notifications_current_v)
	  and resolved_at is null
	  and deleted_at is null;         
    
END$$
DELIMITER ;
