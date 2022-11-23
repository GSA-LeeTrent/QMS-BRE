DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` PROCEDURE `Update_data_errors`()
BEGIN
    declare finished integer default 0;
    declare records_inserted integer default 0;
    declare records_closed integer default 0;
	declare rec_count_match integer default 0;
    
    declare hrdw_data_error_key varchar(20);
    declare hrdw_error_list_id integer;
    declare hrdw_emplid varchar(8);
    declare hrdw_qms_error_code varchar(11);
    declare hrdw_assigned_to_org_id integer;
    declare qms_error_message_text text;
	declare reactivate_age integer default 3;

	DECLARE cur_hrdw_data_errors CURSOR FOR 
				select concat(n.emplid, n.qms_error_code) AS dataError_Key
					  ,n.error_list_id
					  ,n.emplid
					  ,n.qms_error_code
					  ,aca.getAssignToOrgId(n.gvt_poi,e.errorRoutingTypeId) AssignedToOrgId
					  ,n.qms_error_message_text
				from new_hrdw.nhrdw_qms_empl_error_tracker_current_v n join aca.qms_master_error_list e on n.error_list_id = e.error_list_id and e.qms_data_load_enabled = 'Y'
				where n.business_unit in('GSA01','GSA03');
    
	DECLARE CONTINUE HANDLER 
			FOR NOT FOUND SET finished = 1;   
   
		open cur_hrdw_data_errors;
        
       
        getDataElements : LOOP
			FETCH cur_hrdw_data_errors INTO hrdw_data_error_key, hrdw_error_list_id, hrdw_emplid, hrdw_qms_error_code,hrdw_assigned_to_org_id,qms_error_message_text;
            if finished = 1 then 
				leave getDataElements;
            end if;
            set rec_count_match = (select count(*) from aca.qms_DataError where dataError_Key = hrdw_data_error_key and ((resolved_at + INTERVAL @reactivate_age DAY > now()) or resolved_at is null) and deleted_at is null);

			IF rec_count_match = 0 THEN -- create record
				set records_inserted = records_inserted + 1;
				insert into aca.qms_DataError (dataError_Key,error_list_id, emplid,qms_error_code,AssignedToOrgId,CreatedByOrgId,qms_error_message_text,created_at) 
                                       values (hrdw_data_error_key,hrdw_error_list_id,hrdw_emplid,hrdw_qms_error_code,hrdw_assigned_to_org_id,hrdw_assigned_to_org_id,qms_error_message_text,now());
            END IF;
				
		END LOOP getDataElements;
    CLOSE cur_hrdw_data_errors;

	SET SQL_SAFE_UPDATES=0;
	update aca.qms_dataerror set
		   resolved_at = now() 
		  ,deleted_at = now()
		  ,statusid = 4
	where dataerror_key not in (select concat(n.emplid, n.qms_error_code) AS dataError_Key
								from new_hrdw.nhrdw_qms_empl_error_tracker_current_v n join aca.qms_master_error_list e on n.error_list_id = e.error_list_id and e.qms_data_load_enabled = 'Y'
							)
	  and resolved_at is null
	  and deleted_at is null;         
    
   -- select concat(records_inserted, ' records inserted, ', records_updated, ' records updated', records_closed, ' records closed');
END$$
DELIMITER ;
