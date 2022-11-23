DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` PROCEDURE `Update_master_error_list`()
BEGIN
    declare finished integer default 0;
    declare records_inserted integer default 0;
    declare records_updated integer default 0;
	declare rec_count_match integer default 0;
    declare hrdw_error_list_id integer;
    declare hrdw_data_item_id integer;
    declare hrdw_qms_error_code varchar(25);
    declare hrdw_error_message_text text;  
    declare hrdw_qms_data_load_enabled char(1);

	DECLARE cur_hrdw_data_items CURSOR FOR 
						select error_list_id 
                          ,data_item_id
						  ,QMS_ERROR_CODE
						  ,QMS_ERROR_MESSAGE_TEXT
						  ,ENABLE_QMS
						from new_hrdw.nhrdw_qms_master_error_list
					   where error_list_id IN (SELECT error_list_id
                                                 FROM qms_master_error_list
											    WHERE qms_data_load_enabled = 'Y');
                        
	DECLARE CONTINUE HANDLER 
			FOR NOT FOUND SET finished = 1;   
   
		open cur_hrdw_data_items;
        
        getDataElements : LOOP
			FETCH cur_hrdw_data_items INTO hrdw_error_list_id, hrdw_data_item_id, hrdw_qms_error_code, hrdw_error_message_text,hrdw_qms_data_load_enabled;
            if finished = 1 then 
				leave getDataElements;
            end if;
            
			set rec_count_match = (select count(*) from qms_master_error_list where qms_error_code = hrdw_qms_error_code);
			IF rec_count_match = 1 THEN 
				set records_updated = records_updated + 1;
				update qms_master_error_list set 
                    error_list_id = hrdw_error_list_id
			       ,data_item_id = hrdw_data_item_id
				   ,error_message_text = hrdw_error_message_text
                   ,hrdw_data_load_enabled = hrdw_qms_data_load_enabled
				   ,updated_at = now()  
				where error_list_id = hrdw_error_list_id;
			ELSE 
				set records_inserted = records_inserted + 1;
				insert into qms_master_error_list (error_list_id, data_item_id, qms_error_code, error_message_text, hrdw_data_load_enabled, created_at,errorRoutingTypeId,dataRoutingTypeId,qms_data_load_enabled) 
                                       values (hrdw_error_list_id, hrdw_data_item_id, hrdw_qms_error_code, hrdw_error_message_text,hrdw_qms_data_load_enabled, now(), 1, 1,'Y');
			END IF;
		END LOOP getDataElements;
    CLOSE cur_hrdw_data_items;
   
END$$
DELIMITER ;
