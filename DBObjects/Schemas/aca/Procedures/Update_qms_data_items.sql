DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` PROCEDURE `Update_qms_data_items`()
BEGIN
    declare finished integer default 0;
    declare records_inserted integer default 0;
    declare records_updated integer default 0;
	declare rec_count_match integer default 0;
    declare hrdw_data_item_id integer;
    declare hrdw_system_name varchar(25);
    declare hrdw_item_name varchar(100);
    declare hrdw_data_item_category varchar(25);  

	DECLARE cur_hrdw_data_items CURSOR FOR 
						select data_item_id
						  ,system_name
						  ,data_item_name
						  ,data_item_category
						from new_hrdw.nhrdw_data_item;
                        
	DECLARE CONTINUE HANDLER 
			FOR NOT FOUND SET finished = 1;   
   
		open cur_hrdw_data_items;
        
        getDataElements : LOOP
			FETCH cur_hrdw_data_items INTO hrdw_data_item_id, hrdw_system_name, hrdw_item_name, hrdw_data_item_category;
            if finished = 1 then 
				leave getDataElements;
            end if;
			set rec_count_match = (select count(*) from aca.qms_data_item where data_item_id = hrdw_data_item_id);
			IF rec_count_match = 1 THEN 
				set records_updated = records_updated + 1;
				update aca.qms_data_item set 
					system_name = hrdw_system_name
				   ,data_item_name = hrdw_item_name
				   ,data_item_category = hrdw_data_item_category
				   ,updated_at = now()  
				where data_item_id = hrdw_data_item_id;
			ELSE 
				set records_inserted = records_inserted + 1;
				insert into aca.qms_data_item (data_item_id, system_name, data_item_name, data_item_category, created_at) values (hrdw_data_item_id, hrdw_system_name, hrdw_item_name, hrdw_data_item_category, now());
			END IF;
		END LOOP getDataElements;
    CLOSE cur_hrdw_data_items;
    
END$$
DELIMITER ;
