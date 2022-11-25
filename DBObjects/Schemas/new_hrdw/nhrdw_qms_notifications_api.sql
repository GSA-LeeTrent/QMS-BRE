DELIMITER $$
CREATE DEFINER=`HRDWCORPDATA`@`%` PROCEDURE `nhrdw_qms_notifications_api`(
    in p_transaction_control_id         bigint            
--   ,in p_qms_key                        varchar(255)  
   ,in p_system_name                    varchar(60) 
   ,in p_data_item_name                 varchar(60) 
   ,in p_qms_error_code                	varchar(30)  
   ,in p_notification_email_address     varchar(255)  
   ,in p_key_field_1              		  varchar(255)  
   ,in p_key_field_2              		  varchar(255)  
   ,in p_key_field_3              		  varchar(255)  
   ,in p_key_field_4              		  varchar(255)  
   ,in p_key_field_5              		  varchar(255)  
   ,in p_key_field_6              		  varchar(255)  
   ,in p_key_field_7              		  varchar(255)  
   ,in p_key_field_8              		  varchar(255)  
   ,in p_key_field_9              		  varchar(255)  
   ,in p_key_field_10              		  varchar(255)  
   ,in p_field_1              		      varchar(255)  
   ,in p_field_2              		      varchar(255)  
   ,in p_field_3              		      varchar(255)  
   ,in p_field_4              		      varchar(255)  
   ,in p_field_5              		      varchar(255)  
   ,in p_field_6              		      varchar(255)  
   ,in p_field_7              		      varchar(255)  
   ,in p_field_8              		      varchar(255)  
   ,in p_field_9              		      varchar(255)  
   ,in p_field_10              		      varchar(255)  
   ,in p_qms_routing_key_field_1        varchar(255)   
   ,in p_qms_routing_key_field_2        varchar(255)   
   ,in p_qms_routing_key_field_3        varchar(255)   
   ,in p_qms_routing_key_field_4        varchar(255)   
   ,in p_qms_routing_key_field_5        varchar(255)   
)
begin
  #variables
  declare l_rownotfound                int default false;
  declare l_descr                      varchar(255) default null;
  declare l_qms_short_description      varchar(255) default null;
  declare l_notification_message_text  text default null;
  declare l_notification_key_text      text default null;
  declare l_error_list_id              bigint default null;
  declare l_qms_key                    varchar(255) default null;
  declare l_qms_routing_key            varchar(255) default null;
  
    #cursors
  
  declare c_errlist_cur cursor for
  select  qms_error_message_text
         ,qms_key_text
         ,error_list_id
         ,qms_short_description
  from    nhrdw_data_item  dt       
          join    nhrdw_qms_master_error_list qmel on  qmel.qms_error_code = p_qms_error_code and dt.data_item_id = qmel.data_item_id
  where   dt.system_name = p_system_name
  and     dt.data_item_name = p_data_item_name
  and     dt.eff_status = 'A'
  and     dt.effdt = ( select max(dt_ed.effdt) from nhrdw_data_item dt_ed
                       where dt.system_name = dt_ed.system_name
                       and   dt.data_item_name = dt_ed.data_item_name
                       and   dt_ed.eff_status = 'A' 
                       and   dt_ed.effdt <= curdate() );
  
 declare continue handler for not found set l_rownotfound = true;

 declare exit handler for sqlexception, sqlwarning
 begin
    rollback;
    resignal;
 end;  

  set l_rownotfound := false;                


  open  c_errlist_cur;    
  fetch c_errlist_cur into l_notification_message_text,l_notification_key_text,l_error_list_id,l_qms_short_description;
  close c_errlist_cur;
  
  
     set l_qms_key         = concat_ws(':',p_key_field_1,p_key_field_2,p_key_field_3,p_key_field_4,p_key_field_5,p_key_field_6,p_key_field_7,p_key_field_8,p_key_field_9,p_key_field_10);
     set l_qms_routing_key = concat_ws(':',p_qms_routing_key_field_1,p_qms_routing_key_field_2,p_qms_routing_key_field_3,p_qms_routing_key_field_4,p_qms_routing_key_field_5);
     
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_01',COALESCE(p_key_field_1,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_02',COALESCE(p_key_field_2,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_03',COALESCE(p_key_field_3,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_04',COALESCE(p_key_field_4,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_05',COALESCE(p_key_field_5,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_06',COALESCE(p_key_field_6,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_07',COALESCE(p_key_field_7,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_08',COALESCE(p_key_field_8,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_09',COALESCE(p_key_field_9,' '));
     set l_notification_key_text := replace(l_notification_key_text,'&NHRDW_FILLER_10',COALESCE(p_key_field_10,' '));
  
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_01',COALESCE(p_field_1,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_02',COALESCE(p_field_2,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_03',COALESCE(p_field_3,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_04',COALESCE(p_field_4,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_05',COALESCE(p_field_5,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_06',COALESCE(p_field_6,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_07',COALESCE(p_field_7,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_08',COALESCE(p_field_8,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_09',COALESCE(p_field_9,' '));
     set l_notification_message_text := replace(l_notification_message_text,'&NHRDW_FILLER_10',COALESCE(p_field_10,' '));
     
     insert into nhrdw_qms_notifications               
      set                    
         transaction_control_id             = p_transaction_control_id 
        ,qms_key                            = l_qms_key    
        ,qms_short_description              = l_qms_short_description  
        ,error_list_id                      = l_error_list_id
        ,qms_key_field_1                    = p_key_field_1   
        ,qms_key_field_2                    = p_key_field_2   
        ,qms_key_field_3                    = p_key_field_3   
        ,qms_key_field_4                    = p_key_field_4   
        ,qms_key_field_5                    = p_key_field_5   
        ,qms_key_field_6                    = p_key_field_6   
        ,qms_key_field_7                    = p_key_field_7   
        ,qms_key_field_8                    = p_key_field_8   
        ,qms_key_field_9                    = p_key_field_9   
        ,qms_key_field_10                   = p_key_field_10  
        ,field_1                            = p_field_1   
        ,field_2                            = p_field_2   
        ,field_3                            = p_field_3   
        ,field_4                            = p_field_4   
        ,field_5                            = p_field_5   
        ,field_6                            = p_field_6   
        ,field_7                            = p_field_7   
        ,field_8                            = p_field_8   
        ,field_9                            = p_field_9   
        ,field_10                           = p_field_10  
        ,qms_notification_text              = l_notification_message_text		
        ,qms_key_text                       = l_notification_key_text	
        ,notification_email_address         = p_notification_email_address 	
        ,qms_routing_key                    = l_qms_routing_key          
        ,qms_routing_key_field_1            = p_qms_routing_key_field_1  
        ,qms_routing_key_field_2            = p_qms_routing_key_field_2  
        ,qms_routing_key_field_3            = p_qms_routing_key_field_3  
        ,qms_routing_key_field_4            = p_qms_routing_key_field_4  
        ,qms_routing_key_field_5            = p_qms_routing_key_field_5  
       ;         
   
     commit;

end$$
DELIMITER ;
