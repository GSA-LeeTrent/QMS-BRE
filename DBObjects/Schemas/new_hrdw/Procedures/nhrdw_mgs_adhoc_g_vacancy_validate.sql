DELIMITER $$
CREATE DEFINER=`HRDWCORPDATA`@`%` PROCEDURE `nhrdw_mgs_adhoc_g_vacancy_validate`(
   in p_transaction_control_id            bigint
 )
begin


  #variables
  declare l_errmsg  TEXT;  
  declare l_hrdw_version varchar(200)  default 'HRDW Patch 80.0';
  declare l_rownotfound                int default false;
  declare l_descr                      varchar(255) default null;
  declare l_notification_message_text  text default null;
  declare l_notification_key_text      text default null;
  declare l_error_list_id              bigint default null;
  declare l_qms_key                    varchar(255) default null;
  declare l_proc                       varchar(200);



  declare l_vacancy_id                          bigint default null;    
  declare l_vacancy_announcement_number         varchar(255) default null;    
  declare l_vacancy_cpdf_code                   varchar(255) default null;    
  declare l_vacancy_hr_manager_name             varchar(255) default null;    
  declare l_vacancy_request_official            varchar(255) default null;    
  declare l_vacancy_request_no                  varchar(255) default null;    
  declare l_vacancy_open_date                   date;
  declare l_user_email_address                  varchar(255) default null;     

  declare l_qms_routing_key_field_1            varchar(255)   default null;    
  declare l_qms_routing_key_field_2            varchar(255)   default null;    
  declare l_qms_routing_key_field_3            varchar(255)   default null;    
  declare l_qms_routing_key_field_4            varchar(255)   default null;    
  declare l_qms_routing_key_field_5            varchar(255)   default null;    

     
  #cursors
  
  declare c_vacancy_number_is_null cursor for
  select   mgs_mstr_adhoc_g_vacancy.vacancy_id
          ,mgs_mstr_adhoc_g_vacancy.vacancy_announcement_number
          ,mgs_mstr_adhoc_g_vacancy.vacancy_cpdf_code
          ,mgs_mstr_adhoc_g_vacancy.vacancy_hr_manager_name 
          ,mgs_mstr_adhoc_g_vacancy.vacancy_open_date
          ,mgs_mstr_adhoc_g_vacancy.vacancy_request_official
          ,mgs_mstr_adhoc_g_vacancy.vacancy_request_no
          ,mgs_mstr_adhoc_g_user_management.email
  from    hiring.mgs_mstr_adhoc_g_vacancy  
  left outer join hiring.mgs_mstr_adhoc_g_department            on  mgs_mstr_adhoc_g_vacancy.fk_v_department_id = mgs_mstr_adhoc_g_department.department_id
  left outer join hiring.mgs_mstr_adhoc_g_user_management       on  mgs_mstr_adhoc_g_user_management.user_id = mgs_mstr_adhoc_g_vacancy.vacancy_hr_manager
  where   mgs_mstr_adhoc_g_vacancy.vacancy_created_date >= '2021-10-01' 
  and     mgs_mstr_adhoc_g_vacancy.vacancy_request_no is null;
  

  declare c_vacancy_number_format cursor for
  select   mgs_mstr_adhoc_g_vacancy.vacancy_id
          ,mgs_mstr_adhoc_g_vacancy.vacancy_announcement_number
          ,mgs_mstr_adhoc_g_vacancy.vacancy_cpdf_code
          ,mgs_mstr_adhoc_g_vacancy.vacancy_hr_manager_name 
          ,mgs_mstr_adhoc_g_vacancy.vacancy_open_date
          ,mgs_mstr_adhoc_g_vacancy.vacancy_request_official
          ,mgs_mstr_adhoc_g_vacancy.vacancy_request_no
          ,mgs_mstr_adhoc_g_user_management.email
  from    hiring.mgs_mstr_adhoc_g_vacancy  
  left outer join hiring.mgs_mstr_adhoc_g_department             on  mgs_mstr_adhoc_g_vacancy.fk_v_department_id = mgs_mstr_adhoc_g_department.department_id
  left outer join hiring.mgs_mstr_adhoc_g_user_management        on  mgs_mstr_adhoc_g_user_management.user_id = mgs_mstr_adhoc_g_vacancy.vacancy_hr_manager
  where   mgs_mstr_adhoc_g_vacancy.vacancy_created_date >= '2021-10-01' 
  and     mgs_mstr_adhoc_g_vacancy.vacancy_request_no is not null
  and     exists ( select 'x' from hiring.mgs_mstr_adhoc_g_vacancy_request_numbers
                   where mgs_mstr_adhoc_g_vacancy_request_numbers.vacancy_id = mgs_mstr_adhoc_g_vacancy.vacancy_id
                   and   mgs_mstr_adhoc_g_vacancy_request_numbers.request_number not regexp  '^[0-9]+[CU]$'
                 );
    
 declare continue handler for not found set l_rownotfound = true;

 declare exit handler for sqlexception, sqlwarning
 begin
    rollback;
    resignal;
 end;  
 
  call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'BEGIN: nhrdw_mgs_adhoc_g_vacancy_validate');  

  set l_rownotfound := false;

  open c_vacancy_number_is_null;  

  c_vacancy_number_is_null_loop: loop 

       set l_rownotfound := false;                
	     set l_proc := 'Vacancy Number Null Cursor';    
       
	     fetch  c_vacancy_number_is_null into 
                                    l_vacancy_id                        
                                   ,l_vacancy_announcement_number  
                                   ,l_vacancy_cpdf_code     
                                   ,l_vacancy_hr_manager_name           
                                   ,l_vacancy_open_date                 
                                   ,l_vacancy_request_official 
                                   ,l_vacancy_request_no         
                                   ,l_user_email_address     ;
                                   
       if ( l_rownotfound = true )  
       then          
              close c_vacancy_number_is_null;
              leave c_vacancy_number_is_null_loop;
       end if; 
       
       
       
       call nhrdw_mgs_routing_key (            
                                          l_vacancy_announcement_number  
                                         ,l_vacancy_cpdf_code 
                                         ,l_qms_routing_key_field_1         
                                         ,l_qms_routing_key_field_2         
                                         ,l_qms_routing_key_field_3         
                                         ,l_qms_routing_key_field_4         
                                         ,l_qms_routing_key_field_5         
                                  );
                                         
       call nhrdw_qms_notifications_api 
                                (
                                       p_transaction_control_id                                  
                                     ,'MGS'
                                     ,'Vacancy PAR Number'
                                     ,'MGS:VACPAR:001'      
                                     ,l_user_email_address
                                     ,'MGS'
                                     ,'MGS:VACPAR:001'     
                                     ,l_vacancy_id
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null  
                                     ,l_vacancy_announcement_number 
                                     ,date_format(l_vacancy_open_date,"%m-%d-%Y")
                                     ,l_vacancy_hr_manager_name
                                     ,l_vacancy_request_official
                                     ,null 
                                     ,null
                                     ,null
                                     ,null
                                     ,null
                                     ,null
                                     ,l_qms_routing_key_field_1 
                                     ,l_qms_routing_key_field_2 
                                     ,l_qms_routing_key_field_3 
                                     ,l_qms_routing_key_field_4 
                                     ,l_qms_routing_key_field_5 
                                  );                                            
       
 
     
  end loop;        


  set l_rownotfound := false;

  open c_vacancy_number_format;  

  c_vacancy_number_format_loop: loop 

       set l_rownotfound := false;                
	     set l_proc := 'Vacancy Number Format';    
       
	     fetch  c_vacancy_number_format into 
                                    l_vacancy_id                        
                                   ,l_vacancy_announcement_number       
                                   ,l_vacancy_cpdf_code     
                                   ,l_vacancy_hr_manager_name           
                                   ,l_vacancy_open_date                 
                                   ,l_vacancy_request_official 
                                   ,l_vacancy_request_no         
                                   ,l_user_email_address     ;
                                   
       if ( l_rownotfound = true )  
       then          
              close c_vacancy_number_format;
              leave c_vacancy_number_format_loop;
       end if; 
       
       call nhrdw_mgs_routing_key (            
                                          l_vacancy_announcement_number  
                                         ,l_vacancy_cpdf_code 
                                         ,l_qms_routing_key_field_1         
                                         ,l_qms_routing_key_field_2         
                                         ,l_qms_routing_key_field_3         
                                         ,l_qms_routing_key_field_4         
                                         ,l_qms_routing_key_field_5         
                                  );
       
       call nhrdw_qms_notifications_api 
                                (
                                       p_transaction_control_id                                                                                                                           
                                     ,'MGS'                                                                                                                       
                                     ,'Vacancy PAR Number'                                                                                                        
                                     ,'MGS:VACPAR:002'                                                                                                            
                                     ,l_user_email_address                                                                                                        
                                     ,'MGS'                                                                                                                       
                                     ,'MGS:VACPAR:002'                                                                                                            
                                     ,l_vacancy_id                                                                                                                
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                                                                                                              
                                     ,null                                                                                                                        
                                     ,l_vacancy_announcement_number                                                                                               
                                     ,l_vacancy_request_no                                                                                                        
                                     ,l_vacancy_hr_manager_name                                                                                                   
                                     ,l_vacancy_request_official                                                                                                  
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,null                                                                                                                        
                                     ,l_qms_routing_key_field_1                                                                                                                  
                                     ,l_qms_routing_key_field_2                                                                                                                  
                                     ,l_qms_routing_key_field_3                                                                                                                  
                                     ,l_qms_routing_key_field_4                                                                                                                  
                                     ,l_qms_routing_key_field_5                                                                               
                                  );                                            
       
 
     
  end loop;      

  call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'END: nhrdw_mgs_adhoc_g_vacancy_validate');  
       
end$$
DELIMITER ;
