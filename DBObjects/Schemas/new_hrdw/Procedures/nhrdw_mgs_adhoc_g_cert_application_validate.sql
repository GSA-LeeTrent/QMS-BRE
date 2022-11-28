USE new_hrdw;
DROP PROCEDURE nhrdw_mgs_adhoc_g_cert_application_validate;
DELIMITER $$
CREATE PROCEDURE nhrdw_mgs_adhoc_g_cert_application_validate(
   in p_transaction_control_id            bigint
 )
begin


  #variables
  declare l_errmsg  TEXT;  
  declare l_hrdw_version varchar(200)  default 'HRDW Patch 87.0';
  declare l_rownotfound                int default false;
  declare l_descr                      varchar(255) default null;
  declare l_notification_message_text  text default null;
  declare l_notification_key_text      text default null;
  declare l_error_list_id              bigint default null;
  declare l_qms_key                    varchar(255) default null;
  declare l_proc                       varchar(200);



  declare l_fk_an_cert_id                         bigint default null;    
  declare l_fk_an_cert_sequence_number            bigint default null;    
  declare l_fk_an_applicant_id                    bigint default null;    
  declare l_fk_an_vacancy_id                      bigint default null;    
  declare l_application_grade                     varchar(255) default null;    
  declare l_application_status_display            varchar(255) default null;    
  declare l_application_public_status             varchar(255) default null;    
  declare l_certificate_name                      varchar(255) default null;    
  declare l_applicant_name                        varchar(255) default null;    
  declare l_applicant_email                       varchar(255) default null;    
  declare l_applicant_par_number                  varchar(255) default null;    
  declare l_certificate_notif_recps               varchar(1000) default null;    
  declare l_vacancy_announcement_number           varchar(255) default null; 
  declare l_vacancy_cpdf_code                     varchar(255) default null;    
  declare l_user_email_address                    varchar(255) default null; 
  declare l_applicant_last_name                   varchar(255) default null; 
  declare l_application_date                      date;
  declare l_staging_area_number                   varchar(50) default null;     

  declare l_qms_routing_key_field_1            varchar(255)   default null;    
  declare l_qms_routing_key_field_2            varchar(255)   default null;    
  declare l_qms_routing_key_field_3            varchar(255)   default null;    
  declare l_qms_routing_key_field_4            varchar(255)   default null;    
  declare l_qms_routing_key_field_5            varchar(255)   default null; 
  
  #cursors
  
  declare c_cert_app_par_number_is_null cursor for
  select   mgs_mstr_adhoc_g_cert_application.fk_an_cert_id 
          ,mgs_mstr_adhoc_g_cert_application.fk_an_cert_sequence_number
          ,mgs_mstr_adhoc_g_cert_application.fk_an_applicant_id
          ,mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id
          ,mgs_mstr_adhoc_g_cert_application.application_grade
          ,mgs_mstr_adhoc_g_cert_application.application_status_display
          ,mgs_mstr_adhoc_g_cert_application.application_public_status
          ,mgs_mstr_adhoc_g_certificate.certificate_name
          ,mgs_mstr_adhoc_g_applicant.applicant_email
          ,mgs_mstr_adhoc_g_applicant.applicant_last_name
          ,concat(COALESCE(mgs_mstr_adhoc_g_applicant.applicant_last_name,' '), ', ', COALESCE(mgs_mstr_adhoc_g_applicant.applicant_first_name,' '))
          ,mgs_mstr_adhoc_g_certificate.staging_area_number
          ,mgs_mstr_adhoc_g_certificate.certificate_notif_recps
          ,mgs_mstr_adhoc_g_cert_application.applicant_par_number
          ,mgs_mstr_adhoc_g_vacancy.vacancy_announcement_number     
          ,mgs_mstr_adhoc_g_vacancy.vacancy_cpdf_code    
          ,mgs_mstr_adhoc_g_cert_application.application_date  
          ,mgs_mstr_adhoc_g_user_management.email
  from    hiring.mgs_mstr_adhoc_g_cert_application
  join hiring.mgs_mstr_adhoc_g_certificate  on              mgs_mstr_adhoc_g_cert_application.fk_an_organization_id      = mgs_mstr_adhoc_g_certificate.fk_c_organization_id
                                                and         mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id           = mgs_mstr_adhoc_g_certificate.fk_c_vacancy_id
                                                and         mgs_mstr_adhoc_g_cert_application.fk_an_cert_id              = mgs_mstr_adhoc_g_certificate.certificate_id 
                                                and         mgs_mstr_adhoc_g_cert_application.fk_an_cert_sequence_number = mgs_mstr_adhoc_g_certificate.certificate_sequence_number
  join hiring.mgs_mstr_adhoc_g_applicant    on              mgs_mstr_adhoc_g_applicant.applicant_id                      = mgs_mstr_adhoc_g_cert_application.fk_an_applicant_id
  join hiring.mgs_mstr_adhoc_g_vacancy      on              mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id           = mgs_mstr_adhoc_g_vacancy.vacancy_id  
  join hiring.mgs_mstr_adhoc_g_application  on              mgs_mstr_adhoc_g_cert_application.fk_an_organization_id      = mgs_mstr_adhoc_g_application.fk_an_organization_id
                                                    and     mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id           = mgs_mstr_adhoc_g_application.fk_an_vacancy_id
                                                    and     mgs_mstr_adhoc_g_cert_application.application_grade          = mgs_mstr_adhoc_g_application.application_grade
                                                    and     mgs_mstr_adhoc_g_cert_application.fk_an_applicant_id         = mgs_mstr_adhoc_g_application.fk_an_applicant_id
                                                    and     mgs_mstr_adhoc_g_application.application_status    = 900
                                                    and     mgs_mstr_adhoc_g_application.application_status_sa = 900
                                                    and     mgs_mstr_adhoc_g_application.APPLICATION_SELECT_DATE >= '2021-10-01'
    left outer join hiring.mgs_mstr_adhoc_g_user_management          on  mgs_mstr_adhoc_g_user_management.user_id = mgs_mstr_adhoc_g_certificate.certificate_created_by 
--  left outer join hiring.mgs_mstr_adhoc_g_users                    on  mgs_mstr_adhoc_g_users.user_id = mgs_mstr_adhoc_g_certificate.certificate_created_by 
  where     mgs_mstr_adhoc_g_cert_application.applicant_par_number is  null;
 

  declare c_cert_app_par_number_format cursor for
  select   mgs_mstr_adhoc_g_cert_application.fk_an_cert_id 
          ,mgs_mstr_adhoc_g_cert_application.fk_an_cert_sequence_number
          ,mgs_mstr_adhoc_g_cert_application.fk_an_applicant_id
          ,mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id
          ,mgs_mstr_adhoc_g_cert_application.application_grade
          ,mgs_mstr_adhoc_g_cert_application.application_status_display
          ,mgs_mstr_adhoc_g_cert_application.application_public_status
          ,mgs_mstr_adhoc_g_certificate.certificate_name
          ,mgs_mstr_adhoc_g_applicant.applicant_email
          ,mgs_mstr_adhoc_g_applicant.applicant_last_name
          ,concat(COALESCE(mgs_mstr_adhoc_g_applicant.applicant_last_name,' '), ', ', COALESCE(mgs_mstr_adhoc_g_applicant.applicant_first_name,' '))
          ,mgs_mstr_adhoc_g_certificate.staging_area_number
          ,mgs_mstr_adhoc_g_certificate.certificate_notif_recps
          ,mgs_mstr_adhoc_g_cert_application.applicant_par_number
          ,mgs_mstr_adhoc_g_vacancy.vacancy_announcement_number   
          ,mgs_mstr_adhoc_g_vacancy.vacancy_cpdf_code   
          ,mgs_mstr_adhoc_g_cert_application.application_date 
          ,mgs_mstr_adhoc_g_user_management.email
  from    hiring.mgs_mstr_adhoc_g_cert_application
  join hiring.mgs_mstr_adhoc_g_certificate  on     mgs_mstr_adhoc_g_cert_application.fk_an_organization_id      = mgs_mstr_adhoc_g_certificate.fk_c_organization_id
                                           and     mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id           = mgs_mstr_adhoc_g_certificate.fk_c_vacancy_id
                                           and     mgs_mstr_adhoc_g_cert_application.fk_an_cert_id              = mgs_mstr_adhoc_g_certificate.certificate_id 
                                           and     mgs_mstr_adhoc_g_cert_application.fk_an_cert_sequence_number = mgs_mstr_adhoc_g_certificate.certificate_sequence_number
  join hiring.mgs_mstr_adhoc_g_applicant    on     mgs_mstr_adhoc_g_applicant.applicant_id                      = mgs_mstr_adhoc_g_cert_application.fk_an_applicant_id
  join hiring.mgs_mstr_adhoc_g_vacancy      on     mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id           = mgs_mstr_adhoc_g_vacancy.vacancy_id  
  join hiring.mgs_mstr_adhoc_g_application  on     mgs_mstr_adhoc_g_cert_application.fk_an_organization_id      = mgs_mstr_adhoc_g_application.fk_an_organization_id
                                           and     mgs_mstr_adhoc_g_cert_application.fk_an_vacancy_id           = mgs_mstr_adhoc_g_application.fk_an_vacancy_id
                                           and     mgs_mstr_adhoc_g_cert_application.application_grade          = mgs_mstr_adhoc_g_application.application_grade
                                           and     mgs_mstr_adhoc_g_cert_application.fk_an_applicant_id         = mgs_mstr_adhoc_g_application.fk_an_applicant_id
                                           and     mgs_mstr_adhoc_g_application.application_status    = 900
                                           and     mgs_mstr_adhoc_g_application.application_status_sa = 900
                                           and     mgs_mstr_adhoc_g_application.APPLICATION_SELECT_DATE >= '2021-10-01'
    left outer join hiring.mgs_mstr_adhoc_g_user_management          on  mgs_mstr_adhoc_g_user_management.user_id = mgs_mstr_adhoc_g_certificate.certificate_created_by 
--  left outer join hiring.mgs_mstr_adhoc_g_users                    on  mgs_mstr_adhoc_g_users.user_id = mgs_mstr_adhoc_g_certificate.certificate_created_by 
  where   mgs_mstr_adhoc_g_cert_application.applicant_par_number is not null 
  and     mgs_mstr_adhoc_g_cert_application.applicant_par_number not REGEXP  '^[0-9]+[CU]$';
  
  

  
 declare continue handler for not found set l_rownotfound = true;

 declare exit handler for sqlexception, sqlwarning
 begin
    rollback;
    resignal;
 end;  

  call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'BEGIN: nhrdw_mgs_adhoc_g_cert_application_validate');  
  
  set l_rownotfound := false;

  open c_cert_app_par_number_is_null;  

  c_cert_app_par_number_is_null_loop: loop 

       set l_rownotfound := false;                
	     set l_proc := 'Certificate Application PAR Number Null Cursor';    
       
	     fetch  c_cert_app_par_number_is_null into 
                                    l_fk_an_cert_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                   ,l_fk_an_cert_sequence_number                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                                   ,l_fk_an_applicant_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                                   ,l_fk_an_vacancy_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                                   ,l_application_grade                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
                                   ,l_application_status_display                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                                   ,l_application_public_status                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                                   ,l_certificate_name                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                                   ,l_applicant_email                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
                                   ,l_applicant_last_name                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
                                   ,l_applicant_name         
                                   ,l_staging_area_number                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
                                   ,l_certificate_notif_recps                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
                                   ,l_applicant_par_number                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
                                   ,l_vacancy_announcement_number  
                                   ,l_vacancy_cpdf_code                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                                   ,l_application_date                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                                   ,l_user_email_address                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                                   ;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       if ( l_rownotfound = true )  
       then          
              close c_cert_app_par_number_is_null;
              leave c_cert_app_par_number_is_null_loop;
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
                                     ,'Hire PAR Number'
                                     ,'MGS:HIRPAR:001'      
                                     ,l_user_email_address
                                     ,'MGS'  -- Start of Key 
                                     ,'MGS:HIRPAR:001'  
                                     ,l_fk_an_cert_id
                                     ,l_fk_an_cert_sequence_number 
                                     ,l_fk_an_applicant_id 
                                     ,l_application_grade 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,l_vacancy_announcement_number  -- Start of Message Attributes 
                                     ,l_certificate_name 
                                     ,l_applicant_last_name
                                     ,l_staging_area_number
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

  open c_cert_app_par_number_format;  

  c_cert_app_par_number_format_loop: loop 

       set l_rownotfound := false;                
	     set l_proc := 'Certificate Application PAR Number';    
       
	     fetch  c_cert_app_par_number_format into 
                                    l_fk_an_cert_id                                      
                                   ,l_fk_an_cert_sequence_number                         
                                   ,l_fk_an_applicant_id                                 
                                   ,l_fk_an_vacancy_id                                   
                                   ,l_application_grade                                  
                                   ,l_application_status_display                         
                                   ,l_application_public_status                          
                                   ,l_certificate_name                                   
                                   ,l_applicant_email  
                                   ,l_applicant_last_name                                         
                                   ,l_applicant_name       
                                   ,l_staging_area_number                      
                                   ,l_certificate_notif_recps                        
                                   ,l_applicant_par_number                                  
                                   ,l_vacancy_announcement_number  
                                   ,l_vacancy_cpdf_code                      
                                   ,l_application_date  
                                   ,l_user_email_address                            
                                   ;


       if ( l_rownotfound = true )  
       then          
              close c_cert_app_par_number_format;
              leave c_cert_app_par_number_format_loop;
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
                                     ,'Hire PAR Number'
                                     ,'MGS:HIRPAR:002' 
                                     ,l_user_email_address
                                     ,'MGS'  -- Start of Key 
                                     ,'MGS:HIRPAR:002' 
                                     ,l_fk_an_cert_id
                                     ,l_fk_an_cert_sequence_number 
                                     ,l_fk_an_applicant_id 
                                     ,l_application_grade 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,l_vacancy_announcement_number  -- Start of Message Attributes 
                                     ,l_certificate_name 
                                     ,l_applicant_last_name
                                     ,l_applicant_par_number
                                     ,l_staging_area_number
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

  call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'END: nhrdw_mgs_adhoc_g_cert_application_validate');       
end$$
DELIMITER ;
