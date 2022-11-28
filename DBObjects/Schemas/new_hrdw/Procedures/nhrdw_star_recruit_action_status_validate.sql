USE new_hrdw;
DROP PROCEDURE nhrdw_star_recruit_action_status_validate;
DELIMITER $$
CREATE PROCEDURE nhrdw_star_recruit_action_status_validate (
   in p_transaction_control_id            bigint
 )
begin


  #variables
  declare l_errmsg  TEXT;  
  declare l_hrdw_version varchar(200)  default 'HRDW Patch 94.0';
  declare l_rownotfound                int default false;
  declare l_descr                      varchar(255) default null;
  declare l_notification_message_text  text default null;
  declare l_notification_key_text      text default null;
  declare l_error_list_id              bigint default null;
  declare l_qms_key                    varchar(255) default null;
  declare l_proc                       varchar(200);



  declare l_rpa_number                        varchar(255) default null;    
  declare l_hr_consultant                     varchar(255) default null;    
  declare l_eff_date                          date;
  declare l_selectee_last_name                varchar(255) default null;    
  declare l_req_position_nbr                  varchar(255) default null;    
  declare l_req_posn_descr                    varchar(255) default null;    
  declare l_req_reports_to                    varchar(255) default null;    
  declare l_req_deptid                        varchar(255) default null;    
  declare l_transaction_status_descr          varchar(255) default null;     
  declare l_status_code                       varchar(2) default null;     
  declare l_center                            varchar(255) default null; 

  declare l_qms_routing_key_field_1            varchar(255)   default null;    
  declare l_qms_routing_key_field_2            varchar(255)   default null;    
  declare l_qms_routing_key_field_3            varchar(255)   default null;    
  declare l_qms_routing_key_field_4            varchar(255)   default null;    
  declare l_qms_routing_key_field_5            varchar(255)   default null;    

     
  #cursors
  
  declare c_star_eod cursor for
  select  nhrdw_star_recruit_action.rpa_number
  	     ,nhrdw_star_recruit_action.hr_consultant 
         ,nhrdw_star_recruit_action.eff_date
         ,nhrdw_star_recruit_action.selectee_last_name
         ,nhrdw_sf52_requests.req_position_nbr
         ,nhrdw_sf52_requests.req_posn_descr
         ,nhrdw_sf52_requests.req_reports_to
         ,nhrdw_sf52_requests.req_deptid
         ,nhrdw_sf52_requests.transaction_status_descr
         ,nhrdw_star_recruit_action.status_code
         ,nhrdw_star_recruit_action.center
  from  nhrdw_star_recruit_action 
  join   nhrdw_sf52_requests  on    nhrdw_sf52_requests.cadw_transaction_nbr = nhrdw_star_recruit_action.rpa_number and nhrdw_sf52_requests.transaction_type   IN ('NEW','PUF')
  where nhrdw_star_recruit_action.status_code = 20 
  and   nhrdw_star_recruit_action.eff_date >= '2021-10-01'
  and   nhrdw_sf52_requests.transaction_status_code != 'PRO';

  declare c_star_cancel cursor for
  select  nhrdw_star_recruit_action.rpa_number
  	     ,nhrdw_star_recruit_action.hr_consultant 
         ,nhrdw_star_recruit_action.rpa_cancelled
         ,nhrdw_star_recruit_action.selectee_last_name
         ,nhrdw_sf52_requests.req_position_nbr
         ,nhrdw_sf52_requests.req_posn_descr
         ,nhrdw_sf52_requests.req_reports_to
         ,nhrdw_sf52_requests.req_deptid
         ,nhrdw_sf52_requests.transaction_status_descr  
         ,nhrdw_star_recruit_action.status_code
		 ,nhrdw_star_recruit_action.center
   from  nhrdw_star_recruit_action 
   join   nhrdw_sf52_requests  on    nhrdw_sf52_requests.cadw_transaction_nbr = nhrdw_star_recruit_action.rpa_number and  nhrdw_sf52_requests.transaction_type   IN ('NEW','PUF')
   where nhrdw_star_recruit_action.status_code = 21 
   and   nhrdw_star_recruit_action.rpa_sent_to_staffing >= '2021-10-01'
   and   nhrdw_sf52_requests.transaction_status_code in ('APP','WIP')  ;


    
 declare continue handler for not found set l_rownotfound = true;

 declare exit handler for sqlexception, sqlwarning
 begin
    rollback;
    resignal;
 end;  

  call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'BEGIN: nhrdw_star_recruit_action_status_validate');  
  set l_rownotfound := false;

  open c_star_eod;  

  c_star_eod_loop: loop 

       set l_rownotfound := false;                
	     set l_proc := 'STAR EOD Cursor';    
       
	     fetch  c_star_eod into 
                                    l_rpa_number                
                                   ,l_hr_consultant             
                                   ,l_eff_date                  
                                   ,l_selectee_last_name        
                                   ,l_req_position_nbr          
                                   ,l_req_posn_descr            
                                   ,l_req_reports_to   
                                   ,l_req_deptid         
                                   ,l_transaction_status_descr  
                                   ,l_status_code
								   ,l_center
                                    ;
                                   
       if ( l_rownotfound = true )  
       then          
              close c_star_eod;
              leave c_star_eod_loop;
       end if; 
       
       
/*       
       call nhrdw_mgs_routing_key (            
                                          l_vacancy_announcement_number  
                                         ,l_vacancy_cpdf_code 
                                         ,l_qms_routing_key_field_1         
                                         ,l_qms_routing_key_field_2         
                                         ,l_qms_routing_key_field_3         
                                         ,l_qms_routing_key_field_4         
                                         ,l_qms_routing_key_field_5         
                                  );
*/                                         
       call nhrdw_qms_notifications_api
                                (
                                       p_transaction_control_id                                  
                                     ,'STAR'
                                     ,'STAR PAR Status'
                                     ,'STAR:STATUS:001'      
                                     ,l_hr_consultant
                                     ,'STAR'
                                     ,'STAR:STATUS:001'     
                                     ,l_rpa_number
                                     ,l_status_code 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null  
                                     ,l_hr_consultant 
                                     ,l_rpa_number
                                     ,date_format(l_eff_date,"%m-%d-%Y")
                                     ,l_selectee_last_name
                                     ,l_req_position_nbr 
                                     ,l_req_posn_descr
                                     ,l_req_reports_to
                                     ,l_req_deptid
                                     ,null
                                     ,null
                                     ,'STAFFING'
                                     ,'GS'
                                     ,l_center
                                     ,l_qms_routing_key_field_4 
                                     ,l_qms_routing_key_field_5 
                                  );                                                                                       
  end loop;        


  set l_rownotfound := false;


  open c_star_cancel;  

  c_star_cancel_loop: loop 

       set l_rownotfound := false;                
	     set l_proc := 'STAR CANCEL STATUS';    
       
	     fetch  c_star_cancel into 
                                    l_rpa_number                
                                   ,l_hr_consultant             
                                   ,l_eff_date                  
                                   ,l_selectee_last_name        
                                   ,l_req_position_nbr          
                                   ,l_req_posn_descr            
                                   ,l_req_reports_to    
                                   ,l_req_deptid        
                                   ,l_transaction_status_descr  
                                   ,l_status_code
								   ,l_center
                                    ;
                                   
       if ( l_rownotfound = true )  
       then          
              close c_star_cancel;
              leave c_star_cancel_loop;
       end if; 
       
       
/*       
       call nhrdw_mgs_routing_key (            
                                          l_vacancy_announcement_number  
                                         ,l_vacancy_cpdf_code 
                                         ,l_qms_routing_key_field_1         
                                         ,l_qms_routing_key_field_2         
                                         ,l_qms_routing_key_field_3         
                                         ,l_qms_routing_key_field_4         
                                         ,l_qms_routing_key_field_5         
                                  );
*/                                         
       call nhrdw_qms_notifications_api 
                                (
                                       p_transaction_control_id                                  
                                     ,'STAR'
                                     ,'STAR PAR Status'
                                     ,'STAR:STATUS:002'      
                                     ,l_hr_consultant
                                     ,'STAR'
                                     ,'STAR:STATUS:002'     
                                     ,l_rpa_number
                                     ,l_status_code 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null  
                                     ,l_hr_consultant 
                                     ,l_rpa_number
                                     ,date_format(l_eff_date,"%m-%d-%Y")
                                     ,l_selectee_last_name
                                     ,l_req_position_nbr 
                                     ,l_req_posn_descr
                                     ,l_req_reports_to
                                     ,l_req_deptid
                                     ,null
                                     ,null
                                     ,'STAFFING'
                                     ,'GS'
                                     ,l_center
                                     ,l_qms_routing_key_field_4 
                                     ,l_qms_routing_key_field_5 
                                  );                                            
       
                                       
  end loop;      
  COMMIT; 
  
  call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'END: nhrdw_star_recruit_action_status_validate');  
end$$
DELIMITER ;
