DELIMITER $$
CREATE DEFINER=`HRDWCORPDATA`@`%` PROCEDURE `nhrdw_uss_mstr_requests_validate`(
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



  declare l_tenantid                            bigint default null;    
  declare l_requestid                           bigint default null;    
  declare l_approvername                        varchar(255) default null;    
  declare l_approveremail                       varchar(255) default null;    
  declare l_requestnumber                       varchar(255) default null;    
  declare l_creationdatetime                    datetime     default null;    



  
  #cursors
  
  declare c_uss_mstr_requests_no_format cursor for
  select  tenantid
         ,requestid
         ,approvername
         ,approveremail
         ,requestnumber
         ,creationdatetime 
  from  hiring.uss_mstr_requests
  where creationdatetime >= '2021-10-01'
  and   requestnumber not regexp  '^[0-9]+[CU]$';
  
 declare continue handler for not found set l_rownotfound = true;

 declare exit handler for sqlexception, sqlwarning
 begin
    rollback;
    resignal;
 end;  

  set l_rownotfound := false;

  open c_uss_mstr_requests_no_format;  

  c_uss_mstr_requests_no_format_loop: loop 

       set l_rownotfound := false;                
	     set l_proc := 'USA Staffing Request Number Format';    
       
	     fetch  c_uss_mstr_requests_no_format into 
                                    l_tenantid          
                                   ,l_requestid         
                                   ,l_approvername      
                                   ,l_approveremail     
                                   ,l_requestnumber     
                                   ,l_creationdatetime  
                                   ;


       if ( l_rownotfound = true )  
       then          
              close c_uss_mstr_requests_no_format;
              leave c_uss_mstr_requests_no_format_loop;
       end if; 
       
       
       
       call nhrdw_qms_notifications_api 
                                (
                                       p_transaction_control_id                                  
                                     ,'USA_STAFFING'
                                     ,'Request PAR Number'
                                     ,'USS:REQNUM:001'      
                                     ,l_approveremail
                                     ,'USA_STAFFING'
                                     ,'USS:REQNUM:001'  
                                     ,l_tenantid
                                     ,l_requestid 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null 
                                     ,null  
                                     ,l_requestnumber 
                                     ,l_approveremail
                                     ,null
                                     ,null
                                     ,null 
                                     ,null
                                     ,null
                                     ,null
                                     ,null
                                     ,null
                                     ,'STAFFING'
                                     ,'GS'
                                     ,'NRC'
                                     ,null
                                     ,null
                                  );                                            
       
 
     
  end loop;        

       
end$$
DELIMITER ;
