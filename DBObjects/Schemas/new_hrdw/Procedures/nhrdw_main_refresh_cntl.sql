DELIMITER $$
CREATE DEFINER=`HRDWCORPDATA`@`%` PROCEDURE `nhrdw_main_refresh_cntl`(
  in p_record_date date,
  in p_controlling_authority varchar(30),
  in p_type_of_record varchar(100),
  in p_setid VARCHAR(5)
)
begin
  #variables
  declare l_hrdw_version varchar(200) default 'HRDW Patch 26.0';
  declare l_done int default false;
  declare l_master_set_control_id bigint;
  declare l_transaction_control_id bigint;
  declare l_purge_date date;
  declare l_record_date date;
  declare l_current_date date;
  declare lr_transaction_control_id bigint;
  declare l_pay_period_date            date;
  declare l_last_day_month             date;
  declare l_proc varchar(200);
  declare l_type_of_record             varchar(200);       
  declare l_description                varchar(255);       

  declare l_fiscal_year_start_date     date;
  declare l_fiscal_year_end_date       date;
  declare l_calendar_year_start_date   date;
  declare l_calendar_year_end_date     date;
  declare l_pay_period_start_date      date;

  DECLARE errsqlstate VARCHAR(5) DEFAULT '00000';
  DECLARE errmsg  TEXT;
  DECLARE errcode TEXT;  
  
  #cursors

  
  declare c_master_set_ctrl cursor for 
  select 
    master_set_control_id
  from 
    nhrdw_master_set_ctrl
  where
            snapshot_date               = l_record_date
	and controlling_authority	= p_controlling_authority
	and type_of_record              = l_type_of_record;

  declare c_snapshot_ctrl cursor for 
  select 
      transaction_control_id
  from 
    nhrdw_snapshot_ctrl
  where
            snapshot_date               = l_record_date
	and controlling_authority	= p_controlling_authority
	and type_of_record              = l_type_of_record
	and setid                       = p_setid;
		
  declare lc_get_pay_period_date cursor for 
  select  start_date,end_date
     from NHRDW_TIME_PERIODS
    where period_type = 'Bi-Week'
      and l_record_date between start_date and end_date;
        
  #handlers
  declare continue handler for not found set l_done = true;

  declare exit handler for sqlexception, sqlwarning
  begin
            GET CURRENT DIAGNOSTICS CONDITION 1  errsqlstate = RETURNED_SQLSTATE ,  errmsg  = MESSAGE_TEXT , errcode = MYSQL_ERRNO;

            


            rollback;
            
            call nhrdw_process_log_trk (COALESCE(l_transaction_control_id,'-1'),'E',  
                                                      substr(   CONCAT(l_proc, ' SQL State: ', COALESCE(errsqlstate,' No Value '), 
                                                              ' Error Number : ' , COALESCE(errcode,' No Value '),
                                                              ' Error Text : ', COALESCE(errmsg,' No Value ') 
                                                               ) 
                                                               , 1, 2000) 
                                        );

            commit;
            
            resignal;  
  end;  


  set l_proc = 'nhrdw_main_refresh_cntl';
  
  set l_record_date  = DATE_FORMAT(p_record_date,"%Y-%m-%d") ;
  set l_current_date = DATE_FORMAT(SYSDATE(),"%Y-%m-%d") ;

  set l_type_of_record = p_type_of_record;
  
  if p_type_of_record in ('Manual','Refresh','GSA Award Cut-Off')
  then
        set l_record_date = DATE_FORMAT(p_record_date,"%Y-%m-%d") ;
  elseif p_type_of_record in ('Payroll Cut-Off')
  then        
        set l_record_date = DATE_ADD(l_record_date, INTERVAL '-14' DAY);
  else
        set l_record_date = DATE_ADD(l_record_date, INTERVAL '-1' DAY);
  end if;
  
  set l_last_day_month = LAST_DAY(l_record_date );   

 
  open  lc_get_pay_period_date;    
  fetch lc_get_pay_period_date into l_pay_period_start_date,l_pay_period_date;
  close lc_get_pay_period_date;

  if p_type_of_record in ('Payroll Cut-Off')
  then        
        set l_record_date = l_pay_period_date;
  end if;
   
   
  set l_description = p_type_of_record;
       

  if   p_type_of_record in ('GSA Award Cut-Off')
  then
      -- set l_purge_date = DATE_ADD(l_current_date, INTERVAL '60' DAY ) ;
       set l_purge_date = null;
       set l_description =  concat(p_type_of_record,' Refreshed On ', DATE_FORMAT(SYSDATE(),"%m-%d-%Y"));
  elseif  ( l_record_date =  l_pay_period_date or    l_record_date = l_last_day_month )
  then
       set l_purge_date = null;
  else
       set l_purge_date = DATE_ADD(l_current_date, INTERVAL '14' DAY ) ;
  end if;
  
  set l_done = false;

  
  call nhrdw_process_log_trk (-1, 'I', concat('Process Step 10 Started',p_setid,p_record_date));
  
  commit;
  
  open c_snapshot_ctrl ;    
  fetch c_snapshot_ctrl  into lr_transaction_control_id;
  close c_snapshot_ctrl ;

  call nhrdw_process_log_trk (-1, 'I', concat('Process Step 20 : Snapshot Cursor',p_setid,p_record_date));
 
  commit;


  set l_fiscal_year_start_date    = DATE_ADD(DATE_ADD(DATE_FORMAT(DATE_ADD(l_record_date,INTERVAL 3 MONTH) ,'%Y-09-30'),INTERVAL 1 DAY), INTERVAL -12 MONTH) ;
  set l_fiscal_year_end_date      = DATE_FORMAT(DATE_ADD(l_record_date,INTERVAL 3 MONTH) ,'%Y-09-30');     
  set l_calendar_year_start_date  = DATE_FORMAT(l_record_date ,'%Y-01-01');
  set l_calendar_year_end_date    = DATE_FORMAT(l_record_date ,'%Y-12-31');

  if   p_type_of_record in ('GSA Award Cut-Off')
  then
      set l_done = true;
  end if;
  
  if  l_done  
  then


          call nhrdw_process_log_trk (-1, 'I', concat('Process Step 30 : Master Control Cursor',p_setid,p_record_date));
          
          commit;
          
          set l_done := false;
          
          open c_master_set_ctrl;    
          fetch c_master_set_ctrl into l_master_set_control_id;
          close c_master_set_ctrl;

          call nhrdw_process_log_trk (-1, 'I', concat('Process Step 40 : Master Control Cursor',p_setid,p_record_date));
          
          commit;

          if   p_type_of_record in ('GSA Award Cut-Off')
          then
                 set l_done = true;
          end if;
  
          if l_done then
          
                  insert  into nhrdw_master_set_ctrl
        	  set     snapshot_date 			= l_record_date			
                	 ,controlling_authority 	        = p_controlling_authority		
        	         ,type_of_record			= l_type_of_record;
        	         
                  select last_insert_id() into l_master_set_control_id;
        	  
        	  call nhrdw_geolocation_refresh(l_master_set_control_id, p_record_date);
        	  
        	  commit;
        	  
        	  call nhrdw_occ_series_refresh(l_master_set_control_id, p_record_date);	  

                  commit;
                  
                  call nhrdw_process_log_trk (-1, 'I', concat('Process Step 50 : Master Control Completed',p_setid,p_record_date));

                  commit;
          end if;
        
             
        	
           insert into nhrdw_snapshot_ctrl
        	set  master_set_control_id	= l_master_set_control_id
        	     ,snapshot_date 		= l_record_date			
        	     ,setid			= p_setid
        	     ,pay_period_end_date       = l_pay_period_date
        	     ,pay_period_start_date     = l_pay_period_start_date
        	     ,purge_date		            = l_purge_date
               ,controlling_authority     = p_controlling_authority		
        	     ,description      	        = l_description	   
               ,fiscal_year_start_date    = l_fiscal_year_start_date  
               ,fiscal_year_end_date      = l_fiscal_year_end_date    
               ,calendar_year_start_date  = l_calendar_year_start_date
               ,calendar_year_end_date    = l_calendar_year_end_date        	       
        	     ,type_of_record		= l_type_of_record;
        	     	     
           select last_insert_id() into l_transaction_control_id;

           call nhrdw_process_log_trk (-1, 'I', concat('Process Step 60 : Snapshot Control Completed',p_setid,p_record_date));
           
           commit;
           
            /* call proc's to populate various snapshot tables */
        
        	# following is slow
        	
          
                set l_proc = 'nhrdw_location_refresh';        	 
        	call nhrdw_location_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date, p_setid);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Location Refresh Completed');
           
                commit;
                set l_proc = 'nhrdw_department_refresh';        	
        	call nhrdw_department_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date,p_setid);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Organization Refresh Completed');

                set l_proc = 'nhrdw_location_refresh';        	
        	call nhrdw_update_dept_hierarchy(l_transaction_control_id, l_master_set_control_id, l_record_date,p_setid);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Organization Hierarchy Refresh Completed');
                commit;
        	
                set l_proc = 'nhrdw_person_refresh';        	        
        	call nhrdw_person_refresh(l_transaction_control_id, l_record_date, p_setid);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Person Refresh Completed');
                commit;

                set l_proc = 'nhrdw_jobcode_refresh';        	
        	call nhrdw_jobcode_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date, p_setid);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Job Code Refresh Completed');
                commit;

                set l_proc = 'nhrdw_position_refresh';        	
        	call nhrdw_position_refresh(l_transaction_control_id, l_master_set_control_id,l_record_date, p_setid);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Position Refresh Completed');
                commit;

                set l_proc = 'nhrdw_job_refresh';        	        	
        	call nhrdw_job_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date, p_setid);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Job Refresh Completed');
                commit;

                set l_proc = 'nhrdw_employment_refresh';        	
        	call nhrdw_employment_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Employment Refresh Completed');
                commit;

                set l_proc = 'nhrdw_pay_refresh';        	        	
        	call nhrdw_pay_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Pay Refresh Completed');
                commit;

                set l_proc = 'nhrdw_incentive_refresh';        	
        	call nhrdw_incentive_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date);
        	call nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Incentive Refresh Completed');
                commit;

                set l_proc = 'nhrdw_benefit_refresh';        	
        	call nhrdw_benefit_refresh(l_transaction_control_id, l_master_set_control_id, l_record_date);        
        	call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Benefit Refresh Completed');
                commit;

                set l_proc = 'nhrdw_address_refresh';        	
        	call nhrdw_address_refresh(l_transaction_control_id, l_record_date,p_setid);      
        	call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Address Refresh Completed');
                commit;
        	
                set l_proc = 'nhrdw_appraisal_refresh';        	
             	call nhrdw_appraisal_refresh(l_transaction_control_id, l_record_date,p_setid);      
        	    call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Appraisal Refresh Completed');
                commit;

               set l_proc = 'Employment History ';        	
               call nhrdw_employment_history_refresh(l_transaction_control_id,l_record_date,p_setid);           
               call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Employment History Completed');             

               set l_proc = 'nhrdw_education_refresh';        	
               call nhrdw_education_refresh(l_transaction_control_id,l_record_date);           
               call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Education Refresh Completed');             



               if ( p_type_of_record = 'Snapshot' and p_setid IN ('GSASH','GSA03') and l_record_date = DATE_ADD(curdate(), INTERVAL '-1' DAY) ) 
               then  
                   set l_proc = 'GSA LMS OUT Refresh';        	
        	       call  nhrdw_gsa_lms_out_refresh(l_transaction_control_id);        
        	       call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'GSA LMS OUT Refresh Completed');             
               end if;


               if ( p_type_of_record = 'Snapshot' and  l_record_date = DATE_ADD(curdate(), INTERVAL '-1' DAY) )  -- p_setid IN ('GSASH') Removed to execute the QMS Business Rules
               then  
                   set l_proc = 'Refresh QMS Error Checking';        	
                   call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Refresh QMS Error Checking Started');             
                   call nhrdw_qms_empl_error_tracker_refresh(l_transaction_control_id);			       
			 
               end if;

               if ( p_type_of_record = 'Snapshot' and  l_record_date = DATE_ADD(curdate(), INTERVAL '-1' DAY) )  and p_setid IN ('GSASH')  
               then  
                   set l_proc = 'Refresh QMS Staffing Error Checking';        	
                   call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Refresh QMS Staffing Error Checking Started');             
                   call nhrdw_staffing_execute_business_rules(l_transaction_control_id);		 
               end if;
                              
               if ( p_type_of_record = 'Snapshot' and p_setid IN ('GSASH') and l_record_date = DATE_ADD(curdate(), INTERVAL '-1' DAY) ) 
               then  

                   set l_proc = 'Refresh Employee Work Schedule';        	
                   call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Refresh Employee Work Schedule Started');             
                   call  nhrdw_employee_work_schedule_refresh(l_transaction_control_id,l_record_date,p_setid);
			       
			 
               end if;


            
             if p_type_of_record in ('GSA Award Cut-Off')
             then  
                 set l_proc = 'Awared Proration Refresh';        	
        	       call  nhrdw_fy_awd_ep_appr_refresh(   l_transaction_control_id
        	                                           , l_master_set_control_id
        	                                           , l_record_date
        	                                           , l_fiscal_year_start_date  
                                                           , l_fiscal_year_end_date    
                                                           , l_calendar_year_start_date
                                                           , l_calendar_year_end_date  
                                                           , p_setid );        
        	         call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Awared Proration Refresh Completed');             
                   update nhrdw_snapshot_ctrl                                                                              
                     set  purge_date = curdate()                                                                           
                    where transaction_control_id < l_transaction_control_id
					          and   snapshot_date  = l_record_date
                    and   type_of_record =  p_type_of_record
                    and   purge_date is null
                    and   setid = p_setid;

             end if;

             call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Refresh QMS Error Checking Completed');             

             if p_type_of_record in ('Payroll Cut-Off','Snapshot','GSA Award Cut-Off')
             then  
                set l_proc = 'Refresh Latest Transaction Control ID';        	
        	      call  nhrdw_latest_txn_id_refresh(l_transaction_control_id, l_record_date,p_setid,p_type_of_record);        
        	      call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Latest Transaction Control ID Refresh Completed');             
             end if;
                          
             update nhrdw_snapshot_ctrl
                set  completion_status  = 'S'
                    ,completion_date    = sysdate()
                    ,completion_message = 'Successfully Completed.' 
              where transaction_control_id = l_transaction_control_id;


             if ( p_type_of_record = 'Snapshot' and  l_record_date = DATE_ADD(curdate(), INTERVAL '-1' DAY) )  and p_setid IN ('GSASH')  
             then  
                   set l_proc = 'Refresh OHRMBI BPR Roster Refresh';        	
                   call  nhrdw_process_log_trk (l_transaction_control_id, 'I', 'Refresh OHRMBI BPR Roster Refresh');             
                   call nhrdw_ohrmbi_bpr_employee_roster_refresh(l_transaction_control_id);		 
             end if;
                        
             
              commit;
   
           	  call nhrdw_process_log_trk (l_transaction_control_id, 'S', concat( 'Snapshot Process Successfully Completed for Business Set :', p_setid) );
              commit;

  else

           	call nhrdw_process_log_trk (-1, 'E', concat( 'Snapshot Record Already Exists ... ' ,
           	                                          ' Record Date : ', p_record_date,         	                                          
           	                                          ' Controlling Authority : ', p_controlling_authority,
           	                                          ' Type of Record : ', l_type_of_record,
           	                                          ' Set ID : ', p_setid
           	                                                ) );
           	commit;
                                                       
  end if;
  
  commit;
 
end$$
DELIMITER ;
