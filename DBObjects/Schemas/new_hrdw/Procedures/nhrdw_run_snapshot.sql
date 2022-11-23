DELIMITER $$
CREATE DEFINER=`HRDWCORPDATA`@`%` PROCEDURE `nhrdw_run_snapshot`()
begin

  declare l_hrdw_version varchar(200) default 'HRDW Patch 27.0';
  declare l_proc varchar(200);
  declare l_notfound int default false;
  declare l_hr_object_name varchar(200);
  DECLARE errsqlstate VARCHAR(5) DEFAULT '00000';
  DECLARE errmsg  TEXT;
  DECLARE errcode TEXT;  
  
  declare l_enable_award_schedule varchar(30) default 'No';
  
  
  declare c_xlat cursor for 
  SELECT xlat.FIELDVALUE from nhrdw_custom_xlatitem xlat
   WHERE xlat.FIELDNAME  = 'HRDW_ENABLE_AWARD_SCHEDULE'
     AND xlat.EFF_STATUS = 'A'
     AND xlat.EFFDT =
             (SELECT MAX(xlat_ED.EFFDT) FROM nhrdw_custom_xlatitem xlat_ED
              WHERE xlat_ed.FIELDNAME  =  'HRDW_ENABLE_AWARD_SCHEDULE'
                AND xlat_ed.EFF_STATUS = 'A'
                AND xlat_ED.EFFDT  <= curdate() );



  declare c_recon_check cursor for 
    select mast.hr_object_name  
    from  hrlinks.HRLINKS_XFER_RECON mast                                                                                                                                                                       
    where mast.db_name = 'ACAPRD' and mast.record_date = curdate();                                                                                                                                                                       
   

  declare c_recon_xfer cursor for 
    select mast.hr_object_name  
    from  hrlinks.HRLINKS_XFER_RECON mast                                                                                                                                                                       
    left  join hrlinks.HRLINKS_XFER_RECON labd on mast.hr_object_name = labd.hr_object_name and labd.db_name = 'LABD'  and labd.record_date = curdate()                             
    left  join hrlinks.HRLINKS_XFER_RECON acaprd on mast.hr_object_name = acaprd.hr_object_name and acaprd.db_name = 'ACAPRD' and acaprd.record_date = curdate()    
    where mast.db_name = 'MASTER_SET'  and mast.process_type in ('SNAPSHOT','PAYABLE_TIME')                                                                                                                                                                         
      and  coalesce(labd.num_of_rows,0) != coalesce(acaprd.num_of_rows,0); 
  
 
 declare continue handler for not found set l_notfound = true;      

  declare exit handler for sqlexception, sqlwarning
  begin
            GET CURRENT DIAGNOSTICS CONDITION 1  errsqlstate = RETURNED_SQLSTATE ,  errmsg  = MESSAGE_TEXT , errcode = MYSQL_ERRNO;

            


            rollback;
            
            call nhrdw_process_log_trk (-20001,'E',  
                                                      substr(   CONCAT(l_proc, ' SQL State: ', COALESCE(errsqlstate,' No Value '), 
                                                              ' Error Number : ' , COALESCE(errcode,' No Value '),
                                                              ' Error Text : ', COALESCE(errmsg,' No Value ') 
                                                               ) 
                                                               , 1, 2000) 
                                        );

            commit;
            
            resignal;  
  end;  
 
 set l_proc = 'nhrdw_run_snapshot';
 set l_notfound = false;
 set l_enable_award_schedule := 'No';
 
 open  c_xlat ; 
 fetch c_xlat into  l_enable_award_schedule;
 close c_xlat ; 
 
 open  c_recon_check ; 
 fetch c_recon_check into  l_hr_object_name;
 close c_recon_check ;
 
 if l_notfound 
 then
     call nhrdw_process_log_trk (-20001, 'I', 'Snapshot Refreshing ACAPRD Counts Started'); 
     call nhrdw_hrlinks_xfer_recon();  
     commit;
     call nhrdw_process_log_trk (-20001, 'I', 'Snapshot Refreshing ACAPRD Counts Completed'); 	 
 end if;
 
 
 
 set l_notfound = false;
 
 open  c_recon_xfer ; 
 fetch c_recon_xfer into  l_hr_object_name;
 close c_recon_xfer ;

 if l_notfound 
 then
      delete from HRLINKS.PS_Z_GSA_LMS_OUT ;
      commit;

      call nhrdw_process_log_trk (-20001, 'I', 'Refresh Ethnicity and Race' );

      call nhrdw_divers_ethnic_refresh();

      call nhrdw_process_log_trk (-20001, 'I', 'Refresh Employee Accomplishment' );

      call nhrdw_employee_accomplishment_refresh();
      
      call nhrdw_process_log_trk (-20001, 'I', 'Refresh SF50 Transaction' );

      call nhrdw_sf50_transaction_refresh();

      call nhrdw_process_log_trk (-20001, 'I', 'Refresh GB ABS EVENT Table' );

      call nhrdw_gp_abs_event_refresh();
      call nhrdw_gp_abs_ss_sta_refresh();
  
      call nhrdw_main_refresh_cntl (DATE_FORMAT(SYSDATE(),"%Y-%m-%d"),'HRDW','Snapshot','GSASH');
      call nhrdw_main_refresh_cntl (DATE_FORMAT(SYSDATE(),"%Y-%m-%d"),'HRDW','Snapshot','GSA03');	

      call nhrdw_process_log_trk (-20001, 'C', 'Snapshot Process Main Successfully Completed'); 

      call nhrdw_process_log_trk (-20001, 'I', 'QMS Update All Error Tables Started'); 

      call aca.updateAllErrorTables();

      call nhrdw_process_log_trk (-20001, 'I', 'QMS Update All Error Tables Completed'); 
      
      if l_enable_award_schedule = 'Yes'
      then 
          call nhrdw_run_award_cutoff();
          call nhrdw_process_log_trk (-20001, 'I', 'Award Main Process Successfully Completed'); 
      end if;

      call nhrdw_process_log_trk (-20001, 'I', 'TL Payable Time Refresh Completed' );
 
      CALL nhrdw_tl_payable_time_refresh();  
 
            
 else
      call nhrdw_process_log_trk (-20001, 'E', 'Snapshot Process Failed due to mismatch in the Records between ACAPRD and LABD'); 
 end if;
  
  
            
 commit;
 
end$$
DELIMITER ;
