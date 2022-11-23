DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` PROCEDURE `Update_qms_data_error_stats`()
BEGIN
insert into aca.qms_error_stat (poid, error_code, snapshot_date, error_count)
select gvt_poi poid
      ,qms_error_code error_code
      ,snapshot_date
      ,count(*) error_count
from new_hrdw.nhrdw_qms_empl_error_tracker_current_v
group by gvt_poi, qms_error_code, snapshot_date;
END$$
DELIMITER ;
