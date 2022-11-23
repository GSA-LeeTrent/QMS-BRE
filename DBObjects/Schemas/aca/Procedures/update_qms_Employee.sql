DELIMITER $$
CREATE DEFINER=`aortega`@`%` PROCEDURE `update_qms_Employee`()
BEGIN
    declare finished integer default 0;
    declare records_inserted integer default 0;
    declare records_updated integer default 0;
    declare records_deleted integer default 0;
	declare rec_count_match integer default 0;
    
    declare hrdw_emplid varchar(30);
    declare hrdw_userkey varchar(128);
    declare hrdw_firstname varchar(30);
    declare hrdw_middlename varchar(30);  
    declare hrdw_lastname varchar(30);  
    declare hrdw_emailaddress varchar(70);  
    declare hrdw_agencysubelement varchar(5);  
    declare hrdw_poi varchar(4);  
    declare hrdw_deptid varchar(10);  
    declare hrdw_payplan varchar(2);
    declare hrdw_grade varchar(2);
    declare hrdw_managerid varchar(10);
    declare hrdw_Position_Nbr varchar(8);


DECLARE cur_hrdw_employees CURSOR FOR 
select emplid
  ,userkey
  ,firstname
  ,middlename
  ,lastname
  ,emailaddress
  ,agencysubelement
  ,personnelofficeidentifier
  ,departmentid
  ,payplan
  ,grade
  ,managerid
  ,position_nbr
from aca.hrlinks_employee;
                        
DECLARE CONTINUE HANDLER 
FOR NOT FOUND SET finished = 1;   
   
open cur_hrdw_employees;
        
        getEmployees : LOOP
FETCH cur_hrdw_employees INTO hrdw_emplid, hrdw_userkey, hrdw_firstname, hrdw_middlename,hrdw_lastname,hrdw_emailaddress,hrdw_agencysubelement,hrdw_poi,hrdw_deptid,hrdw_payplan,hrdw_grade,hrdw_managerid,hrdw_Position_Nbr;
            if finished = 1 then 
leave getEmployees;
            end if;
set rec_count_match = (select count(*) from aca.qms_employee where EmplId = hrdw_emplid);
IF rec_count_match = 1 THEN -- update existing record
set records_updated = records_updated + 1;
UPDATE aca.qms_employee SET
FirstName = hrdw_firstname,
MiddleName = hrdw_middlename,
LastName = hrdw_lastname,
EmailAddress = hrdw_emailaddress,
AgencySubElement = hrdw_agencysubelement,
PersonnelOfficeIdentifier = hrdw_poi,
DepartmentId = hrdw_deptid,
PayPlan = hrdw_payplan,
Grade = hrdw_grade,
ManagerId = hrdw_managerid,
UpdatedAt = now(),
                            DeletedAt = null,
UserKey = hrdw_userkey,
                            Position_nbr = hrdw_Position_Nbr
WHERE EmplId = hrdw_emplid;
ELSE -- insert new record
set records_inserted = records_inserted + 1;

INSERT INTO aca.qms_employee
(EmplId,FirstName,MiddleName,LastName,EmailAddress,AgencySubElement,PersonnelOfficeIdentifier,DepartmentId,PayPlan,Grade,ManagerId,UserKey,UpdatedAt,Position_nbr)
VALUES
(hrdw_emplid, hrdw_firstname, hrdw_middlename,hrdw_lastname,hrdw_emailaddress,hrdw_agencysubelement,hrdw_poi,hrdw_deptid,hrdw_payplan,hrdw_grade,hrdw_managerid,hrdw_userkey,now(),hrdw_Position_Nbr);

            END IF;
END LOOP getEmployees;
    CLOSE cur_hrdw_employees;

set records_deleted = (select count(*)
   from aca.qms_employee
    where deletedat is null 
     and emplid not in (select emplid 
                        from aca.hrlinks_employee)
                       );
    
    SET SQL_SAFE_UPDATES = 0;
    update aca.qms_employee set
       deletedat = now()
    where deletedat is null
      and emplid not in (select emplid from aca.hrlinks_employee);

insert into aca.ntf_notification (user_id,  notificationevent_id, workitem_id, workitemtype_code,title,message,hasbeenread,sendasemail,createdat)
                              values (1,10,8,'System','Employee Records Updated',concat(records_inserted, ' Records Inserted<br/>', records_updated, ' Records Updated<br/>', records_deleted, ' Records Deleted'),0,1,now());
END$$
DELIMITER ;
