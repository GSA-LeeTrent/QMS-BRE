DELIMITER $$
CREATE DEFINER=`aortega`@`%` PROCEDURE `epc_UpdateEmployeesDueToPositionChange`()
BEGIN
    declare finished integer default 0;
    declare records_inserted integer default 0;
	declare emp_emplid varchar(30);
    declare emp_posnbr varchar(30);
    declare emp_epcid int(10);
    
	declare cur_epc_empls cursor for
		select epc.EmployedPositionCategoryId
              ,epc.emplid
              ,emp.position_nbr
		from aca.epc_EmployeePositionCategory epc join aca.qms_employee emp on epc.EmplId = emp.emplid
		where (epc.ToDate is null and emp.DeletedAt is null)
		  and epc.Position_Nbr <> emp.Position_Nbr;
          
	DECLARE CONTINUE HANDLER 
	FOR NOT FOUND SET finished = 1;   

	open cur_epc_empls;
    getEmployees : LOOP	
	fetch cur_epc_empls into emp_epcid,emp_emplid, emp_posnbr;
		if finished = 1 then 
		   leave getEmployees;
		end if;
		
        update aca.epc_employeepositioncategory set todate= now() where EmployedPositionCategoryId = emp_epcid; 
		insert into aca.epc_employeepositioncategory (emplid, fromdate, Position_Nbr, positioncategoryid, updatedbyuserid) values
                                                     (emp_emplid, now(), emp_posnbr,5, 1);
	
	END LOOP getEmployees;
    CLOSE cur_epc_empls;	

END$$
DELIMITER ;
