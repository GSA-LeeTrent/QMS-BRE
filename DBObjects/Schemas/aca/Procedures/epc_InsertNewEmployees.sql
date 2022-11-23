DELIMITER $$
CREATE DEFINER=`aortega`@`%` PROCEDURE `epc_InsertNewEmployees`()
BEGIN
	insert into aca.epc_employeepositioncategory (emplid, fromdate, position_nbr,positioncategoryid,comments, businessreason,updatedbyuserid) 
		select emplid, curdate(), position_nbr, 4,'','', 1
		from aca.qms_employee 
		where deletedat is null
		   and emplid <> '00000000'
		  and emplid not in (select distinct emplid from aca.epc_employeepositioncategory);
END$$
DELIMITER ;
