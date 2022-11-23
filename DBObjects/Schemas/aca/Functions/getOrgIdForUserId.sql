DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` FUNCTION `getOrgIdForUserId`(pUserId int) RETURNS int(11)
BEGIN
    declare orgId int; 
    DECLARE CONTINUE HANDLER FOR NOT FOUND
	  BEGIN
		set orgId = -1;
	  END;      
    set orgId = (select OrgId from aca.sec_user where user_id = pUserId);
    return orgId;    
END$$
DELIMITER ;
