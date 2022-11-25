DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` FUNCTION `getOrgByRoutingKey`(routingKey varchar(4)) RETURNS int(11)
BEGIN
    declare orgId int; 
    declare orgCode varchar(8);
    DECLARE CONTINUE HANDLER FOR NOT FOUND
	  BEGIN
		set orgId = -1;
	  END;    

    if routingKey = 'GS03' then 
		set orgCode = 'PBSSC';
	elseif routingKey = 'GS30' then
		set orgCode = 'FASSC';
	elseif routingKey = 'STF' then
		set orgCode = 'SSOSC';
    elseif routingKey = 'NRC' then
		set orgCode = 'NRC';
    elseif routingKey = 'PBS' then
		set orgCode = 'PBSSC';
    elseif routingKey = 'FAS' then
		set orgCode = 'FASSC';
	else
		set orgCode = 'GSA_OHRM';
    end if;
    
    set orgId = (select org_id from sec_org where org_code = orgCode);
    return orgId;
    
END$$
DELIMITER ;
