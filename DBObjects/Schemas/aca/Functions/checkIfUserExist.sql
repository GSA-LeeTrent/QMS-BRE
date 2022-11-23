DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` FUNCTION `checkIfUserExist`(emailOrName varchar(100)) RETURNS int(1)
BEGIN
    declare emplIdFound int; 
    DECLARE CONTINUE HANDLER FOR NOT FOUND
	  BEGIN
		set emplIdFound = -1;
	  END;
		set emplIdFound = (select count(*) from sec_user where lower(Email_Address) = lower(emailOrName));
        IF emplIdFound = -1 THEN
			set emplIdFound = (select count(*) from sec_user where lower(display_name) = lower(emailOrName));
        END IF;
    return emplIdFound;
END$$
DELIMITER ;
