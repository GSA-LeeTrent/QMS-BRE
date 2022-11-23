DELIMITER $$
CREATE DEFINER=`ACA_USER`@`%` FUNCTION `getUserIdByEmailOrName`(emailOrName varchar(70)) RETURNS int(11)
BEGIN
    declare emplId int default -1; 
    declare emplIdFound int default 0;
    DECLARE CONTINUE HANDLER FOR NOT FOUND
	  BEGIN
		set emplId =  (select user_id from sec_user where lower(Email_Address) = 'no-reply@gsa.gov');
	  END;

		set emplIdFound = (select count(*) from sec_user where lower(Email_Address) = lower(emailOrName));
		if emplIdFound = 1 then
			set emplId = (select user_id from sec_user where lower(Email_Address) = lower(emailOrName));
		else
			set emplIdFound = (select count(*) from sec_user where lower(display_name) = lower(emailOrName));
			if emplIdFound = 1 then
				set emplId = (select user_id from sec_user where lower(display_name) = lower(emailOrName));
			end if;
        end if;
    return emplId;
END$$
DELIMITER ;
