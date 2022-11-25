DELIMITER $$
CREATE DEFINER=`HRDWCORPDATA`@`%` PROCEDURE `nhrdw_mgs_routing_key`(
    in p_vacancy_announcement_number    varchar(60) 
   ,in p_vacancy_cpdf_code              varchar(60)  
   ,inout p_qms_routing_key_field_1        varchar(255)   
   ,inout p_qms_routing_key_field_2        varchar(255)   
   ,inout p_qms_routing_key_field_3        varchar(255)   
   ,inout p_qms_routing_key_field_4        varchar(255)   
   ,inout p_qms_routing_key_field_5        varchar(255)   
)
begin


 declare exit handler for sqlexception, sqlwarning
 begin
    rollback;
    resignal;
 end;  

 set p_qms_routing_key_field_1 := 'STAFFING';
 set p_qms_routing_key_field_2 := 'GS';
 
 if substr(p_vacancy_announcement_number,3,3) = 'NRC'
 then
   set p_qms_routing_key_field_3 := substr(p_vacancy_announcement_number,3,3);
 elseif p_vacancy_cpdf_code in ('GS03','GS30')
 then
   set p_qms_routing_key_field_3 := p_vacancy_cpdf_code;
 else
   set p_qms_routing_key_field_3 := 'STF';
 end if;
 
end$$
DELIMITER ;
