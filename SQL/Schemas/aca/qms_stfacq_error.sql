use aca;
-- SELECT * FROM aca.qms_stfacq_error;

-- SELECT DISTINCT SYSTEM_NAME FROM aca.qms_stfacq_error;

-- SELECT COUNT(*) FROM aca.qms_stfacq_error;

-- select system_name, count(system_name) from aca.qms_stfacq_error group by system_name;

-- select system_name, status_id, assigned_to_user_id, assigned_by_user_id, assigned_to_org_id, created_by_user_id 
-- from aca.qms_stfacq_error
-- where system_name = 'STAR';

select e.system_name, s.status_code, e.assigned_to_user_id, AssignedToUser.email_address, e.assigned_to_org_id, AssignedToOrg.org_code, AssignedToOrg.org_label 
from qms_stfacq_error e
join qms_status s on s.status_id = e.status_id
join sec_user AssignedToUser on AssignedToUser.user_id = e.assigned_to_user_id
join sec_org AssignedToOrg on AssignedToOrg.org_id = e.assigned_to_org_id
where system_name = 'STAR'
and e.assigned_to_user_id = 186
order by e.assigned_to_user_id;

select s.status_code, count(s.status_code) 
from qms_stfacq_error e
join qms_status s on s.status_id = e.status_id
where system_name = 'STAR'
GROUP BY s.status_code;



