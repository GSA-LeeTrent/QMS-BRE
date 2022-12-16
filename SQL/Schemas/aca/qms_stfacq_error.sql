use aca;
select e.system_name AS System, AssignedToOrg.org_label AS "Assigned to Org", e.qms_key,s.status_code,
		AssignedToUser.display_name AS "Assignee Name", AssignedToUser.email_address AS "Assignee Email",
        e.resolved_at AS "Date Resolved"
from qms_stfacq_error e
join qms_status s on s.status_id = e.status_id
join sec_user AssignedToUser on AssignedToUser.user_id = e.assigned_to_user_id
join sec_org AssignedToOrg on AssignedToOrg.org_id = e.assigned_to_org_id
where system_name = 'STAR'
and AssignedToOrg.org_label = 'FAS Service Center'
order by AssignedToOrg.org_label, e.qms_key, AssignedToUser.email_address;

use aca;
select e.system_name AS System, AssignedToOrg.org_label AS "Assignee Org", e.qms_key,s.status_code,
		AssignedToUser.display_name AS "Assignee Name", AssignedToUser.email_address AS "Assignee Email",
        e.resolved_at AS "Date Resolved"
from qms_stfacq_error e
join qms_status s on s.status_id = e.status_id
join sec_user AssignedToUser on AssignedToUser.user_id = e.assigned_to_user_id
join sec_org AssignedToOrg on AssignedToOrg.org_id = e.assigned_to_org_id
where AssignedToOrg.org_label = 'FAS Service Center'
order by e.system_name, AssignedToOrg.org_label, e.qms_key, AssignedToUser.email_address;

-- SELECT COUNT(*) FROM aca.qms_stfacq_error;

-- SELECT COUNT(*) FROM aca.qms_stfacq_error WHERE status_id NOT IN (4,16);
-- SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications_current_v;

-- SELECT COUNT(*) FROM aca.qms_stfacq_error
-- WHERE status_id NOT IN (4,16)
-- AND qms_key NOT IN (SELECT qms_key FROM new_hrdw.nhrdw_qms_notifications_current_v);

-- SELECT qms_key, count(qms_key) FROM new_hrdw.nhrdw_qms_notifications_current_v GROUP BY qms_key HAVING count(qms_key) > 1;
-- SELECT qms_key, count(qms_key) FROM aca.qms_stfacq_error GROUP BY qms_key HAVING count(qms_key) > 1;

