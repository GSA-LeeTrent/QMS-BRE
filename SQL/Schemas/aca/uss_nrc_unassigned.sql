USE aca;
SELECT ee.created_at, ee.resolved_at, ee.qms_stfacq_error_id, ee.qms_key, ee.system_name, ee.assigned_to_org_id, oo.org_label, ee.assigned_to_user_id, uu.display_name, uu.email_address, ooo.org_label, ee.resolved_at
FROM qms_stfacq_error ee 
JOIN sec_org oo ON oo.org_id = ee.assigned_to_org_id
JOIN sec_user uu ON uu.user_id = ee.assigned_to_user_id
JOIN sec_org ooo on ooo.org_id = uu.orgId
WHERE ee.assigned_to_user_id = 186
AND ee.system_name = 'USA_STAFFING'
-- ORDER BY ee.system_name, ee.assigned_to_org_id;
-- ORDER BY ee.qms_stfacq_error_id ASC;
ORDER BY ee.created_at ASC;

-- SELECT distinct oo.org_label 
-- FROM  aca.qms_stfacq_error ee
-- JOIN sec_org oo ON oo.org_id = ee.assigned_to_org_id
-- WHERE ee.system_name = 'USA_STAFFING'; 