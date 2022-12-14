USE aca;
SELECT ee.system_name, ee.assigned_to_org_id, oo.org_label, ee.assigned_to_org_id, uu.display_name, uu.email_address, ooo.org_label
FROM qms_stfacq_error ee 
JOIN sec_org oo ON oo.org_id = ee.assigned_to_org_id
JOIN sec_user uu ON uu.user_id = ee.assigned_to_user_id
JOIN sec_org ooo on ooo.org_id = uu.orgId
WHERE ee.assigned_to_user_id = 186
ORDER BY ee.system_name, ee.assigned_to_org_id;