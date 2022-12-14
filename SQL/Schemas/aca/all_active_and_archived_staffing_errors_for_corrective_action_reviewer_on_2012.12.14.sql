USE aca;
SELECT 
	qms_stfacq_error_id AS "Id",
	assigned_to_user_name AS "Assigned To",
	assigned_to_org_name AS "Responsible Office",
	short_error_description AS "Data Error",
	status_description AS "Status",
    assigned_at AS "Assigned On",
	system_name AS "System Name",
	created_at AS "Created"
FROM sa_StaffAcquisitionListItem
WHERE deleted_at IS NULL
ORDER BY qms_stfacq_error_id;