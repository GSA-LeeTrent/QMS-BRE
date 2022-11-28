-- SELECT * FROM new_hrdw.nhrdw_qms_notifications;

-- SELECT MAX(CREATION_DATE) FROM new_hrdw.nhrdw_qms_notifications;

SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications WHERE CREATION_DATE > '2022-11-27';