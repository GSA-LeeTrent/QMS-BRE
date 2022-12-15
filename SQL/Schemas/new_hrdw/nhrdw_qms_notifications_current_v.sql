-- SELECT * FROM new_hrdw.nhrdw_qms_notifications_current_v;

SELECT system_name, qms_key, notification_email_address, qms_routing_key_field_3
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE system_name = 'USA_STAFFING'
ORDER BY notification_email_address;

SELECT DISTINCT notification_email_address
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE system_name = 'USA_STAFFING'
ORDER BY notification_email_address;

SELECT DISTINCT notification_email_address
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE system_name = 'USA_STAFFING'
AND notification_email_address NOT IN (SELECT email_address FROM aca.sec_user);



SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications_current_v WHERE system_name = 'USA_STAFFING';
SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications_current_v WHERE system_name = 'USA_STAFFING' AND notification_email_address IS NULL;
SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications_current_v WHERE system_name = 'USA_STAFFING' AND notification_email_address IS NOT NULL;

SELECT system_name, qms_key, notification_email_address, qms_routing_key_field_3
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE qms_key IN ('USA_STAFFING:USS:REQNUM:001:5:136231', 'USA_STAFFING:USS:REQNUM:001:5:137059');

