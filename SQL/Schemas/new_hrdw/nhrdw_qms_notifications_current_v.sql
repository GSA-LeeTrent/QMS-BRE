-- SELECT * FROM new_hrdw.nhrdw_qms_notifications_current_v;

SELECT system_name, qms_key, notification_email_address, qms_routing_key_field_3
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE system_name = 'USA_STAFFING'
ORDER BY notification_email_address;

SELECT DISTINCT notification_email_address
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE system_name = 'USA_STAFFING'
ORDER BY notification_email_address;

SELECT  notification_email_address, system_name, qms_routing_key_field_3, length(qms_routing_key_field_3)
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE system_name = 'USA_STAFFING'
AND notification_email_address NOT IN (SELECT email_address FROM aca.sec_user);



SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications_current_v WHERE system_name = 'USA_STAFFING';
SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications_current_v WHERE system_name = 'USA_STAFFING' AND notification_email_address IS NULL;
SELECT COUNT(*) FROM new_hrdw.nhrdw_qms_notifications_current_v WHERE system_name = 'USA_STAFFING' AND notification_email_address IS NOT NULL;

SELECT system_name, qms_key, notification_email_address, qms_routing_key_field_3
FROM new_hrdw.nhrdw_qms_notifications_current_v
WHERE qms_key IN ('USA_STAFFING:USS:REQNUM:001:5:136231', 'USA_STAFFING:USS:REQNUM:001:5:137059');


CREATE ALGORITHM=UNDEFINED DEFINER=`HRDWCORPDATA`@`%` SQL SECURITY DEFINER VIEW `new_hrdw`.`nhrdw_qms_notifications_current_v` AS select `rec`.`QMS_KEY` AS `qms_key`,`rec`.`QMS_KEY_TEXT` AS `qms_key_text`,`rec`.`QMS_SHORT_DESCRIPTION` AS `qms_short_description`,`dt`.`SYSTEM_NAME` AS `system_name`,`dt`.`DATA_ITEM_NAME` AS `data_item_name`,`dt`.`DATA_ITEM_CATEGORY` AS `data_item_category`,`qmel`.`DATA_ITEM_ID` AS `data_item_id`,`qmel`.`ERROR_LIST_ID` AS `error_list_id`,`qmel`.`QMS_ERROR_CODE` AS `qms_error_code`,`qmel`.`ENABLE_QMS` AS `enable_qms`,`rec`.`QMS_KEY_FIELD_1` AS `qms_key_field_1`,`rec`.`QMS_KEY_FIELD_2` AS `qms_key_field_2`,`rec`.`QMS_KEY_FIELD_3` AS `qms_key_field_3`,`rec`.`QMS_KEY_FIELD_4` AS `qms_key_field_4`,`rec`.`QMS_KEY_FIELD_5` AS `qms_key_field_5`,`rec`.`QMS_KEY_FIELD_6` AS `qms_key_field_6`,`rec`.`QMS_KEY_FIELD_7` AS `qms_key_field_7`,`rec`.`QMS_KEY_FIELD_8` AS `qms_key_field_8`,`rec`.`QMS_KEY_FIELD_9` AS `qms_key_field_9`,`rec`.`QMS_KEY_FIELD_10` AS `qms_key_field_10`,`rec`.`FIELD_1` AS `field_1`,`rec`.`FIELD_2` AS `field_2`,`rec`.`FIELD_3` AS `field_3`,`rec`.`FIELD_4` AS `field_4`,`rec`.`FIELD_5` AS `field_5`,`rec`.`FIELD_6` AS `field_6`,`rec`.`FIELD_7` AS `field_7`,`rec`.`FIELD_8` AS `field_8`,`rec`.`FIELD_9` AS `field_9`,`rec`.`FIELD_10` AS `field_10`,`rec`.`NOTIFICATION_EMAIL_ADDRESS` AS `notification_email_address`,`rec`.`QMS_NOTIFICATION_TEXT` AS `qms_notification_text`,`rec`.`TRANSACTION_CONTROL_ID` AS `transaction_control_id`,`rec`.`QMS_ROUTING_KEY` AS `qms_routing_key`,`rec`.`QMS_ROUTING_KEY_FIELD_1` AS `qms_routing_key_field_1`,`rec`.`QMS_ROUTING_KEY_FIELD_2` AS `qms_routing_key_field_2`,`rec`.`QMS_ROUTING_KEY_FIELD_3` AS `qms_routing_key_field_3`,`rec`.`QMS_ROUTING_KEY_FIELD_4` AS `qms_routing_key_field_4`,`rec`.`QMS_ROUTING_KEY_FIELD_5` AS `qms_routing_key_field_5` 

from ((((`new_hrdw`.`nhrdw_qms_notifications` `rec` 
join `new_hrdw`.`nhrdw_latest_txn_controls` `ltc` on(((`ltc`.`TYPE_OF_RECORD` = 'Snapshot') and (`rec`.`TRANSACTION_CONTROL_ID` = `ltc`.`TRANSACTION_CONTROL_ID`)))) 
join `new_hrdw`.`nhrdw_snapshot_ctrl` `snap` on((`snap`.`TRANSACTION_CONTROL_ID` = `ltc`.`TRANSACTION_CONTROL_ID`))) 
join `new_hrdw`.`nhrdw_qms_master_error_list` `qmel` on((`qmel`.`ERROR_LIST_ID` = `rec`.`ERROR_LIST_ID`))) 
join `new_hrdw`.`nhrdw_data_item` `dt` on((`dt`.`DATA_ITEM_ID` = `qmel`.`DATA_ITEM_ID`)))

