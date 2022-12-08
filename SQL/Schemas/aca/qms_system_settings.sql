USE aca;
SELECT ss.SettingId, sst.Setting_Code as 'QMS Role', ss.SettingValue as '# of days', sst.Setting_Description as 'Description'
FROM sys_setting ss
JOIN sys_settingtype sst on sst.SettingTypeId = ss.SettingTypeId
WHERE ss.environment = 'TEST'
AND (ss.SettingId > 19 AND ss.SettingId < 32 OR ss.SettingId IN (60, 63, 66, 69, 72))
ORDER BY ss.SettingId;