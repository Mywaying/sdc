use ods;
insert overwrite table ods_rcs_icrResidence
  select T.*,
  ROW_NUMBER() OVER(PARTITION BY `reportNo` ORDER BY `getTime` desc) rn
  from (SELECT
          `_id`      `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')      `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.address')       `address`,
          get_json_object(concat('{', mvtstr, '}'), '$.residenceType') `residenceType`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')       `getTime`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrResidence'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;