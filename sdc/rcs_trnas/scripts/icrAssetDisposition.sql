use ods;
insert overwrite table ods_rcs_icrAssetDisposition
  select *
  from (SELECT
          `_id`        `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')        `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')       `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')         `getTime`,
          get_json_object(concat('{', mvtstr, '}'), '$.money')           `money`,
          get_json_object(concat('{', mvtstr, '}'), '$.latestRepayDate') `latestRepayDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.balance')         `balance`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrAssetDisposition'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;