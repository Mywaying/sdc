use ods;
insert overwrite table ods_rcs_icrAssurerRepay
  select *
  from (SELECT
          `_id`               `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')               `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')              `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.latestAssurerRepayDate') `latestAssurerRepayDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.money')                  `money`,
          get_json_object(concat('{', mvtstr, '}'), '$.latestRepayDate')        `latestRepayDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.balance')                `balance`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrAssurerRepay'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;