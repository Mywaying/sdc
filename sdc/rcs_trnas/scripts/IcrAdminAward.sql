use ods;
insert overwrite table ods_rcs_IcrAdminAward
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')  `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName') `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.content')   `content`,
          get_json_object(concat('{', mvtstr, '}'), '$.beginDate') `beginDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.endDate')   `endDate`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.IcrAdminAward'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`serialNo` is not null;