use ods;
insert overwrite table ods_rcs_IcrDissentInfo
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.account')  `account`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo') `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.content')  `content`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')  `getTime`,
          get_json_object(concat('{', mvtstr, '}'), '$.type')     `type`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.IcrDissentInfo'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`account` is not null;