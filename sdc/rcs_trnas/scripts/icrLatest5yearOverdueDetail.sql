use ods;
insert overwrite table ods_rcs_icrLatest5yearOverdueDetail
  select *
  from (SELECT
          `_id`   `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.account')    `account`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')   `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.month')      `month`,
          get_json_object(concat('{', mvtstr, '}'), '$.lastMonths') `lastMonths`,
          get_json_object(concat('{', mvtstr, '}'), '$.amount')     `amount`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrLatest5yearOverdueDetail'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;