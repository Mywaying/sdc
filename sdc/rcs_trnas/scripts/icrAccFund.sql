use ods;
insert overwrite table ods_rcs_icrAccFund
  select *
  from (SELECT
          cast (`_id` as string)     `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')     `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.area')         `area`,
          get_json_object(concat('{', mvtstr, '}'), '$.registerDate') `registerDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.firstMonth')   `firstMonth`,
          get_json_object(concat('{', mvtstr, '}'), '$.toMonth')      `toMonth`,
          get_json_object(concat('{', mvtstr, '}'), '$.state')        `state`,
          get_json_object(concat('{', mvtstr, '}'), '$.pay')          `pay`,
          get_json_object(concat('{', mvtstr, '}'), '$.ownPercent')   `ownPercent`,
          get_json_object(concat('{', mvtstr, '}'), '$.comPercent')   `comPercent`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')    `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')      `getTime`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrAccFund'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;