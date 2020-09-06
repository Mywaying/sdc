use ods;
insert overwrite table ods_rcs_icrSpouse
  select *,
  ROW_NUMBER() OVER(PARTITION BY `reportNo` ORDER BY `reportNo` desc) rn
  from (SELECT
          cast (`_id` as string)     `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.name')        `name`,
          get_json_object(concat('{', mvtstr, '}'), '$.certType')    `certType`,
          get_json_object(concat('{', mvtstr, '}'), '$.certNo')      `certNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.employer')    `employer`,
          get_json_object(concat('{', mvtstr, '}'), '$.telephoneNo') `telephoneNo`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrSpouse'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;