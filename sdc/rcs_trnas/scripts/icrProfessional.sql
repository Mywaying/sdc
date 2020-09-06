use ods;
insert overwrite table ods_rcs_icrProfessional
  select *,
  ROW_NUMBER() OVER(PARTITION BY `reportNo` ORDER BY `getTime` desc) rn
  from (SELECT
          `_id`        `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')        `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.employer')        `employer`,
          get_json_object(concat('{', mvtstr, '}'), '$.employerAddress') `employerAddress`,
          get_json_object(concat('{', mvtstr, '}'), '$.occupation')      `occupation`,
          get_json_object(concat('{', mvtstr, '}'), '$.industry')        `industry`,
          get_json_object(concat('{', mvtstr, '}'), '$.duty')            `duty`,
          get_json_object(concat('{', mvtstr, '}'), '$.title')           `title`,
          get_json_object(concat('{', mvtstr, '}'), '$.startYear')       `startYear`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')         `getTime`,
          get_json_object(concat('{', mvtstr, '}'), '$.sortNo')          `sortNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.isSpecial')       `isSpecial`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrProfessional'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`serialNo` is not null;