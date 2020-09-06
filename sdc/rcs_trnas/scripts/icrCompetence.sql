use ods;
insert overwrite table ods_rcs_icrCompetence
  select *
  from (SELECT
          `_id`       `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')       `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.competencyName') `competencyName`,
          get_json_object(concat('{', mvtstr, '}'), '$.grade')          `grade`,
          get_json_object(concat('{', mvtstr, '}'), '$.awardDate')      `awardDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.endDate')        `endDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.revokeDate')     `revokeDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')      `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.area')           `area`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrCompetence'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;