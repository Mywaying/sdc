use ods;
insert overwrite table ods_rcs_IcrEndowmentInsuranceDeliver
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')    `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.area')        `area`,
          get_json_object(concat('{', mvtstr, '}'), '$.retireType')  `retireType`,
          get_json_object(concat('{', mvtstr, '}'), '$.retiredDate') `retiredDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.workDate')    `workDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.money')       `money`,
          get_json_object(concat('{', mvtstr, '}'), '$.pauseReason') `pauseReason`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')   `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')     `getTime`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.IcrEndowmentInsuranceDeliver'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`serialNo` is not null;