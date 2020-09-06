use ods;
insert overwrite table ods_rcs_IcrCivilJudgement
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')         `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.court')            `court`,
          get_json_object(concat('{', mvtstr, '}'), '$.caseReason')       `caseReason`,
          get_json_object(concat('{', mvtstr, '}'), '$.registerDate')     `registerDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.closedType')       `closedType`,
          get_json_object(concat('{', mvtstr, '}'), '$.caseResult')       `caseResult`,
          get_json_object(concat('{', mvtstr, '}'), '$.caseValidateDate') `caseValidateDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.suitObject')       `suitObject`,
          get_json_object(concat('{', mvtstr, '}'), '$.suitObjectMoney')  `suitObjectMoney`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.IcrCivilJudgement'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`serialNo` is not null;