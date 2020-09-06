use ods;
insert overwrite table ods_rcs_icrEndowmentInsuranceDeposit
  select *
  from (SELECT
          `_id`                 `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')                 `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.area')                     `area`,
          get_json_object(concat('{', mvtstr, '}'), '$.registerDate')             `registerDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.BigDecimal monthDuration') `BigDecimal monthDuration`,
          get_json_object(concat('{', mvtstr, '}'), '$.workDate')                 `workDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.state')                    `state`,
          get_json_object(concat('{', mvtstr, '}'), '$.BigDecimal ownBasicMoney') `BigDecimal ownBasicMoney`,
          get_json_object(concat('{', mvtstr, '}'), '$.BigDecimal money')         `BigDecimal money`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')                `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.pauseReason')              `pauseReason`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')                  `getTime`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrEndowmentInsuranceDeposit'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;