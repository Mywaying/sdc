use ods;
insert overwrite table ods_rcs_icrLoanCardInfo
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.shareCreditLimitAmount')    `shareCreditLimitAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.usedCreditLimitAmount')     `usedCreditLimitAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.latest6MonthUsedAvgAmount') `latest6MonthUsedAvgAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.usedHighestAmount')         `usedHighestAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.scheduledPaymentAmount')    `scheduledPaymentAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.scheduledPaymentDate')      `scheduledPaymentDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.actualPaymentAmount')       `actualPaymentAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.recentPayDate')             `recentPayDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.currOverdueCyc')            `currOverdueCyc`,
          get_json_object(concat('{', mvtstr, '}'), '$.currOverdueAmount')         `currOverdueAmount`,
          `_id`                  `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')                  `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.bizType')                   `bizType`,
          get_json_object(concat('{', mvtstr, '}'), '$.account')                   `account`,
          get_json_object(concat('{', mvtstr, '}'), '$.cue')                       `cue`,
          get_json_object(concat('{', mvtstr, '}'), '$.financeOrg')                `financeOrg`,
          get_json_object(concat('{', mvtstr, '}'), '$.currency')                  `currency`,
          get_json_object(concat('{', mvtstr, '}'), '$.openDate')                  `openDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.creditLimitAmount')         `creditLimitAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.guaranteeType')             `guaranteeType`,
          get_json_object(concat('{', mvtstr, '}'), '$.state')                     `state`,
          get_json_object(concat('{', mvtstr, '}'), '$.stateEndDate')              `stateEndDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.beginMonth')                `beginMonth`,
          get_json_object(concat('{', mvtstr, '}'), '$.endMonth')                  `endMonth`,
          get_json_object(concat('{', mvtstr, '}'), '$.latest24State')             `latest24State`,
          get_json_object(concat('{', mvtstr, '}'), '$.balance')                   `balance`,
          get_json_object(concat('{', mvtstr, '}'), '$.badBalance')                `badBalance`,
          get_json_object(concat('{', mvtstr, '}'), '$.loanAcctState')             `loanAcctState`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdueBeginDate')          `overdueBeginDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdueEndDate')            `overdueEndDate`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrLoanCardInfo'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`shareCreditLimitAmount` is not null;