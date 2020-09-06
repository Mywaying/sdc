use ods;
insert overwrite table ods_rcs_icrLoanInfo
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.class5State')            `class5State`,
          get_json_object(concat('{', mvtstr, '}'), '$.balance')                `balance`,
          get_json_object(concat('{', mvtstr, '}'), '$.remainPayMentcyc')       `remainPayMentcyc`,
          get_json_object(concat('{', mvtstr, '}'), '$.scheduledPayMentAmount') `scheduledPayMentAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.scheduledPayMentDate')   `scheduledPayMentDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.actualPayMentAmount')    `actualPayMentAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.recentPayDate')          `recentPayDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.currOverdueCyc')         `currOverdueCyc`,
          get_json_object(concat('{', mvtstr, '}'), '$.currOverdueAmount')      `currOverdueAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdue31To60Amount')    `overdue31To60Amount`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdue61To90Amount')    `overdue61To90Amount`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdue91To180Amount')   `overdue91To180Amount`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdueOver180Amount')   `overdueOver180Amount`,
          `_id`               `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')               `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.bizType')                `bizType`,
          get_json_object(concat('{', mvtstr, '}'), '$.cue')                    `cue`,
          get_json_object(concat('{', mvtstr, '}'), '$.financeorg')             `financeorg`,
          get_json_object(concat('{', mvtstr, '}'), '$.account')                `account`,
          get_json_object(concat('{', mvtstr, '}'), '$.type')                   `type`,
          get_json_object(concat('{', mvtstr, '}'), '$.currency')               `currency`,
          get_json_object(concat('{', mvtstr, '}'), '$.openDate')               `openDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.endDate')                `endDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.creditLimitAmount')      `creditLimitAmount`,
          get_json_object(concat('{', mvtstr, '}'), '$.guaranteeType')          `guaranteeType`,
          get_json_object(concat('{', mvtstr, '}'), '$.payMentRating')          `payMentRating`,
          get_json_object(concat('{', mvtstr, '}'), '$.payMentCyc')             `payMentCyc`,
          get_json_object(concat('{', mvtstr, '}'), '$.state')                  `state`,
          get_json_object(concat('{', mvtstr, '}'), '$.stateendDate')           `stateendDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.stateendMonth')          `stateendMonth`,
          get_json_object(concat('{', mvtstr, '}'), '$.beginMonth')             `beginMonth`,
          get_json_object(concat('{', mvtstr, '}'), '$.endMonth')               `endMonth`,
          get_json_object(concat('{', mvtstr, '}'), '$.latest24State')          `latest24State`,
          get_json_object(concat('{', mvtstr, '}'), '$.badBalance')             `badBalance`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdueBeginDate')       `overdueBeginDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.overdueEndDate')         `overdueEndDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.loanAcctState')          `loanAcctState`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrLoanInfo'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`class5State` is not null;