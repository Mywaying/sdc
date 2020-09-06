use ods;
insert overwrite table ods_rcs_icrForceExecution
  select *
  from (SELECT
          `_id`                      `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')                      `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.court')                         `court`,
          get_json_object(concat('{', mvtstr, '}'), '$.caseReason')                    `caseReason`,
          get_json_object(concat('{', mvtstr, '}'), '$.registerDate')                  `registerDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.closedType')                    `closedType`,
          get_json_object(concat('{', mvtstr, '}'), '$.casesTate')                     `casesTate`,
          get_json_object(concat('{', mvtstr, '}'), '$.closedDate')                    `closedDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.enforceBigDecimal')             `enforceBigDecimal`,
          get_json_object(concat('{', mvtstr, '}'), '$.enforceBigDecimalMoney')        `enforceBigDecimalMoney`,
          get_json_object(concat('{', mvtstr, '}'), '$.alreadyEnforceBigDecimal')      `alreadyEnforceBigDecimal`,
          get_json_object(concat('{', mvtstr, '}'), '$.alreadyEnforceBigDecimalMoney') `alreadyEnforceBigDecimalMoney`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrForceExecution'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;