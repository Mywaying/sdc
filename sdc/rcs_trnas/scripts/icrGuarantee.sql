use ods;
insert overwrite table ods_rcs_icrGuarantee
  select *
  from (SELECT
          `_id`         `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')         `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')        `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.contractMoney')    `contractMoney`,
          get_json_object(concat('{', mvtstr, '}'), '$.beginDate')        `beginDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.endDate')          `endDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.guananteeMoney')   `guananteeMoney`,
          get_json_object(concat('{', mvtstr, '}'), '$.guaranteeBalance') `guaranteeBalance`,
          get_json_object(concat('{', mvtstr, '}'), '$.class5State')      `class5State`,
          get_json_object(concat('{', mvtstr, '}'), '$.billingDate')      `billingDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.guarantorName')    `guarantorName`,
          get_json_object(concat('{', mvtstr, '}'), '$.guarantorIdNo')    `guarantorIdNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.guarantorIdType')  `guarantorIdType`,
          get_json_object(concat('{', mvtstr, '}'), '$.sortNo')           `sortNo`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrGuarantee'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;