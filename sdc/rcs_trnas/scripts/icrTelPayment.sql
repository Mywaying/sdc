use ods;
insert overwrite table ods_rcs_icrTelPayment
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')     `serialNo`,
          cast (`_id` as string)     `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')    `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.type')         `type`,
          get_json_object(concat('{', mvtstr, '}'), '$.registerDate') `registerDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.state')        `state`,
          get_json_object(concat('{', mvtstr, '}'), '$.arrearMoney')  `arrearMoney`,
          get_json_object(concat('{', mvtstr, '}'), '$.arrearMonths') `arrearMonths`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')      `getTime`,
          get_json_object(concat('{', mvtstr, '}'), '$.status24')     `status24`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrTelPayment'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`serialNo` is not null;