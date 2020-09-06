use ods;
insert overwrite table ods_rcs_icrCardGuarantee
  select *
  from (SELECT
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')        `serialNo`,
          `_id`        `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.organName')       `organName`,
          get_json_object(concat('{', mvtstr, '}'), '$.creditLimit')     `creditLimit`,
          get_json_object(concat('{', mvtstr, '}'), '$.beginDate')       `beginDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.guananteeMoney')  `guananteeMoney`,
          get_json_object(concat('{', mvtstr, '}'), '$.usedLimit')       `usedLimit`,
          get_json_object(concat('{', mvtstr, '}'), '$.guarantorName')   `guarantorName`,
          get_json_object(concat('{', mvtstr, '}'), '$.guarantorIdNo')   `guarantorIdNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.guarantorIdType') `guarantorIdType`,
          get_json_object(concat('{', mvtstr, '}'), '$.billingDate')     `billingDate`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrCardGuarantee'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`serialNo` is not null;