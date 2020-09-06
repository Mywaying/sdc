use ods;
insert overwrite table ods_rcs_icrRecordDetail
  select *
  from (SELECT
          cast (`_id` as string)     `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')    `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.queryDate')   `queryDate`,
          get_json_object(concat('{', mvtstr, '}'), '$.querier')     `querier`,
          get_json_object(concat('{', mvtstr, '}'), '$.queryReason') `queryReason`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrRecordDetail'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`serialNo` is not null;