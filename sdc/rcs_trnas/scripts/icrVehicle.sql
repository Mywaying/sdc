use ods;
insert overwrite table ods_rcs_icrVehicle
  select *
  from (SELECT
          cast (`_id` as string)     `reportNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.serialNo')     `serialNo`,
          get_json_object(concat('{', mvtstr, '}'), '$.engineCode')   `engineCode`,
          get_json_object(concat('{', mvtstr, '}'), '$.licenseCode')  `licenseCode`,
          get_json_object(concat('{', mvtstr, '}'), '$.brand')        `brand`,
          get_json_object(concat('{', mvtstr, '}'), '$.carType')      `carType`,
          get_json_object(concat('{', mvtstr, '}'), '$.useCharacter') `useCharacter`,
          get_json_object(concat('{', mvtstr, '}'), '$.state')        `state`,
          get_json_object(concat('{', mvtstr, '}'), '$.pledgeFlag')   `pledgeFlag`,
          get_json_object(concat('{', mvtstr, '}'), '$.getTime')      `getTime`
        from
          ods_rcs_uat_credit_icr_report LATERAL VIEW  explode(split(regexp_replace(regexp_replace(get_json_object(`icrcreditdto`, '$.icrVehicle'), '\\[\\{', ''), '}]', ''), '},\\{')) addTable as mvtstr) T
where T.`reportNo` is not null;