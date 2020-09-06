use ods;
insert overwrite table ods.ods_rcs_reportBaseInfo
select *
from (SELECT
        cast (`_id` as string)     `reportNo`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.reportType')       `reportType`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.operateUser')      `operateUser`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.operateOrg')       `operateOrg`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.queryReason')      `queryReason`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.name')             `name`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.certType')         `certType`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.certNo')           `certNo`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.queryTime')        `queryTime`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.reportCreateTime') `reportCreateTime`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.queryMode')        `queryMode`,
        get_json_object(`icrcreditdto`, '$.reportBaseInfo.reportPath')       `reportPath`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;