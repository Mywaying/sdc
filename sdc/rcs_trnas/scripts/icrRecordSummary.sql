use ods;
insert overwrite table ods.ods_rcs_icrRecordSummary
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.reportNo')          `reportNo`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.orgSum1')           `orgSum1`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.orgSum2')           `orgSum2`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.recordSum1')        `recordSum1`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.recordSum2')        `recordSum2`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.recordSumSelf')     `recordSumSelf`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.towYearRecordSum1') `towYearRecordSum1`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.towYearRecordSum2') `towYearRecordSum2`,
        get_json_object(`icrcreditdto`, '$.icrRecordSummary.towYearRecordSum3') `towYearRecordSum3`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;