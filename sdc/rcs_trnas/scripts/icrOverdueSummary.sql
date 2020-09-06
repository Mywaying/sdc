use ods;
insert overwrite table ods.ods_rcs_icrOverdueSummary
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrOverdueSummary.reportNo')     `reportNo`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.count') as double)        `count`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.months') as double)      `months`,
        cast (get_json_object(`icrcreditdto`,
                        '$.icrOverdueSummary.highestOverdueAmountPermon')as double)   `highestOverdueAmountPermon`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.maxDuration') as double) `maxDuration`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.count2') as double)      `count2`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.months2') as double)  `months2`,
        cast (get_json_object(`icrcreditdto`,
                        '$.icrOverdueSummary.highestOverdueAmountPermon2')as double)  `highestOverdueAmountPermon2`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.maxDuration2')as double) `maxDuration2`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.count3')  as double)        `count3`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.months3') as double)     `months3`,
        cast (get_json_object(`icrcreditdto`,
                        '$.icrOverdueSummary.highestOverdueAmountPermon3') as double)    `highestOverdueAmountPermon3`,
        cast (get_json_object(`icrcreditdto`, '$.icrOverdueSummary.maxDuration3')as double)    `maxDuration3`

      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;