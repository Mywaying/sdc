use ods;
insert overwrite table ods.ods_rcs_icrGuaranteeSummary
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrGuaranteeSummary.reportNo') `reportNo`,
        cast (get_json_object(`icrcreditdto`, '$.icrGuaranteeSummary.count') as double)   `count`,
        cast (get_json_object(`icrcreditdto`, '$.icrGuaranteeSummary.amount') as double)   `amount`,
        cast (get_json_object(`icrcreditdto`, '$.icrGuaranteeSummary.balance')  as double) `balance`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;