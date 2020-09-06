use ods;
insert overwrite table ods.ods_rcs_icrCreditCue
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrCreditCue.reportNo')               `reportNo`,
        cast (get_json_object(`icrcreditdto`, '$.icrCreditCue.houseLoanCount') as double)         `houseLoanCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrCreditCue.otherLoanCount') as double)         `otherLoanCount`,
        get_json_object(`icrcreditdto`, '$.icrCreditCue.firstLoanOpenMonth')     `firstLoanOpenMonth`,
        cast (get_json_object(`icrcreditdto`, '$.icrCreditCue.loanCardCount')  as double)         `loanCardCount`,
        get_json_object(`icrcreditdto`, '$.icrCreditCue.firstLoanCardOpenMonth') `firstLoanCardOpenMonth`,
        cast (get_json_object(`icrcreditdto`, '$.icrCreditCue.standardLoanCardCount') as double)  `standardLoanCardCount`,
        get_json_object(`icrcreditdto`,       '$.icrCreditCue.firstStandardLoanCardOpenMonth')         `firstStandardLoanCardOpenMonth`,

        cast (get_json_object(`icrcreditdto`, '$.icrCreditCue.announceCount') as double)          `announceCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrCreditCue.dissentCount')  as double)          `dissentCount`,
        get_json_object(`icrcreditdto`, '$.icrCreditCue.score')                  `score`,
        get_json_object(`icrcreditdto`, '$.icrCreditCue.month')                  `month`,
        cast (get_json_object(`icrcreditdto`, '$.icrCreditCue.houseLoan2Count')   as double)      `houseLoan2Count`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;