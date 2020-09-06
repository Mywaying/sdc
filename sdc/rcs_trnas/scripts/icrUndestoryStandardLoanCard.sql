use ods;
insert overwrite table ods.ods_rcs_icrUndestoryStandardLoanCard
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.reportNo')             `reportNo`,
        cast (get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.financeCorpCount') as double)     `financeCorpCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.financeOrgCount') as double)      `financeOrgCount`,
         cast (get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.accountCount') as double)         `accountCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.creditLimit') as double)          `creditLimit`,
        cast (get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.maxCreditLimitPerOrg') as double) `maxCreditLimitPerOrg`,
        cast (get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.minCreditLimitPerOrg') as double) `minCreditLimitPerOrg`,
        cast (get_json_object(`icrcreditdto`, '$.icrUndestoryStandardLoanCard.usedCreditLimit') as double)      `usedCreditLimit`,
        cast (get_json_object(`icrcreditdto`,'$.icrUndestoryStandardLoanCard.latest6MonthUseDavgAmount') as double)            `latest6MonthUseDavgAmount`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;