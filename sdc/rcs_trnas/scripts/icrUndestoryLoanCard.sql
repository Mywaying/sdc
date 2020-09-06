use ods;
insert overwrite table ods.ods_rcs_icrUndestoryLoanCard
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.reportNo')             `reportNo`,
        get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.financeCorpCount')     `financeCorpCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.financeOrgCount') as double)     `financeOrgCount`,
       cast ( get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.accountCount') as double)       `accountCount`,
       cast ( get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.creditLimit')    as double)      `creditLimit`,
       cast ( get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.maxCreditLimitPerOrg')  as double)`maxCreditLimitPerOrg`,
       cast ( get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.minCreditLimitPerOrg') as double)`minCreditLimitPerOrg`,
       cast ( get_json_object(`icrcreditdto`, '$.icrUndestoryLoanCard.usedCreditLimit')   as double)   `usedCreditLimit`,
       cast ( get_json_object(`icrcreditdto`,'$.icrUndestoryLoanCard.latest6MonthUseDavgAmount')  as double)           `latest6MonthUseDavgAmount`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;