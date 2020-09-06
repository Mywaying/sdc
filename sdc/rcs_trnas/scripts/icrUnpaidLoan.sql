use ods;
insert overwrite table ods.ods_rcs_icrUnpaidLoan
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrUnpaidLoan.reportNo')                  `reportNo`,
        cast (get_json_object(`icrcreditdto`, '$.icrUnpaidLoan.financeCorpCount') as double)         `financeCorpCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrUnpaidLoan.financeOrgCount') as double)           `financeOrgCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrUnpaidLoan.accountCount')  as double)             `accountCount`,
        cast (get_json_object(`icrcreditdto`, '$.icrUnpaidLoan.creditLimit')   as double)             `creditLimit`,
        cast (get_json_object(`icrcreditdto`, '$.icrUnpaidLoan.balance') as double)                   `balance`,
        cast (get_json_object(`icrcreditdto`, '$.icrUnpaidLoan.latest6MonthUseDavgAmount')  as double)`latest6MonthUseDavgAmount`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;