use ods;
insert overwrite table ods.ods_rcs_icrIdentity
select *
from (SELECT
        get_json_object(`icrcreditdto`, '$.icrIdentity.reportNo')          `reportNo`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.gender')            `gender`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.birthday')          `birthday`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.maritalState')      `maritalState`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.mobile')            `mobile`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.officetelePhoneno') `officetelePhoneno`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.hometelePhoneno')   `hometelePhoneno`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.edulevel')          `edulevel`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.edudegree')         `edudegree`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.postAddress')       `postAddress`,
        get_json_object(`icrcreditdto`, '$.icrIdentity.registeredAddress') `registeredAddress`
      from ods_rcs_uat_credit_icr_report) T
where T.`reportNo` is not null;