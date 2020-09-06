use ods;
drop table if  exists ods.odstmp_bpms_biz_product_apply_info_common;
create table odstmp_bpms_biz_product_apply_info_common as
SELECT
a.id,
a.apply_no,
a.is_normal_pay,
a.apply_amount_capped,
a.contract_agency,
a.insurance_company_code,
a.intent_apply_amount,
a.rate_adjust_method,
a.business_type,
a.loan_term,
a.repay_cycle,
a.repay_method,
a.balloon_loan_term,
a.loan_usage,
sd.NAME_ loan_usage_name,
a.loan_usage_other,
a.loan_usage_nature,
a.borrower_industry_name,
a.borrower_profession_name,
a.is_online_query,
a.loan_rate,
a.remark,
a.create_user_id,
a.create_time,
a.update_user_id,
a.update_time,
a.rev,
a.delete_flag
FROM
ods_bpms_biz_product_apply_info a
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bpms_sys_dic_common where  TYPE_ID_='10000036070259' ) sd on sd.KEY_=a.loan_usage;
drop table if exists ods_bpms_biz_product_apply_info_common;
ALTER TABLE odstmp_bpms_biz_product_apply_info_common RENAME TO ods_bpms_biz_product_apply_info_common;