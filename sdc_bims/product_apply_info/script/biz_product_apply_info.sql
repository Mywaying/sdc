use ods;
drop table if  exists ods.odstmp_bims_biz_product_apply_info_common;
CREATE TABLE ods.odstmp_bims_biz_product_apply_info_common (
   id STRING COMMENT 'id',
   apply_no STRING COMMENT '订单编号',
   is_normal_pay STRING COMMENT '最近6个月是否连续按月正常缴纳公积金',
   apply_amount_capped DOUBLE COMMENT '申请金额上限',
   contract_agency STRING COMMENT '签约机构',
   insurance_company_code STRING COMMENT '保险公司编码',
   intent_apply_amount DOUBLE COMMENT '意向申请金额',
   rate_adjust_method STRING COMMENT '利率调整方式',
   business_type STRING COMMENT '业务类型',
   wx_product_code STRING COMMENT '选择的产品ID',
   loan_term STRING COMMENT '贷款期限',
   repay_cycle STRING COMMENT '还款周期',
   repay_method STRING COMMENT '还款方式',
   balloon_loan_term STRING COMMENT '气球贷摊还期限（单位是“月”）',
   loan_usage STRING COMMENT '贷款用途',
   loan_usage_other STRING COMMENT '贷款用途其他（当贷款用途选择其他时，该字段必填值）',
   loan_usage_nature STRING COMMENT '贷款用途性质',
   borrower_industry_name STRING COMMENT '借款人行业名称',
   borrower_profession_name STRING COMMENT '借款人职业名称',
   is_online_query STRING COMMENT '是否线上查询征信（0：否，1：是）',
   loan_rate DOUBLE COMMENT '贷款利率',
   remark STRING COMMENT '备注',
   create_user_id STRING COMMENT '创建人',
   create_time TIMESTAMP COMMENT '创建时间',
   update_user_id STRING COMMENT '更新人',
   update_time TIMESTAMP COMMENT '更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '删除标识',
   repay_method_name STRING COMMENT '还款方式名'
)  STORED AS parquet;
insert overwrite table odstmp_bims_biz_product_apply_info_common
select
a.`id`,
a.`apply_no`,
a.`is_normal_pay`,
a.`apply_amount_capped`,
a.`contract_agency`,
a.`insurance_company_code`,
a.`intent_apply_amount`,
a.`rate_adjust_method`,
a.`business_type`,
a.`wx_product_code`,
a.`loan_term`,
a.`repay_cycle`,
a.`repay_method`,
a.`balloon_loan_term`,
a.`loan_usage`,
a.`loan_usage_other`,
a.`loan_usage_nature`,
a.`borrower_industry_name`,
a.`borrower_profession_name`,
a.`is_online_query`,
a.`loan_rate`,
a.`remark`,
a.`create_user_id`,
a.`create_time`,
a.`update_user_id`,
a.`update_time`,
a.`rev`,
a.`delete_flag`,
b.NAME_ repay_method_name
FROM ods_bims_biz_product_apply_info a
left join (
select DISTINCT KEY_,NAME_new_ NAME_ from ods_bims_sys_dic_common where type_id_='10000036070256')b
on b.KEY_=lower(a.repay_method);
drop table if exists ods_bims_biz_product_apply_info_common;
ALTER TABLE odstmp_bims_biz_product_apply_info_common RENAME TO ods_bims_biz_product_apply_info_common;