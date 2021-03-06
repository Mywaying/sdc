use ods;
drop table if exists odstmp_bpms_biz_new_loan_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_new_loan_common (
  id STRING COMMENT '主键id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号关联房产信息',
  new_loan_bank_name STRING COMMENT '新贷款银行名称',
  new_loan_bank_code STRING COMMENT '新贷款银行编号',
  new_bank_email STRING COMMENT '银行联系邮箱',
  new_bank_address STRING COMMENT '银行地址',
  new_bank_user STRING COMMENT '银行联系人',
  new_bank_phone STRING COMMENT '联系方式',
  loan_terms STRING COMMENT '贷款期数',
  term_unit STRING COMMENT '期限单位；D:天；M:月；Y:年;',
  seller_card_no STRING COMMENT '借款人身份证号码',
  seller_name STRING COMMENT '借款人姓名',
  biz_loan_amount DOUBLE COMMENT '商业贷款金额（ 元）',
  new_loan_rate DOUBLE COMMENT '贷款利率',
  new_rate_multiply DOUBLE COMMENT '利率倍数',
  repay_method_loan STRING COMMENT '还款方式 等额本金：DEBJ  等额利息：DELX',
  is_release STRING COMMENT '状态类型，01-可放款',
  is_regulation_flag STRING COMMENT '是否有监管  有：Y  没：N ',
  first_regulation_bank_name STRING COMMENT '首期监管款银行',
  first_regulation_bank_code STRING COMMENT '首期监管款银行编号',
  first_regulation_bank_account STRING COMMENT '首期监管款账户',
  first_regulation_amount DOUBLE COMMENT '首期监管款金额（元）',
  remark STRING COMMENT '备注',
  create_user_id STRING COMMENT '创建人id',
  update_user_id STRING COMMENT '更新人id',
  create_time timestamp COMMENT '记录创建时间',
  update_time timestamp COMMENT '记录更新时间',
  rev bigint comment '版本号',
  delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
  serial_no STRING COMMENT '额度协议编号',
  currency STRING COMMENT '币种',
  business_sum DOUBLE COMMENT '名义金额',
  contract_no STRING COMMENT '合同流水号zyb',
  borrow_cont_no STRING COMMENT '借款合同编号',
  max_mortgage_no STRING COMMENT '最高额抵押合同编号',
  start_time timestamp COMMENT '超始日',
  end_time timestamp COMMENT 'ENDTIME',
  provident_fund_loan_amount DOUBLE COMMENT '公积金贷款金额',
  have_new_loan STRING COMMENT '有无新贷款信息',
  is_need_mortgage STRING COMMENT '是否需我司办理按揭',
  is_pay_type_check STRING,
  new_loan_type STRING COMMENT '贷款类型（对应字典dklx）',
  natural_rate_code STRING COMMENT '基准利率代码',
  natural_float_type STRING COMMENT '利率浮动方式(对应字典llfdfs)',
  provident_fund_center STRING COMMENT '贷款公积金中心',
  natural_float_value DOUBLE COMMENT '利率浮动值',
  new_loan_customer STRING COMMENT '客户经理',
  new_loan_customer_tel STRING COMMENT '客户经理联系方式',
  new_loan_fund_center STRING COMMENT '贷款公积金中心',
  new_loan_guaranty STRING COMMENT '担保方式',
  fund_rate DOUBLE COMMENT '公积金贷款利率',
  fund_loan_terms STRING COMMENT '公积金贷款期限',
  fund_term_unit STRING COMMENT '公积金期限单位；D:天；M:月；Y:年;',
  has_fund_supervision STRING COMMENT '是否已做资金监管？(Y：是，N：否)',
  supervision_time timestamp COMMENT '预计监管时间',
  supervision_agreement_get_time timestamp COMMENT '监管协议领取时间',
  borrower_type STRING COMMENT '新贷款借款人类型(1:个人 2:公司)',
  transfer_loan_cause STRING COMMENT '转贷动机(1利率、2还款方式、3期限、4额度)',
  third_type STRING COMMENT '第三方类型（ 个人，公司）',
  is_have_fund_loan STRING COMMENT '是否有公积金贷款（Y：是，N：否）',
  third_payee STRING COMMENT '新贷款借款人',
  other_non_bank STRING COMMENT '其他非银机构',
  agree_loan_type STRING COMMENT '同贷类型；字典：AgreeLoanType',
  agree_loan_source STRING COMMENT '同贷来源；字典：AgreeLoanSource',
  agree_loan_other_remark STRING COMMENT '同贷其他来源备注',
  revolving_credit_flag STRING COMMENT '是否循环授信贷款，1表示是，0表示否',
  apply_withdraw_amount DOUBLE COMMENT '申请取款金额（元）',
  total_loan_amount DOUBLE  COMMENT '借款总金额',
  agree_loan_source_name STRING COMMENT '同贷来源名称',
  rn bigint
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_new_loan_common
select 
T.*
,sdic.agree_loan_source_name  
,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY biz_loan_amount desc) rn
from
(
SELECT
a.id,
a.apply_no,
a.house_no,
a.new_loan_bank_name,
a.new_loan_bank_code,
a.new_bank_email,
a.new_bank_address,
a.new_bank_user,
a.new_bank_phone,
a.loan_terms,
a.term_unit,
a.seller_card_no,
a.seller_name,
a.biz_loan_amount,
a.new_loan_rate,
a.new_rate_multiply,
a.repay_method_loan,
a.is_release,
regexp_replace(regexp_replace(lower(a.is_regulation_flag),'y','是'),'n','否'),
a.first_regulation_bank_name,
a.first_regulation_bank_code,
a.first_regulation_bank_account,
a.first_regulation_amount,
a.remark,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
a.serial_no,
a.currency,
a.business_sum,
a.contract_no,
a.borrow_cont_no,
a.max_mortgage_no,
a.start_time,
a.end_time,
a.provident_fund_loan_amount,
a.have_new_loan,
a.is_need_mortgage,
a.is_pay_type_check,
a.new_loan_type,
a.natural_rate_code,
a.natural_float_type,
a.provident_fund_center,
a.natural_float_value,
a.new_loan_customer,
a.new_loan_customer_tel,
a.new_loan_fund_center,
a.new_loan_guaranty,
a.fund_rate,
a.fund_loan_terms,
a.fund_term_unit,
a.has_fund_supervision,
a.supervision_time,
a.supervision_agreement_get_time,
a.borrower_type,
a.transfer_loan_cause,
a.third_type,
a.is_have_fund_loan,
a.third_payee,
a.other_non_bank,
a.agree_loan_type,
a.agree_loan_source,
a.agree_loan_other_remark,
a.revolving_credit_flag,
a.apply_withdraw_amount,
a.total_loan_amount
FROM
( select * from ods_bpms_biz_new_loan where (apply_no<>'' and apply_no is not null) and(house_no='' or house_no is null ) ) a
union all
SELECT
a1.id,
c1.apply_no,
a1.house_no,
a1.new_loan_bank_name,
a1.new_loan_bank_code,
a1.new_bank_email,
a1.new_bank_address,
a1.new_bank_user,
a1.new_bank_phone,
a1.loan_terms,
a1.term_unit,
a1.seller_card_no,
a1.seller_name,
a1.biz_loan_amount,
a1.new_loan_rate,
a1.new_rate_multiply,
a1.repay_method_loan,
a1.is_release,
regexp_replace(regexp_replace(lower(a1.is_regulation_flag),'y','是'),'n','否'),
a1.first_regulation_bank_name,
a1.first_regulation_bank_code,
a1.first_regulation_bank_account,
a1.first_regulation_amount,
a1.remark,
a1.create_user_id,
a1.update_user_id,
a1.create_time,
a1.update_time,
a1.rev,
a1.delete_flag,
a1.serial_no,
a1.currency,
a1.business_sum,
a1.contract_no,
a1.borrow_cont_no,
a1.max_mortgage_no,
a1.start_time,
a1.end_time,
a1.provident_fund_loan_amount,
a1.have_new_loan,
a1.is_need_mortgage,
a1.is_pay_type_check,
a1.new_loan_type,
a1.natural_rate_code,
a1.natural_float_type,
a1.provident_fund_center,
a1.natural_float_value,
a1.new_loan_customer,
a1.new_loan_customer_tel,
a1.new_loan_fund_center,
a1.new_loan_guaranty,
a1.fund_rate,
a1.fund_loan_terms,
a1.fund_term_unit,
a1.has_fund_supervision,
a1.supervision_time,
a1.supervision_agreement_get_time,
a1.borrower_type,
a1.transfer_loan_cause,
a1.third_type,
a1.is_have_fund_loan,
a1.third_payee,
a1.other_non_bank,
a1.agree_loan_type,
a1.agree_loan_source,
a1.agree_loan_other_remark,
a1.revolving_credit_flag,
a1.apply_withdraw_amount,
a1.total_loan_amount
FROM
( select * from ods_bpms_biz_new_loan where (apply_no<>'' and apply_no is not null) and(house_no<>'' and house_no is not null ) ) a1
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c1 ON c1.house_no = a1.house_no
union all
select
b.id,
c.apply_no apply_no,
b.house_no,
b.new_loan_bank_name,
b.new_loan_bank_code,
b.new_bank_email,
b.new_bank_address,
b.new_bank_user,
b.new_bank_phone,
b.loan_terms,
b.term_unit,
b.seller_card_no,
b.seller_name,
b.biz_loan_amount,
b.new_loan_rate,
b.new_rate_multiply,
b.repay_method_loan,
b.is_release,
regexp_replace(regexp_replace(lower(b.is_regulation_flag),'y','是'),'n','否'),
b.first_regulation_bank_name,
b.first_regulation_bank_code,
b.first_regulation_bank_account,
b.first_regulation_amount,
b.remark,
b.create_user_id,
b.update_user_id,
b.create_time,
b.update_time,
b.rev,
b.delete_flag,
b.serial_no,
b.currency,
b.business_sum,
b.contract_no,
b.borrow_cont_no,
b.max_mortgage_no,
b.start_time,
b.end_time,
b.provident_fund_loan_amount,
b.have_new_loan,
b.is_need_mortgage,
b.is_pay_type_check,
b.new_loan_type,
b.natural_rate_code,
b.natural_float_type,
b.provident_fund_center,
b.natural_float_value,
b.new_loan_customer,
b.new_loan_customer_tel,
b.new_loan_fund_center,
b.new_loan_guaranty,
b.fund_rate,
b.fund_loan_terms,
b.fund_term_unit,
b.has_fund_supervision,
b.supervision_time,
b.supervision_agreement_get_time,
b.borrower_type,
b.transfer_loan_cause,
b.third_type,
b.is_have_fund_loan,
b.third_payee,
b.other_non_bank,
b.agree_loan_type,
b.agree_loan_source,
b.agree_loan_other_remark,
b.revolving_credit_flag,
b.apply_withdraw_amount,
b.total_loan_amount from ( SELECT
*
FROM
ods_bpms_biz_new_loan
WHERE
(house_no <> '' AND house_no IS NOT NULL ) and (apply_no ='' or apply_no is null )
) b
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c ON c.house_no = b.house_no
)T
left join (select key_,name_new_ agree_loan_source_name from ods_bpms_sys_dic_common where type_id_='10000076380257') sdic on sdic.key_=lower(T.agree_loan_source)
;
drop table if exists ods_bpms_biz_new_loan_common;
ALTER TABLE odstmp_bpms_biz_new_loan_common RENAME TO ods_bpms_biz_new_loan_common;