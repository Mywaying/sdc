use ods;
drop table if exists odstmp_bpms_c_fund_module_common;
CREATE TABLE if not exists ods.odstmp_bpms_c_fund_module_common (
   id STRING COMMENT 'id',
   apply_no STRING COMMENT '订单编号',
   source_funds STRING COMMENT '资金来源',
   con_funds_time TIMESTAMP COMMENT '确认资金到账时间',
   con_funds_cost STRING COMMENT '确认资金到账金额',
   con_plat_date TIMESTAMP COMMENT '确认到账平台起息日',
   con_plat_expire TIMESTAMP COMMENT '确认资金到账平台到期日',
   con_payee_acct STRING COMMENT '确认资金到账收款方',
   con_payee_acct_no STRING COMMENT '确认资金到账收款卡号',
   con_payee_acct_name STRING COMMENT '确认资金到账收款户名',
   con_funds_deal STRING COMMENT '确认资金到账交易明细id',
   con_exec_times bigint comment '确认资金到账任务执行次数',
   con_payee_bank STRING COMMENT '确认资金到账收款开户行',
   con_arrival_type STRING COMMENT '确认本金到账放式(系统查询/线下查询)',
   con_arrival_status STRING COMMENT '确认本金到账状态(已确认/未确认)',
   transfer_re_status STRING COMMENT '还款资金划转状态(N未划转 Y已划转)',
   plat_arrival STRING COMMENT '平台款是否到账',
   plat_source_funds STRING COMMENT '平台款资金来源',
   plat_arrival_time TIMESTAMP COMMENT '平台款到账时间',
   plat_recei_acct_no STRING COMMENT '平台收款账户',
   plat_recei_acct_name STRING COMMENT '平台收款账户户名',
   plat_recei_bank_name STRING COMMENT '平台收款账户开户行',
   plat_interest_date TIMESTAMP COMMENT '平台起息日',
   plat_rest_date TIMESTAMP COMMENT '平台到期日',
   plat_arriva_money DOUBLE COMMENT '平台款到账金额',
   plat_arrival_deal STRING COMMENT '平台款到账交易明细id',
   ad_pay_time TIMESTAMP COMMENT '赎楼前垫资回款时间',
   ad_pay_cost DOUBLE COMMENT '赎楼前垫资回款金额',
   ad_pay_deal STRING COMMENT '赎楼前垫资回款交易明细id',
   transfer_deal STRING COMMENT '资金划拨申请交易明细id',
   advance_cost DOUBLE COMMENT '垫资金额',
   fact_pay_source STRING COMMENT '实际回款来源',
   loan_pay_day TIMESTAMP COMMENT '借款回款日',
   re_arrival_deal STRING COMMENT '回款到账交易明细id',
   cust_re_acct_no STRING COMMENT '客户回款账户账号',
   cust_re_acct_name STRING COMMENT '客户回款账户户名',
   cust_re_bank_name STRING COMMENT '客户回款账户开户行',
   cust_re_bank_code STRING COMMENT '客户回款开户行编码',
   cust_pay_day TIMESTAMP COMMENT '客户回款日',
   cust_re_source STRING COMMENT '客户回款来源',
   cust_exec_times bigint comment '客户回款执行次数',
   cust_re_deal STRING COMMENT '客户回款交易明细id',
   should_re_capital DOUBLE COMMENT '应还本金',
   fact_re_capital DOUBLE COMMENT '实还本金',
   should_re_interest DOUBLE COMMENT '应还利息',
   fact_re_interest DOUBLE COMMENT '实际还利息',
   should_re_penalty DOUBLE COMMENT '应还罚息',
   penalty_overdue DOUBLE COMMENT '罚息—逾期（元）',
   penalty_show_time DOUBLE COMMENT '罚息—展期（元）',
   penalty_advanced_refund DOUBLE COMMENT '罚息—提前还款（元）',
   fact_re_penalty DOUBLE COMMENT '实还罚息',
   should_plat_cost DOUBLE COMMENT '应还平台费',
   fact_plat_cost DOUBLE COMMENT '实还平台费',
   should_re_sum DOUBLE COMMENT '应还合计',
   fact_re_sum DOUBLE COMMENT '实还合计',
   re_deal STRING COMMENT '归还申请交易明细id',
   plat_rece_path STRING COMMENT '平台回款路径',
   principal_trans_id STRING COMMENT '本息划拨申请交易id',
   trans_flow_id STRING COMMENT '划拨流程实例id',
   return_flow_id STRING COMMENT '归还流程实例id',
   parent_flow_id STRING COMMENT '父流程实例id',
   cust_should_re_capital DOUBLE COMMENT '本息划应还本金',
   cust_fact_re_capital DOUBLE COMMENT '本息划拨实还本金',
   cust_should_re_interest DOUBLE COMMENT '本息划拨应还利息',
   cust_fact_re_interest DOUBLE COMMENT '本息划拨实还利息',
   cust_should_re_penalty DOUBLE COMMENT '本息划拨应还罚息',
   cust_fact_re_penalty DOUBLE COMMENT '本息划拨实还罚息',
   cust_should_plat_cost DOUBLE COMMENT '本息划拨应还平台费',
   cust_fact_plat_cost DOUBLE COMMENT '本息划拨实还平台费',
   cust_should_re_sum DOUBLE COMMENT '本息划拨应还合计',
   cust_fact_re_sum DOUBLE COMMENT '本息划拨实还合计',
   create_user_id STRING COMMENT '创建人',
   create_time TIMESTAMP COMMENT '创建时间',
   update_user_id STRING COMMENT '更新人',
   update_time TIMESTAMP COMMENT '更新时间',
   rev STRING COMMENT '版本号',
   delete_flag STRING COMMENT '删除标识',
   plat_recei_acct STRING COMMENT '平台收款方',
   con_payee_acct_last STRING COMMENT '确认资金到账上一次收款方',
   deal_time TIMESTAMP COMMENT '交易时间',
   bank_number STRING COMMENT '银行流水号',
   count_head_tail STRING COMMENT '费用算头尾方式（suanTouBuSuanWei 算头不算尾 suanTouSuanWei 算头算尾）',
   is_need_internal STRING COMMENT '是否需要内部转款（资金归还场景的内部转款）',
   internal_trans_id STRING COMMENT '内部转款的交易id',
   rn bigint comment '排序',
   re_arrival_settl_way STRING COMMENT '回款结算方式',
   re_arrival_settl_way_name STRING COMMENT '回款结算方式名'
) STORED AS parquet;
insert overwrite table odstmp_bpms_c_fund_module_common
select
  T.*,
  cct.settl_way re_arrival_settl_way,
  b.NAME_ re_arrival_settl_way_name
from
  (SELECT
a.id
,a.apply_no
,a.source_funds
,a.con_funds_time
,a.con_funds_cost
,a.con_plat_date
,a.con_plat_expire
,a.con_payee_acct
,a.con_payee_acct_no
,a.con_payee_acct_name
,a.con_funds_deal
,a.con_exec_times
,a.con_payee_bank
,a.con_arrival_type
,a.con_arrival_status
,a.transfer_re_status
,a.plat_arrival
,a.plat_source_funds
,a.plat_arrival_time
,a.plat_recei_acct_no
,a.plat_recei_acct_name
,a.plat_recei_bank_name
,a.plat_interest_date
,a.plat_rest_date
,a.plat_arriva_money
,a.plat_arrival_deal
,a.ad_pay_time
,a.ad_pay_cost
,a.ad_pay_deal
,a.transfer_deal
,a.advance_cost
,a.fact_pay_source
,a.loan_pay_day
,a.re_arrival_deal
,a.cust_re_acct_no
,a.cust_re_acct_name
,a.cust_re_bank_name
,a.cust_re_bank_code
,a.cust_pay_day
,a.cust_re_source
,a.cust_exec_times
,a.cust_re_deal
,a.should_re_capital
,a.fact_re_capital
,a.should_re_interest
,a.fact_re_interest
,a.should_re_penalty
,a.penalty_overdue
,a.penalty_show_time
,a.penalty_advanced_refund
,a.fact_re_penalty
,a.should_plat_cost
,a.fact_plat_cost
,a.should_re_sum
,a.fact_re_sum
,a.re_deal
,a.plat_rece_path
,a.principal_trans_id
,a.trans_flow_id
,a.return_flow_id
,a.parent_flow_id
,a.cust_should_re_capital
,a.cust_fact_re_capital
,a.cust_should_re_interest
,a.cust_fact_re_interest
,a.cust_should_re_penalty
,a.cust_fact_re_penalty
,a.cust_should_plat_cost
,a.cust_fact_plat_cost
,a.cust_should_re_sum
,a.cust_fact_re_sum
,a.create_user_id
,a.create_time
,a.update_user_id
,a.update_time
,a.rev
,a.delete_flag
,a.plat_recei_acct
,a.con_payee_acct_last
,a.deal_time
,a.bank_number
,a.count_head_tail
,a.is_need_internal
,a.internal_trans_id
,ROW_NUMBER() OVER (PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_c_fund_module a
) T
left join ods.ods_bpms_c_cost_trade cct on cct.cost_id=T.re_arrival_deal
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods.ods_bpms_sys_dic_common where type_id_='10000053770004') b on b.KEY_=lower(cct.settl_way)
where rn=1;
drop table if exists ods_bpms_c_fund_module_common;
ALTER TABLE odstmp_bpms_c_fund_module_common
RENAME TO ods_bpms_c_fund_module_common;
