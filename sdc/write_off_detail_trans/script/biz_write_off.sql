
use ods;
drop table if  exists ods.odstmp_bpms_biz_write_off_common;
CREATE TABLE ods.odstmp_bpms_biz_write_off_common (
   id STRING,
   source STRING COMMENT '来源',
   loan_type STRING COMMENT '借贷类型(borrow借，loan贷)',
   affairs_type STRING COMMENT '账务类型',
   bank_tran_type STRING COMMENT '支付交易类型（预留）',
   bank_unique_no STRING COMMENT '流水唯一号',
   voucher_no STRING COMMENT '银行流水凭证号',
   buss_refer_no STRING COMMENT '业务参考号',
   merchants_no STRING COMMENT '商户号',
   terminal_no STRING COMMENT '终端号',
   bank_card_type STRING COMMENT '银行卡类型（平安或农行）',
   bank_card_no STRING COMMENT '公司银行卡号',
   bank_card_account STRING COMMENT '公司账户名',
   bank_card_code STRING COMMENT '公司银行卡行号编码',
   bank_card_name STRING COMMENT '公司账户开户行名称',
   trans_time TIMESTAMP COMMENT '交易时间',
   in_come_amonut DOUBLE COMMENT '交易金额（收入金额）',
   expenditure DOUBLE COMMENT '支出金额',
   card_balance DOUBLE COMMENT '卡的余额',
   front_amount DOUBLE COMMENT '上笔余额',
   tot_chg DOUBLE COMMENT '手续费总额',
   cust_card_no STRING COMMENT '客户卡号',
   cust_card_account STRING COMMENT '客户账户名',
   cust_card_code STRING COMMENT '客户银行卡行号编码',
   cust_card_name STRING COMMENT '客户银行卡开户行名称',
   narrative STRING COMMENT '附言用途',
   check_status STRING COMMENT '核销状态',
   wait_check_amount DOUBLE COMMENT '待核销金额',
   fund_package_code STRING COMMENT '资金包编码',
   fund_package_time TIMESTAMP COMMENT '资金包包月的时间',
   remark STRING COMMENT '备注',
   create_time TIMESTAMP COMMENT '创建时间',
   create_user_id STRING COMMENT '创建人',
   update_time TIMESTAMP COMMENT '更新时间',
   update_user_id STRING COMMENT '更新人',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '删除标识',
   check_status_name STRING COMMENT '核销状态',
   source_name STRING COMMENT '来源'
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_write_off_common
SELECT
bowd.id
,bowd.source
,bowd.loan_type
,bowd.affairs_type
,bowd.bank_tran_type
,bowd.bank_unique_no
,bowd.voucher_no
,bowd.buss_refer_no
,bowd.merchants_no
,bowd.terminal_no
,bowd.bank_card_type
,bowd.bank_card_no
,bowd.bank_card_account
,bowd.bank_card_code
,bowd.bank_card_name
,bowd.trans_time
,bowd.in_come_amonut
,bowd.expenditure
,bowd.card_balance
,bowd.front_amount
,bowd.tot_chg
,bowd.cust_card_no
,bowd.cust_card_account
,bowd.cust_card_code
,bowd.cust_card_name
,bowd.narrative
,bowd.check_status
,bowd.wait_check_amount
,bowd.fund_package_code
,bowd.fund_package_time
,bowd.remark
,bowd.create_time
,bowd.create_user_id
,bowd.update_time
,bowd.update_user_id
,bowd.rev
,bowd.delete_flag
,sd1.check_status
,sd2.source
FROM
ods_bpms_biz_write_off bowd
left join (SELECT KEY_,sd.NAME_ check_status FROM ods_bpms_sys_dic sd WHERE sd.TYPE_ID_='10000046980123')sd1 on sd1.KEY_ = bowd.check_status
left join (SELECT KEY_,sd.NAME_ source FROM ods_bpms_sys_dic sd WHERE sd.TYPE_ID_ = '10000010570024' )sd2 on sd2.KEY_ = bowd.source;
drop table if exists ods_bpms_biz_write_off_common;
ALTER TABLE odstmp_bpms_biz_write_off_common RENAME TO ods_bpms_biz_write_off_common;