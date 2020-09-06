use ods;
drop table if  exists ods.odstmp_bims_biz_query_credit_common;
CREATE TABLE ods.odstmp_bims_biz_query_credit_common (
   id STRING COMMENT '主键id',
   apply_no STRING COMMENT '订单编号',
   customer_no STRING COMMENT '客户编号',
   customer_name STRING COMMENT '客户姓名',
   credit_type STRING COMMENT '征信类型',
   parse_time TIMESTAMP COMMENT '解析时间',
   parse_ret_time TIMESTAMP COMMENT '解析完成时间',
   credit_channel STRING COMMENT '征信获取方式',
   report_no STRING COMMENT '征信报告编号',
   report_from STRING COMMENT '征信报告来源',
   parse_way STRING COMMENT '解析方式',
   credit_status STRING COMMENT '征信状态',
   task_id STRING COMMENT '唯一任务标识(半刻查询结果使用)',
   cache_flag STRING COMMENT '是否走了缓存，未向机构发起解析请求(Y：本次解析走缓存N：本次解析未走缓存)',
   operate_user_id STRING COMMENT '操作人员id',
   operate_user_name STRING COMMENT '操作人员姓名',
   manual_credit_query_date STRING COMMENT '征信查询日期',
   manual_credit_name STRING COMMENT '姓名',
   manual_is_sensitive_career STRING COMMENT '是否为敏感职业',
   manual_cert_type STRING COMMENT '证件类型',
   manual_cert_no STRING COMMENT '证件号码',
   manual_query_times_month3 bigint comment '近3月查询次数',
   manual_query_times_month12 bigint comment '近12月查询次数',
   manual_m2_overdue_has STRING COMMENT '近3月有无M2以上逾期',
   manual_m2_overdue_name STRING COMMENT '近3月M2以上逾期信贷名称',
   manual_m4_overdue_has STRING COMMENT '近12月有无M4以上逾期',
   manual_m4_overdue_name STRING COMMENT '近12月M4以上逾期信贷名称',
   manual_month12_overdue_time bigint comment '近12月逾期次数',
   manual_month12_overdue_name STRING COMMENT '近12月逾期信贷名称',
   manual_month6_overdue_time bigint comment '近6月逾期次数',
   manual_month6_overdue_name STRING COMMENT '近6月逾期信贷名称',
   manual_dgz_has STRING COMMENT '近24月有无“D/G/Z”',
   manual_dgz_name STRING COMMENT '近24月“D/G/Z”信贷名称',
   manual_loan_status_json STRING COMMENT '贷款状态列表JSON：贷款名称，贷款账户状态，五级分类情况',
   manual_loan_end_date_json STRING COMMENT '贷款到期列表JSON：贷款名称，到期日，本金余额',
   manual_account_info_json STRING COMMENT '准/贷记卡账户信息列表JSON：名称，账户状态，当前预期次数',
   manual_guarantee_info_json STRING COMMENT '担保信息列表JSON：编号，本金余额，五级分类',
   manual_new_loan_has STRING COMMENT '有无新增无抵/质押贷款',
   manual_new_loan_count bigint comment '新增无抵/质押贷款合计笔数',
   manual_new_loan_amount DOUBLE COMMENT '新增无抵/质押贷款合计金额',
   manual_card_use_amount DOUBLE COMMENT '（准）贷记卡使用金额',
   manual_card_credit_line DOUBLE COMMENT '（准）贷记卡总授信金额',
   manual_overdraft70_percent DOUBLE COMMENT '是否超过70%百分比',
   manual_credit_total_amount DOUBLE COMMENT '无抵/质押贷款余额',
   manual_loan_use_limit DOUBLE COMMENT '贷记卡使用额度',
   manual_loan_overdue_amount DOUBLE COMMENT '准贷记卡透支余额',
   remark STRING COMMENT '备注',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   update_time TIMESTAMP COMMENT '记录更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
   credit_num bigint comment '上传数量',
   credit_modify_time TIMESTAMP COMMENT '最后上传时间',
   credit_operate_user STRING COMMENT '最后上传征信操作人' ,
   credit_type_name STRING COMMENT '征信类型名' ,
   credit_channel_name STRING COMMENT '征信获取方式名' ,
   rn bigint
   )  STORED AS parquet;
insert overwrite table odstmp_bims_biz_query_credit_common
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time asc) rn from (
select
a.`id`,
a.`apply_no`,
a.`customer_no`,
a.`customer_name`,
a.`credit_type`,
a.`parse_time`,
a.`parse_ret_time`,
a.`credit_channel`,
a.`report_no`,
a.`report_from`,
a.`parse_way`,
a.`credit_status`,
a.`task_id`,
a.`cache_flag`,
a.`operate_user_id`,
a.`operate_user_name`,
a.`manual_credit_query_date`,
a.`manual_credit_name`,
a.`manual_is_sensitive_career`,
a.`manual_cert_type`,
a.`manual_cert_no`,
a.`manual_query_times_month3`,
a.`manual_query_times_month12`,
a.`manual_m2_overdue_has`,
a.`manual_m2_overdue_name`,
a.`manual_m4_overdue_has`,
a.`manual_m4_overdue_name`,
a.`manual_month12_overdue_time`,
a.`manual_month12_overdue_name`,
a.`manual_month6_overdue_time`,
a.`manual_month6_overdue_name`,
a.`manual_dgz_has`,
a.`manual_dgz_name`,
a.`manual_loan_status_json`,
a.`manual_loan_end_date_json`,
a.`manual_account_info_json`,
a.`manual_guarantee_info_json`,
a.`manual_new_loan_has`,
a.`manual_new_loan_count`,
a.`manual_new_loan_amount`,
a.`manual_card_use_amount`,
a.`manual_card_credit_line`,
a.`manual_overdraft70_percent`,
a.`manual_credit_total_amount`,
a.`manual_loan_use_limit`,
a.`manual_loan_overdue_amount`,
a.`remark`,
a.`create_user_id`,
a.`update_user_id`,
a.`create_time`,
a.`update_time`,
a.`rev`,
a.`delete_flag`,
a.`credit_num`,
a.`credit_modify_time`,
a.`credit_operate_user`,
b.`NAME_` credit_type_name,
c.`NAME_` credit_channel_name
from ods_bims_biz_query_credit a
left join (
select DISTINCT KEY_,NAME_new_ NAME_ from ods_bims_sys_dic_common where type_id_='10000051500653' or type_id_='10000051500654' )b
on b.KEY_=lower(a.credit_type)
left join  (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bims_sys_dic_common where type_id_='10000051500652' or type_id_='10000051500654')c
on c.KEY_=lower(a.credit_channel))T;
drop table if exists ods_bims_biz_query_credit_common;
ALTER TABLE odstmp_bims_biz_query_credit_common RENAME TO ods_bims_biz_query_credit_common;