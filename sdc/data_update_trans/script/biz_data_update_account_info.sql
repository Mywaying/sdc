use ods;
drop table if  exists ods.odstmp_bpms_biz_data_update_account_info;
CREATE TABLE if not exists ods.odstmp_bpms_biz_data_update_account_info (
  id STRING COMMENT 'id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号',
  product_id STRING COMMENT '产品id',
  customer_name STRING COMMENT '客户姓名',
  sales_user_name STRING COMMENT '渠道经理姓名',
  update_module STRING COMMENT '修改模块',
  account_info STRING COMMENT '客户信息',
  authority STRING COMMENT '修改部分的权限',
  update_user_id STRING COMMENT '更新人id',
  update_time TIMESTAMP COMMENT '记录更新时间',
  `datetime` TIMESTAMP,
  create_user_id STRING COMMENT '创建人id',
  rev bigint comment '版本号',
  delete_flag STRING COMMENT '删除记录标识',
  create_time TIMESTAMP COMMENT '记录创建时间',
  company_code STRING COMMENT '分公司编号',
  flow_instance_id STRING COMMENT '流程实例id',
  datau_time timestamp COMMENT '数据更新成功时间',
o_is_platform_return_account STRING COMMENT '原是否选定为平台款还款账户',
o_type STRING COMMENT '原账户类型',
o_name STRING COMMENT '原户名',
o_number STRING COMMENT '原账户号',
o_open_bank STRING COMMENT '原开户行',
  n_is_platform_return_account STRING COMMENT '新是否选定为平台款还款账户',
n_type STRING COMMENT '新账户类型',
n_name STRING COMMENT '新户名',
n_number STRING COMMENT '新账户号',
n_open_bank STRING COMMENT '新开户行',
  remark STRING COMMENT '备注',
  otherremark STRING COMMENT '其它备注'

  ) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_data_update_account_info
select a.id
,a.apply_no
,a.house_no
,a.product_id
,a.customer_name
,a.sales_user_name
,a.update_module
,a.account_info
,a.authority
,a.update_user_id
,a.update_time
,a.`datetime`
,a.create_user_id
,a.rev
,a.delete_flag
,a.create_time
,a.company_code
,a.flow_instance_id
,a.datau_time
,get_json_object(concat('[', account_info_json, ']'),'$.[0].o.is_platform_return_account')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].o.type')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].o.name')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].o.number')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].o.open_bank')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].n.is_platform_return_account')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].n.type')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].n.name')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].n.number')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].n.open_bank')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].remark')
,get_json_object(concat('[', account_info_json, ']'),'$.[0].otherRemark')
from ods_bpms_biz_data_update_common a where instr(lower(update_module),'accountinfo')>0;

drop table if exists ods_bpms_biz_data_update_account_info;
ALTER TABLE odstmp_bpms_biz_data_update_account_info RENAME TO ods_bpms_biz_data_update_account_info;