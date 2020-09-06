use ods;
drop table if  exists ods.odstmp_bpms_biz_data_update_deal_info;
CREATE TABLE if not exists ods.odstmp_bpms_biz_data_update_deal_info (
  id STRING COMMENT 'id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号',
  product_id STRING COMMENT '产品id',
  customer_name STRING COMMENT '客户姓名',
  sales_user_name STRING COMMENT '渠道经理姓名',
  update_module STRING COMMENT '修改模块',
  deal_info STRING COMMENT '客户信息',
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
o_trading_price DOUBLE COMMENT '原合同成交价（元）',
o_actual_trading_price DOUBLE COMMENT '原实际成交价（元）',
  n_trading_price DOUBLE COMMENT '新合同成交价（元）',
n_actual_trading_price DOUBLE COMMENT '新实际成交价（元）',
  remark STRING COMMENT '备注',
  otherremark STRING COMMENT '其它备注'

  ) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_data_update_deal_info
select a.id
,a.apply_no
,a.house_no
,a.product_id
,a.customer_name
,a.sales_user_name
,a.update_module
,a.deal_info
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
,cast (get_json_object(concat('[', deal_info_json, ']'),'$.[0].o.trading_price') as double)
,cast (get_json_object(concat('[', deal_info_json, ']'),'$.[0].o.actual_trading_price') as double)
,cast (get_json_object(concat('[', deal_info_json, ']'),'$.[0].n.trading_price') as double)
,cast (get_json_object(concat('[', deal_info_json, ']'),'$.[0].n.actual_trading_price') as double)
,cast (get_json_object(concat('[', deal_info_json, ']'),'$.[0].remark') as String)
,cast (get_json_object(concat('[', deal_info_json, ']'),'$.[0].otherRemark') as String)

from ods_bpms_biz_data_update_common a where instr(lower(update_module),'dealinfo')>0;

drop table if exists ods_bpms_biz_data_update_deal_info;
ALTER TABLE odstmp_bpms_biz_data_update_deal_info RENAME TO ods_bpms_biz_data_update_deal_info;