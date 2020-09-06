use ods;
drop table if  exists ods.odstmp_bpms_biz_data_update_house_info;
CREATE TABLE if not exists ods.odstmp_bpms_biz_data_update_house_info (
  id STRING COMMENT 'id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号',
  product_id STRING COMMENT '产品id',
  customer_name STRING COMMENT '客户姓名',
  sales_user_name STRING COMMENT '渠道经理姓名',
  update_module STRING COMMENT '修改模块',
  house_info STRING COMMENT '客户信息',
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
  o_house_cert_no STRING COMMENT '原房产编号',
o_house_area STRING COMMENT '原房产所在区域',
o_house_acreage DOUBLE COMMENT '原房屋面积（㎡）',
o_house_address STRING COMMENT '原房产地址（坐落）',
o_house_type STRING COMMENT '原房产用途',
o_property_owner_info STRING COMMENT '原产权信息',
o_cert_type STRING COMMENT '原房产证类型',
n_house_cert_no STRING COMMENT '新房产编号',
n_house_area STRING COMMENT '新房产所在区域',
n_house_acreage DOUBLE COMMENT '新房屋面积（㎡）',
n_house_address STRING COMMENT '新房产地址（坐落）',
n_house_type STRING COMMENT '新房产用途',
n_property_owner_info STRING COMMENT '新产权信息',
n_cert_type STRING COMMENT '新房产证类型',
  remark STRING COMMENT '备注',
  otherremark STRING COMMENT '其它备注'
  ) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_data_update_house_info
select a.id
,a.apply_no
,a.house_no
,a.product_id
,a.customer_name
,a.sales_user_name
,a.update_module
,a.house_info
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
,get_json_object(concat('[', house_info_json, ']'),'$.[0].o.house_cert_no')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].o.house_area')
,cast (get_json_object(concat('[', house_info_json, ']'),'$.[0].o.house_acreage') as double)
,get_json_object(concat('[', house_info_json, ']'),'$.[0].o.house_address')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].o.house_type')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].o.property_owner_info')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].o.cert_type')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].n.house_cert_no')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].n.house_area')
,cast (get_json_object(concat('[', house_info_json, ']'),'$.[0].n.house_acreage') as double)
,get_json_object(concat('[', house_info_json, ']'),'$.[0].n.house_address')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].n.house_type')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].n.property_owner_info')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].n.cert_type')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].remark')
,get_json_object(concat('[', house_info_json, ']'),'$.[0].otherRemark')
from ods_bpms_biz_data_update_common a where instr(lower(update_module),'houseinfo')>0;
drop table if exists ods_bpms_biz_data_update_house_info;
ALTER TABLE odstmp_bpms_biz_data_update_house_info RENAME TO ods_bpms_biz_data_update_house_info;