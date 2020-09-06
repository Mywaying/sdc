use ods;
drop table if  exists ods.odstmp_bpms_biz_data_update_customer_info;
CREATE TABLE if not exists ods.odstmp_bpms_biz_data_update_customer_info (
  id STRING COMMENT 'id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号',
  product_id STRING COMMENT '产品id',
  customer_name STRING COMMENT '客户姓名',
  sales_user_name STRING COMMENT '渠道经理姓名',
  update_module STRING COMMENT '修改模块',
  customer_info STRING COMMENT '客户信息',
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
  o_relation STRING COMMENT '原关系类型',
  o_isProposer STRING COMMENT '原是否为申请人',
  o_isActualBorrowerName STRING COMMENT '原是否为主借款人/投保人',
  o_name STRING COMMENT '原姓名',
  o_id_card_type STRING COMMENT '原证件类型',
  o_id_card_no STRING COMMENT '原证件号码',
  o_phone STRING COMMENT '原客户联系方式',
  o_address STRING COMMENT '原家庭住址',
  o_age STRING COMMENT '原年龄',
  o_id_card_validity STRING COMMENT '原证件有效期',
  o_marital_status STRING COMMENT '原婚姻状态',
  o_education STRING COMMENT '原教育程度',
  o_monthly_income DOUBLE COMMENT '原个人月收入',
  o_income_type STRING COMMENT '原收入类型',
  o_has_children STRING COMMENT '原有无子女',
  o_employer STRING COMMENT '原工作单位',
  o_email STRING COMMENT '原电子邮箱',
  n_relation STRING COMMENT '新关系类型',
  n_isProposer STRING COMMENT '新是否为申请人',
  n_isActualBorrowerName STRING COMMENT '新是否为主借款人/投保人',
  n_name STRING COMMENT '新姓名',
  n_id_card_type STRING COMMENT '新证件类型',
  n_id_card_no STRING COMMENT '新证件号码',
  n_phone STRING COMMENT '新客户联系方式',
  n_address STRING COMMENT '新家庭住址',
  n_age STRING COMMENT '新年龄',
  n_id_card_validity STRING COMMENT '新证件有效期',
  n_marital_status STRING COMMENT '新婚姻状态',
  n_education STRING COMMENT '新教育程度',
  n_monthly_income DOUBLE COMMENT '新个人月收入',
  n_income_type STRING COMMENT '新收入类型',
  n_has_children STRING COMMENT '新有无子女',
  n_employer STRING COMMENT '新工作单位',
  n_email STRING COMMENT '新电子邮箱',
  remark STRING COMMENT '备注',
  otherremark STRING COMMENT '其它备注'

  ) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_data_update_customer_info
select a.id
,a.apply_no
,a.house_no
,a.product_id
,a.customer_name
,a.sales_user_name
,a.update_module
,a.customer_info
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
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.relation')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.isProposer')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.isActualBorrowerName')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.name')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.id_card_type')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.id_card_no')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.phone')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.address')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.age')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.id_card_validity')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.marital_status')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.education')
,cast (get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.monthly_income') as double)
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.income_type')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.has_children')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.employer')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].o.email')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.relation')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.isProposer')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.isActualBorrowerName')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.name')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.id_card_type')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.id_card_no')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.phone')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.address')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.age')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.id_card_validity')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.marital_status')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.education')
,cast (get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.monthly_income') as double)
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.income_type')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.has_children')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.employer')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].n.email')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].remark')
,get_json_object(concat('[', customer_info_json, ']'),'$.[0].otherRemark')
from ods_bpms_biz_data_update_common a where instr(lower(update_module),'customerinfo')>0;

drop table if exists ods_bpms_biz_data_update_customer_info;
ALTER TABLE odstmp_bpms_biz_data_update_customer_info RENAME TO ods_bpms_biz_data_update_customer_info;