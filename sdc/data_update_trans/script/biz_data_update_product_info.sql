use ods;
drop table if  exists ods.odstmp_bpms_biz_data_update_product_info;
CREATE TABLE if not exists ods.odstmp_bpms_biz_data_update_product_info (
  id STRING COMMENT 'id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号',
  product_id STRING COMMENT '产品id',
  customer_name STRING COMMENT '客户姓名',
  sales_user_name STRING COMMENT '渠道经理姓名',
  update_module STRING COMMENT '修改模块',
  product_info STRING COMMENT '客户信息',
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
o_tailReleaseNode STRING COMMENT '原放款节点 ',
o_partnerInsuranceName STRING COMMENT '原合作机构',
o_refundSource STRING COMMENT '原回款来源',
o_borrowTerm DOUBLE COMMENT '原借款期限',
o_borrowingAmount DOUBLE COMMENT '原借款金额（元）',
o_expectCost DOUBLE COMMENT '原预估用款期限',
o_ownFundAmount DOUBLE COMMENT '原卖方自有资金（元）',
o_partnerBankName STRING COMMENT '原合作银行',
o_productTerm DOUBLE COMMENT '原产品期限',
o_ransomBorrowAmount DOUBLE COMMENT '原赎楼金额（元）',
o_mostPermission DOUBLE COMMENT '原（申请）保险金额',
o_tailAccountType STRING COMMENT '原尾款收款账户类型',
  n_tailReleaseNode STRING COMMENT '新放款节点 ',
n_partnerInsuranceName STRING COMMENT '新合作机构',
n_refundSource STRING COMMENT '新回款来源',
n_borrowTerm DOUBLE COMMENT '新借款期限',
n_borrowingAmount DOUBLE COMMENT '新借款金额（元）',
n_expectCost DOUBLE COMMENT '新预估用款期限',
n_ownFundAmount DOUBLE COMMENT '新卖方自有资金（元）',
n_partnerBankName STRING COMMENT '新合作银行',
n_productTerm DOUBLE COMMENT '新产品期限',
n_ransomBorrowAmount DOUBLE COMMENT '新赎楼金额（元）',
n_mostPermission DOUBLE COMMENT '新（申请）保险金额',
n_tailAccountType STRING COMMENT '新尾款收款账户类型',
  remark STRING COMMENT '备注',
  otherremark STRING COMMENT '其它备注'

  ) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_data_update_product_info
select a.id
,a.apply_no
,a.house_no
,a.product_id
,a.customer_name
,a.sales_user_name
,a.update_module
,a.product_info
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
,get_json_object(concat('[', product_info_json, ']'),'$.[0].o.tailReleaseNode')
,get_json_object(concat('[', product_info_json, ']'),'$.[0].o.partnerInsuranceName')
,get_json_object(concat('[', product_info_json, ']'),'$.[0].o.refundSource')
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].o.borrowTerm') as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].o.borrowingAmount') as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].o.expectCost') as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].o.ownFundAmount') as double)
,get_json_object(concat('[', product_info_json, ']'),'$.[0].o.partnerBankName')
, cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].o.productTerm')  as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].o.ransomBorrowAmount')  as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].o.mostPermission') as double)
,get_json_object(concat('[', product_info_json, ']'),'$.[0].o.tailAccountType')
,get_json_object(concat('[', product_info_json, ']'),'$.[0].n.tailReleaseNode')
,get_json_object(concat('[', product_info_json, ']'),'$.[0].n.partnerInsuranceName')
,get_json_object(concat('[', product_info_json, ']'),'$.[0].n.refundSource')
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].n.borrowTerm') as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].n.borrowingAmount')as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].n.expectCost')as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].n.ownFundAmount')as double)
,get_json_object(concat('[', product_info_json, ']'),'$.[0].n.partnerBankName')
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].n.productTerm') as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].n.ransomBorrowAmount') as double)
,cast (get_json_object(concat('[', product_info_json, ']'),'$.[0].n.mostPermission') as double)
,get_json_object(concat('[', product_info_json, ']'),'$.[0].n.tailAccountType')
,get_json_object(concat('[', product_info_json, ']'),'$.[0].remark')
,get_json_object(concat('[', product_info_json, ']'),'$.[0].otherRemark')
from ods_bpms_biz_data_update_common a where instr(lower(update_module),'productinfo')>0;
drop table if exists ods_bpms_biz_data_update_product_info;
ALTER TABLE odstmp_bpms_biz_data_update_product_info RENAME TO ods_bpms_biz_data_update_product_info;