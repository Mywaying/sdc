use ods;
drop table if  exists ods.odstmp_bpms_biz_data_update_fee_info;
create table if not exists ods.odstmp_bpms_biz_data_update_fee_info (
  id string comment 'id',
  apply_no string comment '订单编号',
  house_no string comment '房产信息编号',
  product_id string comment '产品id',
  customer_name string comment '客户姓名',
  sales_user_name string comment '渠道经理姓名',
  update_module string comment '修改模块',
  fee_info string comment '客户信息',
  authority string comment '修改部分的权限',
  update_user_id string comment '更新人id',
  update_time timestamp comment '记录更新时间',
  `datetime` timestamp,
  create_user_id string comment '创建人id',
  rev bigint comment '版本号',
  delete_flag string comment '删除记录标识',
  create_time timestamp comment '记录创建时间',
  company_code string comment '分公司编号',
  flow_instance_id string comment '流程实例id',
  datau_time timestamp comment '数据更新成功时间',
o_borrowingvaluedate string comment '原借款起息日（客户）',
o_borrowingduedate string comment '原实际借款回款日（客户）-新增字段',
o_countheadtail string comment '原算头算尾方式-新增字段',
o_platformvaluedate string comment '原平台起息日：',
o_producttermandchargeway string comment '原收费方式选择',
o_hasbillflag string comment '原是否有专票',
o_tag string comment '原渠道标签（加盟商不显示）',
o_channelname string comment '原渠道名称（加盟商不显示）',
o_pricetag string comment '原价格标签（加盟商不显示，自营显示不可修改）',
o_pricediscount string comment '原定价折扣（加盟商不显示，自营显示不可修改）',
o_risklevel string comment '原风险等级',
o_channelpriceperorder double comment '原渠道价%/笔（%）',
o_agentcommissionperorder double comment '原代收返佣%/笔（%）',
o_overduerateperday double comment '原超期费率%/天（%）',
o_channelpriceperorderfirstpart double comment '原首段期限渠道价/笔(%)',
o_channelpriceperorderperpart double comment '原其他时间段渠道价/段(%)',

n_borrowingvaluedate string comment '新借款起息日（客户）',
n_borrowingduedate string comment '新实际借款回款日（客户）-新增字段',
n_countheadtail string comment '新算头算尾方式-新增字段',
n_platformvaluedate string comment '新平台起息日',
n_producttermandchargeway string comment '新收费方式选择',
n_hasbillflag string comment '新是否有专票',
n_tag string comment '新渠道标签（加盟商不显示）',
n_channelname string comment '新渠道名称（加盟商不显示）',
n_pricetag string comment '新价格标签（加盟商不显示，自营显示不可修改）',
n_pricediscount string comment '新定价折扣（加盟商不显示，自营显示不可修改）',
n_risklevel string comment '新风险等级',
n_channelpriceperorder double comment '新渠道价%/笔（%）',
n_agentcommissionperorder double comment '新代收返佣%/笔（%）',
n_overduerateperday double comment '新超期费率%/天（%）',
n_channelpriceperorderfirstpart double comment '新首段期限渠道价/笔(%)',
n_channelpriceperorderperpart double comment '新其他时间段渠道价/段(%)',
  remark STRING COMMENT '备注',
  otherremark STRING COMMENT '其它备注'

  ) stored as parquet;
insert overwrite table odstmp_bpms_biz_data_update_fee_info
select a.id
,a.apply_no
,a.house_no
,a.product_id
,a.customer_name
,a.sales_user_name
,a.update_module
,a.fee_info
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
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.borrowingValueDate')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.borrowingDueDate')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.countHeadTail')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.platformValueDate')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.productTermAndChargeWay')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.fillInFeeRateList[].hasBillFlag')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.tag')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.channelName')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.priceTag')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.priceDiscount')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.riskLevel')
,cast(get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.fillInFeeRateList[0].channelPricePerOrder') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.fillInFeeRateList[0].agentCommissionPerOrder')  as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.fillInFeeRateList[0].overdueRatePerDay') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.fillInFeeRateList[0].channelPricePerOrderFirstPart') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].o.fillInFeeRateList[0].channelPricePerOrderPerPart') as double)
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.borrowingValueDate')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.borrowingDueDate')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.countHeadTail')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.platformValueDate')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.productTermAndChargeWay')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.fillInFeeRateList[].hasBillFlag')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.tag')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.channelName')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.priceTag')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.priceDiscount')
,get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.riskLevel')
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.fillInFeeRateList[0].channelPricePerOrder') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.fillInFeeRateList[0].agentCommissionPerOrder') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.fillInFeeRateList[0].overdueRatePerDay') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.fillInFeeRateList[0].channelPricePerOrderFirstPart') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].n.fillInFeeRateList[0].channelPricePerOrderPerPart') as double)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].remark') as String)
,cast (get_json_object(concat('[', fee_info_json, ']'),'$.[0].otherRemark') as String)
from ods_bpms_biz_data_update_common a where instr(lower(update_module),'feeinfo')>0;
drop table if exists ods_bpms_biz_data_update_fee_info;
ALTER TABLE odstmp_bpms_biz_data_update_fee_info RENAME TO ods_bpms_biz_data_update_fee_info;