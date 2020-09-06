use ods;
drop table if  exists ods.odstmp_bpms_biz_apply_order_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_apply_order_common (
  id STRING COMMENT '主键id',
  apply_no STRING COMMENT '业务申请编号',
  house_no STRING COMMENT '房产信息编号, 关联房产信息',
  product_id STRING COMMENT '产品ID',
  product_name STRING COMMENT '产品名字',
  service_type STRING COMMENT '业务类型（加盟商业务：JMS）',
  partner_bank_id STRING COMMENT '合作银行id',
  partner_bank_name STRING COMMENT '合作银行名称',
  partner_insurance_id STRING COMMENT '合作保险公司id',
  partner_insurance_name STRING COMMENT '合作保险公司名称',
  city_no STRING COMMENT '城市编码',
  channel STRING COMMENT '录单渠道',
  branch_id STRING COMMENT '大道分公司编号',
  sales_branch_id STRING COMMENT '市场部机构编号',
  sales_user_id STRING COMMENT '渠道经理用户ID',
  sales_user_name STRING COMMENT '渠道经理姓名',
  apply_time timestamp COMMENT '业务订单申请时间',
  flow_instance_id STRING COMMENT '流程实例id',
  apply_status STRING COMMENT '订单状态',
  create_user_id STRING COMMENT '创建人id',
  update_user_id STRING COMMENT '更新人id',
  create_time timestamp COMMENT '记录创建时间',
  update_time timestamp COMMENT '记录更新时间',
  rev bigint comment '版本号',
  delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
  seller_name STRING COMMENT '卖家姓名',
  seller_phone STRING COMMENT '卖家电话',
  seller_card_no STRING COMMENT '卖方身份证号',
  seller_card_type STRING COMMENT '证件类型',
  buyer_name STRING COMMENT '买家姓名',
  buyer_phone STRING COMMENT '买家电话',
  remark STRING COMMENT '备注',
  version STRING COMMENT '流程版本号',
  keywords_ STRING COMMENT '订单关键字,通过分类,第一个是订单的所有客户姓名',
  thirdparty_name STRING COMMENT '第三方公司名字',
  thirdparty_no STRING COMMENT '第三方申请编号',
  preliminary_result STRING COMMENT '预审结果',
  follow_up_userid STRING COMMENT '跟进人员ID',
  follow_up_username STRING COMMENT '跟进人员名称',
  relate_type STRING COMMENT '订单关联生成关系',
  relate_apply_no STRING COMMENT '关联的订单号',
  group_apply_no STRING COMMENT '关联在一起的订单同属一个分组',
  man_check_first STRING COMMENT '是否先审批（Y：先审批后面签，N：先面签后审批）',
  project_tag STRING COMMENT '项目标识',
  rob_user_id STRING COMMENT '抢单用户id',
  rob_time timestamp COMMENT '抢单时间',
  rob_user_name STRING COMMENT '抢单用户名称',
  matter_version bigint comment '事项的流程版本',
  green_channel STRING COMMENT '绿色通道 (0 不是，1 是)',
  matter_sort bigint comment '事项排序字段',
  query_part_type STRING COMMENT '查询阶段类型 0 待面签、1 待放款、2 已放款',
  proc_def_key_ STRING,
  product_version STRING,
  product_type STRING,
  transaction_type STRING COMMENT '交易类型',
  relate_type_new STRING,
  relate_type_name STRING,
  product_term_and_charge_way STRING,
  city_name STRING COMMENT '城市名称',
  branch_name STRING COMMENT '附属公司',
  apply_status_name STRING,
  isjms STRING comment '是否加盟',
  release_amount_xg double comment '放款金额_销管',
  release_amount double comment '放款金额',
  seller_name_new STRING COMMENT '卖方/借款人姓名',
  seller_id_card_no_new STRING COMMENT '卖方/借款人证件号码',
  seller_phone_new STRING COMMENT '卖方/借款人联系方式',
  seller_income_type_name_new STRING COMMENT '卖方/借款人收入类型',
  seller_employer_new STRING COMMENT '卖方/借款人工作单位',
  seller_marital_status_name_new STRING COMMENT '卖方/借款人婚姻状况',
  omate_name STRING COMMENT '配偶姓名',
  id_card_no STRING COMMENT '配偶证件号码',
  buy_name_new STRING COMMENT '买方姓名',
  buy_id_card_no_new STRING COMMENT '买方证件号码',
  buy_phone_new STRING COMMENT '买方联系方式',
  buy_income_type_name_new STRING COMMENT '买方收入类型',
  buy_employer_new STRING COMMENT '买方工作单位',
  sales_branch_name STRING COMMENT '市场部机构',
  extra_group_apply_no STRING  COMMENT '联*标签的主订单',
  is_interview_name STRING  COMMENT '是否需要面签 0 不需要 1 需要'
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_apply_order_common
SELECT
	bao.`id`,
bao.`apply_no`,
bao.`house_no`,
bao.`product_id`,
bao.`product_name`,
bao.`service_type`,
bao.`partner_bank_id`,
bao.`partner_bank_name`,
bao.`partner_insurance_id`,
bao.`partner_insurance_name`,
bao.`city_no`,
bao.`channel`,
bao.`branch_id`,
bao.`sales_branch_id`,
bao.`sales_user_id`,
bao.`sales_user_name`,
bao.`apply_time`,
bao.`flow_instance_id`,
bao.`apply_status`,
bao.`create_user_id`,
bao.`update_user_id`,
bao.`create_time`,
bao.`update_time`,
bao.`rev`,
bao.`delete_flag`,
bao.`seller_name`,
bao.`seller_phone`,
bao.`seller_card_no`,
bao.`seller_card_type`,
bao.`buyer_name`,
bao.`buyer_phone`,
bao.`remark`,
bao.`version`,
bao.`keywords_`,
bao.`thirdparty_name`,
bao.`thirdparty_no`,
bao.`preliminary_result`,
bao.`follow_up_userid`,
bao.`follow_up_username`,
bao.`relate_type`,
bao.`relate_apply_no`,
bao.`group_apply_no`,
bao.`man_check_first`,
bao.`project_tag`,
bao.`rob_user_id`,
bao.`rob_time`,
bao.`rob_user_name`,
bao.`matter_version`,
bao.`green_channel`,
bao.`matter_sort`,
bao.`query_part_type`,
	t2.PROC_DEF_KEY_,
	case
      when t2.PROC_DEF_KEY_ IN (
        'bizApply_cash',
        'bizApply_all'
        ) then 'v1.0'
      when t2.PROC_DEF_KEY_ IN (
          'bizApply_all_cash',
          'bizApply_all_insure',
          'bizApply_zztfb'
        ) then 'v1.5'
      when t2.PROC_DEF_KEY_ IN (
        'bizApplyFlowCash_v2',
        'bizApplyFlowIns_v2',
        'bizApply_mortgage',
        'bizApplyTransition',
        'bizApply_mfb', 'bizApplyFlowInsurance_v2'
        ) then 'v2.0'
      when t2.PROC_DEF_KEY_ IN (
        'bizApplyFlowCash_v2_5', 'bizApply_guaranty',
        'bizApplyFlowIns_v2_5', 'bizApplyFlowIns_mfb',
    'bizApplyFlowIns_zyb'
        ) then 'v2.5'

      when product_id = "NSL-JYB371" then "v1.0"
      when product_id = "NSL-JYB377" then "v1.0"
      when product_id = "NSL-JYB755" then "v1.0"
      when product_id = "NSL-TFB371" then "v1.0"
      when product_id = "SL-TFB371" then "v1.0"
      when product_id = "SL-JYB371" then "v1.5"
      when product_id = "SL-JYB374" then "v1.5"

      else 'v2.5'
  end product_version,
CASE
      WHEN (
        product_id LIKE '%SLY%'
        OR (product_id LIKE '%JSD%' and product_id <>'JSD_NSL_NJY_SER')
      )
      THEN '现金类产品'
      WHEN (
        product_id LIKE '%JYB%'
        OR product_id LIKE '%TFB%'
        or product_id like "%MFB%"
        or product_id like "%ZYB%"
  			 or product_id like "%PMB_YJY_ISR%"
      )
      THEN '保险类产品'
      WHEN (
      product_id LIKE '%AJFW%'
      ) then '按揭服务'
 ELSE '其他'
 END product_type,
case when (bao.product_name like '%及时贷（交易%'
      or bao.product_name like '%交易保%'
      or bao.product_name like '%买付保%'
      or bao.product_name like '%拍卖保%'
      ) then '交易'
    when (bao.product_name like '%及时贷（非交易%'
      or bao.product_name like '%提放保%'
      ) then '非交易'
    else '其他'
 end transaction_type,
nvl(tmp1.relate_type,bao.relate_type) relate_type_new, -- '关联生成关系'
nvl(tmp1.relate_type_name,
case when bao.relate_type = "MAIN" then ''
     when bao.relate_type = "BDCF" then "标的拆分"
     when bao.relate_type = "DQGHPT" then "到期更换平台"
     when bao.relate_type = "PTGL" then "普通关联"
     when bao.relate_type = "CPBG" then "产品变更"
end) relate_type_name,
CASE
	WHEN t3.`product_term_and_charge_way` = 'Y' OR t3.`product_term_and_charge_way` = 'calculateDaily' THEN '按天计息'
	WHEN t3.`product_term_and_charge_way` = 'N' OR t3.`product_term_and_charge_way` = 'fixedTerm' THEN '固定期限'
	WHEN t3.`product_term_and_charge_way` = 'piecewiseCalculate' THEN '区间计息'
ELSE
	NULL
END product_term_and_charge_way,
dca.company_name_2_level city_name,
dca.company_name_3_level branch_name,
nvl(sd1.`NAME_`,bao.apply_status) apply_status_name,
regexp_replace(regexp_replace(regexp_replace(service_type,'','否'),'JMX','是'),'否J否M否S否','是')
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,NULL
,extra_group_apply_no
,case when bao.is_interview = "1" then '是' else '否' end
FROM
	(select * from ods_bpms_biz_apply_order where apply_status is not null and apply_status!='')  bao
left join (select bpmr.follow_applyorder apply_no,sd.`key_` relate_type ,sd.`NAME_` relate_type_name from ods_bpms_biz_product_mate_relation bpmr
left join ods_bpms_biz_product_rel_rule bprr on bpmr.product_rel_id = bprr.id
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bpms_sys_dic_common where TYPE_ID_='10000080670030' or TYPE_ID_='10000010830911' or TYPE_ID_='10000057530172' or TYPE_ID_='10000034210010' or TYPE_ID_='10000025010002' or  TYPE_ID_='10000001220223' or TYPE_ID_='10000034210010' or TYPE_ID_='1000000122021' or TYPE_ID_='10000033770002' ) sd on bprr.rule_type = sd.KEY_ ) tmp1 on tmp1.apply_no=bao.apply_no
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bpms_sys_dic_common where TYPE_ID_='10000080670030' or TYPE_ID_='10000010830911' or TYPE_ID_='10000057530172' or TYPE_ID_='10000034210010' or TYPE_ID_='10000025010002' or  TYPE_ID_='10000001220223' or TYPE_ID_='10000034210010' or TYPE_ID_='1000000122021' or TYPE_ID_='10000033770002' ) sd1 on sd1.KEY_= lower(bao.apply_status)
LEFT JOIN ods_bpms_bpm_pro_inst t2 ON bao.flow_instance_id = t2.ID_
LEFT JOIN ods_bpms_biz_fee_summary t3 ON t3.apply_no = bao.apply_no
left join dim.dim_company dca on bao.branch_id = dca.company_id_3_level;

drop table if exists odstmp_bpms_biz_apply_order_common_ex;
create table odstmp_bpms_biz_apply_order_common_ex like odstmp_bpms_biz_apply_order_common;
with tmp_seel as (
select a.apply_no,
group_concat(a.name) seller_name, -- 卖方：SEL
group_concat(a.phone) seller_phone, --手机
group_concat(a.id_card_no) seller_id_card_no, -- 身份证
group_concat(employer) employer, --工作单位
group_concat(income_type_name) income_type_name, -- 收入类型
group_concat(marital_status_tag) marital_status_name -- 婚姻状态
from (select * from ods_bpms_biz_customer_rel_common order by is_actual_borrower_name desc) a where  a.`role`='OWN' and (a.relation_name='产权人' or a.relation_name='产权人配偶'
or a.relation_name='新贷款借款人' or a.relation_name='卖方' or (a.is_actual_borrower_name='Y' and a.relation_name='原贷款借款人'))
group by a.apply_no
),
tmp_buy as (
select a.apply_no,
group_concat(a.name) buy_name, -- 买方：BUY
group_concat(a.phone) buy_phone,
group_concat(a.id_card_no) buy_id_card_no,
group_concat(a.income_type_name) income_type_name,  -- 收入类型
group_concat(employer) employer, -- 工作单位
group_concat(marital_status_tag) marital_status_name  -- 婚姻状态
from ods_bpms_biz_customer_rel_common a where  a.`role`='BUY' and a.`relation`='BUY'  group by a.apply_no
),
tmp_custormer as (
select a.apply_no,
group_concat(a.name) omate_name, -- 卖方配偶姓名
group_concat(a.phone) omate_phone,
group_concat(a.id_card_no) omate_id_card_no, -- 配偶证件号码
group_concat(a.income_type_name) omate_income_type_name,
group_concat( employer) omate_employer,
group_concat(marital_status_tag) omate_marital_status_name
from ods_bpms_biz_customer_rel_common a where a.relation_name like '%卖方配偶%'
group by a.apply_no
)
insert overwrite table odstmp_bpms_biz_apply_order_common_ex
select
  bao.id,
bao.apply_no,
bao.house_no,
bao.product_id,
bao.product_name,
bao.service_type,
bao.partner_bank_id,
bao.partner_bank_name,
bao.partner_insurance_id,
bao.partner_insurance_name,
bao.city_no,
bao.channel,
bao.branch_id,
bao.sales_branch_id,
bao.sales_user_id,
bao.sales_user_name,
bao.apply_time,
bao.flow_instance_id,
bao.apply_status,
bao.create_user_id,
bao.update_user_id,
bao.create_time,
bao.update_time,
bao.rev,
bao.delete_flag,
bao.seller_name,
bao.seller_phone,
bao.seller_card_no,
bao.seller_card_type,
bao.buyer_name,
bao.buyer_phone,
bao.remark,
bao.version,
bao.keywords_,
bao.thirdparty_name,
bao.thirdparty_no,
bao.preliminary_result,
bao.follow_up_userid,
bao.follow_up_username,
bao.relate_type,
bao.relate_apply_no,
bao.group_apply_no,
bao.man_check_first,
bao.project_tag,
bao.rob_user_id,
bao.rob_time,
bao.rob_user_name,
bao.matter_version,
bao.green_channel,
bao.matter_sort,
bao.query_part_type,
bao.proc_def_key_,
bao.product_version,
bao.product_type,
bao.transaction_type,
bao.relate_type_new,
bao.relate_type_name,
bao.product_term_and_charge_way,
bao.city_name,
bao.branch_name,
bao.apply_status_name,
bao.isjms
,(case
   when bao.product_type='现金类产品' then cast (cfm.con_funds_cost as double)
   when bao.product_name like'买付保%' then cast (bfs.guarantee_amount as double)
   when bao.product_name='大道按揭' then 0
   else nvl(bnl.biz_loan_amount, 0)
 end ) release_amount_xg  -- 放款金额_销管
,(case
   when bao.product_type='现金类产品' then cast (cfm.con_funds_cost as double)
   when bao.product_name like'买付保%' then cast (bfs.guarantee_amount as double)
   else nvl(bnl.biz_loan_amount, 0)
 end) release_amount  -- 放款金额
 ,tseel.seller_name `seller_name`   --  卖方/借款人姓名
 ,tseel.seller_id_card_no `seller_id_card_no`  --  卖方/借款人证件号码
 ,tseel.seller_phone `seller_phone`      --  卖方/借款人联系方式
 ,tseel.income_type_name `income_type_name` --  卖方/收入类型
 ,tseel.employer `employer` -- 工作单位
 ,tseel.marital_status_name-- 婚姻状况
 ,bcr.omate_name `omate_name` -- 配偶姓名
 ,bcr.omate_id_card_no -- 配偶证件号码
 ,tbuy.buy_name --  买方姓名
 ,tbuy.buy_id_card_no  --  买方证件号码
 ,tbuy.buy_phone --  买方联系方式
 ,tbuy.income_type_name buy_income_type_name --  买方/收入类型
 ,tbuy.employer buy_employer --  买方/收入类型
 ,sog.name_ sales_org_name -- 市场部
,bao.extra_group_apply_no
,bao.is_interview_name
from odstmp_bpms_biz_apply_order_common bao
left join ods_bpms_c_fund_module_common cfm on cfm.apply_no=bao.apply_no
LEFT JOIN ods_bpms_biz_fee_summary bfs on bfs.apply_no=bao.apply_no
LEFT JOIN (select * from ods_bpms_biz_new_loan_common where rn=1) bnl on bnl.apply_no=bao.apply_no
left join tmp_seel tseel on tseel.apply_no=bao.apply_no
left join tmp_buy tbuy on tbuy.apply_no=bao.apply_no
left join tmp_custormer bcr on bcr.apply_no=bao.apply_no
left join  ods_bpms_sys_org sog on sog.CODE_=bao.sales_branch_id;
drop table if exists ods_bpms_biz_apply_order_common;
ALTER TABLE odstmp_bpms_biz_apply_order_common_ex RENAME TO ods_bpms_biz_apply_order_common;