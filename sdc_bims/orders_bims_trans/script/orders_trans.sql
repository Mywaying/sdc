use ods;
drop table if  exists ods.odstmp_bims_biz_apply_order_common;
CREATE TABLE if not exists ods.odstmp_bims_biz_apply_order_common (
  id STRING COMMENT '主键id',
   apply_no STRING COMMENT '业务申请编号',
   house_no STRING COMMENT '房产信息编号关联房产信息',
   product_id STRING COMMENT '产品ID',
   product_name STRING COMMENT '产品名字',
   product_type STRING COMMENT '产品类型，一押二押',
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
   apply_time TIMESTAMP COMMENT '业务订单申请时间',
   flow_instance_id STRING COMMENT '流程实例id',
   apply_status STRING COMMENT '订单状态',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   update_time TIMESTAMP COMMENT '记录更新时间',
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
   keywords_ STRING COMMENT '订单关键字通过“”分类第一个是订单的所有客户姓名',
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
   rob_time TIMESTAMP COMMENT '抢单时间',
   rob_user_name STRING COMMENT '抢单用户名称',
   matter_version bigint comment '事项的流程版本',
   green_channel STRING COMMENT '绿色通道 (0 不是，1 是)',
   matter_sort bigint comment '事项排序字段',
   query_part_type STRING COMMENT '查询阶段类型 0 待面签、1 待放款、2 已放款',
   blaze_version STRING COMMENT '1表示版本1，2表示版本2',
   batch_no STRING COMMENT '订单标示（大数要求）',
   loan_amount DOUBLE COMMENT '借款金额(取决策输出结果，一直调整，会变)',
   adjustable_loan_amount STRING COMMENT '可调整金额',
   adjustable_proportion_number STRING COMMENT '可调整成数',
   relate_type_name STRING COMMENT '订单关联生成关系名',
   city_name STRING COMMENT '城市名称',
   branch_name STRING COMMENT '附属公司',
   sales_branch_name STRING COMMENT '市场所属公司',
   apply_status_name STRING,
   isjms STRING comment '是否加盟',
   product_type_name STRING COMMENT '产品类型名'
) STORED AS parquet;

insert overwrite table odstmp_bims_biz_apply_order_common
SELECT
	bao.*,
nvl(tmp1.relate_type_name,
case when bao.relate_type = "MAIN" then ''
     when bao.relate_type = "BDCF" then "标的拆分"
     when bao.relate_type = "DQGHPT" then "到期更换平台"
     when bao.relate_type = "PTGL" then "普通关联"
     when bao.relate_type = "PTGL" then "产品变更"
end) relate_type_name,
so.`city_name`,
so.`branch_name`,
som.`sales_branch_name`,
sd1.`NAME_` apply_status_name,
regexp_replace(regexp_replace(regexp_replace(service_type,'','否'),'JMX','是'),'否J否M否S否','是'),
case product_type when '0' then '一押' when '1' then '二押'  end product_type_name
FROM
	(select * from ods_bims_biz_apply_order where apply_status is not null and apply_status!='')  bao
left join (select bpmr.follow_applyorder apply_no,sd.`key_` relate_type ,sd.`NAME_` relate_type_name from ods_bims_biz_product_mate_relation bpmr
left join ods_bims_biz_product_rel_rule bprr on bpmr.product_rel_id = bprr.id
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bims_sys_dic_common where TYPE_ID_='10000034210010' or TYPE_ID_='10000025010002' or  TYPE_ID_='10000001220223' or TYPE_ID_='10000034210010' or TYPE_ID_='1000000122021' or TYPE_ID_='10000033770002' ) sd on bprr.rule_type = sd.KEY_ ) tmp1 on tmp1.apply_no=bao.apply_no
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bims_sys_dic_common where TYPE_ID_='10000033770002' ) sd1 on sd1.KEY_= lower(bao.apply_status)
left JOIN (SELECT a.apply_no,b.NAME_ branch_name,dso.city_name city_name from ods_bims_biz_apply_order a LEFT JOIN ods_bims_sys_org b on b.CODE_=a.branch_id left join dim.dim_sys_org dso on dso.branch_id=b.CODE_) so on  so.apply_no=bao.apply_no
left JOIN (SELECT a.apply_no,b.NAME_ sales_branch_name from ods_bims_biz_apply_order a LEFT JOIN ods_bims_sys_org b on b.CODE_=a.sales_branch_id) som on  som.apply_no=bao.apply_no;
drop table if exists ods_bims_biz_apply_order_common;
ALTER TABLE odstmp_bims_biz_apply_order_common RENAME TO ods_bims_biz_apply_order_common;