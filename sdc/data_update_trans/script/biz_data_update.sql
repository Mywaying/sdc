use ods;
drop table if  exists ods.odstmp_bpms_biz_data_update_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_data_update_common (
  id STRING COMMENT 'id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号',
  product_id STRING COMMENT '产品id',
  customer_name STRING COMMENT '客户姓名',
  sales_user_name STRING COMMENT '渠道经理姓名',
  update_module STRING COMMENT '修改模块',
  customer_info STRING COMMENT '客户信息',
  house_info STRING COMMENT '房产信息',
  deal_info STRING COMMENT '交易信息',
  ori_loan_info STRING COMMENT '原贷款信息',
  new_loan_info STRING COMMENT '新贷款信息',
  product_info STRING COMMENT '产品信息',
  account_info STRING COMMENT '账户信息',
  fee_info STRING,
  other_info STRING COMMENT '其他信息',
  sales_user_info STRING COMMENT '渠道经理模块',
  tech_info STRING COMMENT '科技修改信息',
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
  osalesuserid STRING COMMENT '旧渠道经理id',
  osalesusername STRING COMMENT '旧渠道经理名',
  nsalesuserid STRING COMMENT '新渠道经理id',
  nsalesusername STRING COMMENT '新渠道经理名',
  datau_time timestamp COMMENT '数据更新成功时间',
  customer_info_json STRING COMMENT '客户信息',
  house_info_json STRING COMMENT '房产信息',
  deal_info_json STRING COMMENT '交易信息',
  ori_loan_info_json STRING COMMENT '原贷款信息',
  new_loan_info_json STRING COMMENT '新贷款信息',
  product_info_json STRING COMMENT '产品信息',
  account_info_json STRING COMMENT '账户信息',
  fee_info_json STRING COMMENT '费用信息',
  other_info_json STRING COMMENT '其他信息',
  sales_user_info_json STRING COMMENT '渠道经理模块',
  tech_info_json STRING COMMENT '科技修改信息',
  create_user_name STRING COMMENT '创建人',
  flow_status STRING COMMENT '流程状态',
  flow_status_name STRING COMMENT '流程状态名',
  rn bigint
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_data_update_common
select T.*
,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sales_user_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.o.salesUserId')
,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sales_user_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.o.salesUserName')
,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sales_user_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.n.salesUserid')
,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sales_user_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.n.salesUserName')
,b.CREATE_TIME_ datau_time
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(customer_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(house_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(deal_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(ori_loan_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(new_loan_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(product_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(account_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(fee_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(other_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(sales_user_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$','')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(tech_info,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'^"\\[',''),'\\]"$',''),'^\\[',''),'\\]$',''),sys.`fullname_`
,b.status_
,case when b.status_ = "end" then '结束'
     when b.status_ = "manualend" then '人工结束'
     when b.status_ = "draft" then '草稿'
     when b.status_ = "revoke" then '驳回'
     when b.status_ = "pigeonhole" then '归档'
     when b.status_ = "back" then '退回'
     when b.status_ = "running" then '运行中'
     when b.status_ = "revoketostart" then '驳回到开始'
end flow_status_name
  ,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from
( SELECT
a.`id`,
a.`apply_no`,
a.`house_no`,
a.`product_id`,
a.`customer_name`,
a.`sales_user_name`,
a.`update_module`,
a.`customer_info`,
a.`house_info`,
a.`deal_info`,
a.`ori_loan_info`,
a.`new_loan_info`,
a.`product_info`,
a.`account_info`,
a.`fee_info`,
a.`other_info`,
a.`sales_user_info`,
a.`tech_info`,
a.`authority`,
a.`update_user_id`,
a.`update_time`,
a.`datetime`,
a.`create_user_id`,
a.`rev`,
a.`delete_flag`,
a.`create_time`,
a.`company_code`,
a.`flow_instance_id`
  FROM
( select * from ods_bpms_biz_data_update where apply_no<>'' and apply_no is not null ) a )T
left join ods_bpms_bpm_pro_inst b on b.ID_=T.flow_instance_id
left join ods_bpms_sys_user sys on sys.id_=T.create_user_id;
drop table if exists ods_bpms_biz_data_update_common;
ALTER TABLE odstmp_bpms_biz_data_update_common RENAME TO ods_bpms_biz_data_update_common;