drop table  if exists dim.dim_order_fr_rate;
CREATE TABLE dim.dim_order_fr_rate (
  id string,
  jms_name STRING COMMENT '加盟公司',
  product_type STRING COMMENT '产品类型',
  start_date timestamp COMMENT '开始时间（财务归档完成）',
  end_date timestamp COMMENT '结束时间（财务归档完成）',
  dd_rate DOUBLE COMMENT '大道分成比例',
  jms_rate DOUBLE COMMENT '加盟商分成比例',
  create_user_id	STRING	COMMENT	'创建人',
  create_time	TIMESTAMP	COMMENT	'创建时间',
  update_user_id	STRING	COMMENT	'更新人',
  update_time	TIMESTAMP	COMMENT	'更新时间',
  sortnum BIGINT	COMMENT	'排序',
  etl_update_time TIMESTAMP COMMENT '更新时间'
) STORED AS PARQUET;
insert overwrite table dim.dim_order_fr_rate
select
id
,branch_name '加盟公司'
,product_type '产品类型'
,loan_start_time '开始时间（财务归档完成）'
,loan_end_time '结束时间（财务归档完成）'
,avenue_proportion '大道分成比例'
,franchisee_proportion '加盟商分成比例'
,create_user_id
,create_time
,update_user_id
,update_time
,`sort`
,'${hivevar:logdate}'
from ods.ods_bpms_biz_share_proportion;