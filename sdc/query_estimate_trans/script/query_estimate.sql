use ods;
drop table if exists odstmp_bpms_biz_query_estimate_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_query_estimate_common (
  id STRING COMMENT '主键id',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号关联房产信息',
  cert_no STRING COMMENT '产证编号',
  cert_type STRING COMMENT '产证类型',
  price_source STRING COMMENT '价格来源',
  query_time timestamp COMMENT '查询时间',
  market_average_price DOUBLE COMMENT '市场均价（元）',
  query_user_id STRING COMMENT '查询人id',
  query_user_name STRING COMMENT '查询姓名',
  remark STRING COMMENT '备注',
  create_user_id STRING COMMENT '创建人id',
  update_user_id STRING COMMENT '更新人id',
  create_time timestamp COMMENT '记录创建时间',
  update_time timestamp COMMENT '记录更新时间',
  rev bigint comment '版本号',
  delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
  sourcetype STRING COMMENT '征信来源',
  total_price DOUBLE COMMENT '查评估总价1',
  manual_total_price DOUBLE COMMENT '人工录入评估总价2',
  average_price DOUBLE COMMENT '查评估均价',
  liveness STRING COMMENT '活跃度',
  average_price2 DOUBLE COMMENT '查评估均价2',
  total_price2 DOUBLE COMMENT '查评估总价2',
  comprehensive_evaluation_price DOUBLE COMMENT '综合评估价',
  comprehensive_average_price DOUBLE COMMENT '综合均价',
  total_price3 DOUBLE COMMENT '云房总价',
  average_price3 DOUBLE COMMENT '云房均价',
  old_or_new STRING COMMENT '新老单标记',
  system_source STRING COMMENT '系统来源',
  query_condition STRING COMMENT '查询条件',
  price DOUBLE COMMENT '均价(元/m2)',
  query_way STRING COMMENT '查询方式(线上/线下)',
  query_result STRING COMMENT '查询结果状态',
  unique_id STRING COMMENT '查询条件',
  house_evaluation_company STRING COMMENT '房产评估公司',
  house_evaluation_price DOUBLE COMMENT '房产评估价格',
  house_evaluation_time timestamp COMMENT '房产评估时间',
  rn bigint
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_query_estimate_common
select T.*,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from
( SELECT
a.id,
a.apply_no,
a.house_no,
a.cert_no,
a.cert_type,
a.price_source,
a.query_time,
a.market_average_price,
a.query_user_id,
a.query_user_name,
a.remark,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
a.sourcetype,
a.total_price,
a.manual_total_price,
a.average_price,
a.liveness,
a.average_price2,
a.total_price2,
a.comprehensive_evaluation_price,
a.comprehensive_average_price,
a.total_price3,
a.average_price3,
a.old_or_new,
a.system_source,
a.query_condition,
a.price,
a.query_way,
a.query_result,
a.unique_id,
a.house_evaluation_company,
a.house_evaluation_price,
a.house_evaluation_time
FROM
( select * from ods_bpms_biz_query_estimate where apply_no<>'' and apply_no is not null ) a
union all
SELECT
a1.id,
c1.apply_no,
a1.house_no,
a1.cert_no,
a1.cert_type,
a1.price_source,
a1.query_time,
a1.market_average_price,
a1.query_user_id,
a1.query_user_name,
a1.remark,
a1.create_user_id,
a1.update_user_id,
a1.create_time,
a1.update_time,
a1.rev,
a1.delete_flag,
a1.sourcetype,
a1.total_price,
a1.manual_total_price,
a1.average_price,
a1.liveness,
a1.average_price2,
a1.total_price2,
a1.comprehensive_evaluation_price,
a1.comprehensive_average_price,
a1.total_price3,
a1.average_price3,
a1.old_or_new,
a1.system_source,
a1.query_condition,
a1.price,
a1.query_way,
a1.query_result,
a1.unique_id,
a1.house_evaluation_company,
a1.house_evaluation_price,
a1.house_evaluation_time
FROM
( select * from ods_bpms_biz_query_estimate where (apply_no<>'' and apply_no is not null) and(house_no<>'' and house_no is not null ) ) a1
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c1 ON c1.house_no = a1.house_no
union all
select
b.id,
c.apply_no apply_no,
b.house_no,
b.cert_no,
b.cert_type,
b.price_source,
b.query_time,
b.market_average_price,
b.query_user_id,
b.query_user_name,
b.remark,
b.create_user_id,
b.update_user_id,
b.create_time,
b.update_time,
b.rev,
b.delete_flag,
b.sourcetype,
b.total_price,
b.manual_total_price,
b.average_price,
b.liveness,
b.average_price2,
b.total_price2,
b.comprehensive_evaluation_price,
b.comprehensive_average_price,
b.total_price3,
b.average_price3,
b.old_or_new,
b.system_source,
b.query_condition,
b.price,
b.query_way,
b.query_result,
b.unique_id,
b.house_evaluation_company,
b.house_evaluation_price,
b.house_evaluation_time
from ( SELECT
*
FROM
ods_bpms_biz_query_estimate
WHERE
(house_no <> '' AND house_no IS NOT NULL ) and (apply_no ='' or apply_no is null )
) b
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c ON c.house_no = b.house_no
)T;
drop table if exists ods_bpms_biz_query_estimate_common;
ALTER TABLE odstmp_bpms_biz_query_estimate_common RENAME TO ods_bpms_biz_query_estimate_common;