use ods;
drop table if  exists ods.odstmp_app_biz_risk_control_common;
CREATE TABLE ods.odstmp_app_biz_risk_control_common (
    id BIGINT COMMENT '主键',
   apply_no STRING COMMENT '订单编号',
   customer_no STRING COMMENT '客户编号',
   credit_type STRING COMMENT '征信查询类型(最新一次，不论成功失败)[report:上传征信报告zld:中兰德征信查询认证码]',
   zld_code STRING COMMENT '中兰德征信授权码',
   credit_report_id STRING COMMENT '主借款人征信报告id',
   credit_result STRING COMMENT '主借款人征信结果',
   credit_result_explain STRING COMMENT '主借款人主借款人征信结果说明',
   decision_id STRING COMMENT '决策id',
   judge_result STRING COMMENT '审核结果',
   judge_result_explain STRING COMMENT '审核结果说明',
   has_tfb STRING COMMENT '有无提放保',
   remarks STRING COMMENT '备注信息',
   create_user_id STRING COMMENT '创建用户id',
   create_date TIMESTAMP COMMENT '创建时间',
   update_user_id STRING COMMENT '更新用户id',
   update_date TIMESTAMP COMMENT '更新时间',
   del_flag STRING COMMENT '删除标记（0：正常；1：删除；2：审核）',
   rulename STRING COMMENT '大数i房否决原因',
   rn bigint
   )  STORED AS parquet;
insert overwrite table odstmp_app_biz_risk_control_common
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no,credit_result ORDER BY create_date asc) rn from (
select
a.`id`,
a.`apply_no`,
a.`customer_no`,
a.`credit_type`,
a.`zld_code`,
a.`credit_report_id`,
a.`credit_result`,
regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`credit_result_explain`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),
a.`decision_id`,
a.`judge_result`,
regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`judge_result_explain`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),
a.`has_tfb`,
a.`remarks`,
a.`create_user_id`,
a.`create_date`,
a.`update_user_id`,
a.`update_date`,
a.`del_flag`,
case when a.judge_result='D' then
get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`judge_result_explain`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.comHitList[0].ruleName')
else NULL end ruleName
from ods_app_biz_risk_control a)T;
drop table if exists ods_app_biz_risk_control_common;
ALTER TABLE odstmp_app_biz_risk_control_common RENAME TO ods_app_biz_risk_control_common;