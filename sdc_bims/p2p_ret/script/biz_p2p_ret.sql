use ods;
drop table if  exists ods.odstmp_bims_biz_p2p_ret_common;
CREATE TABLE ods.odstmp_bims_biz_p2p_ret_common (
      id STRING COMMENT '主键Id',
   apply_no STRING COMMENT '订单编号',
   partner_id STRING COMMENT '合作机构编号',
   partner_name STRING COMMENT '合作机构名称',
   task_name STRING COMMENT '请求事件(TEAM_ORG_APPROVE=审批通过，LOAN=放款，EXTEN=展期，REPAYMENT=还款)',
   status bigint comment '返回状态（1=成功，2=拒绝，3=退回（缺少材料，需补充材料后继续做审核），4=失败）',
   result STRING COMMENT '返回结果，不要status了',
   description STRING COMMENT '返回结果备注',
   create_time TIMESTAMP COMMENT '插入时间',
   update_time TIMESTAMP COMMENT '更新时间',
   status_name STRING COMMENT '返回状态',
   rn bigint
   )  STORED AS parquet;
insert overwrite table odstmp_bims_biz_p2p_ret_common
select T.*,

ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time asc) rn from (
select
a.`id`,
a.`apply_no`,
a.`partner_id`,
a.`partner_name`,
a.`task_name`,
a.`status`,
a.`result`,
a.`description`,
a.`create_time`,
a.`update_time`,
cast (a.`status` as string)
from ods_bims_biz_p2p_ret a
)T;

drop table if exists ods_bims_biz_p2p_ret_common;
ALTER TABLE odstmp_bims_biz_p2p_ret_common RENAME TO ods_bims_biz_p2p_ret_common;