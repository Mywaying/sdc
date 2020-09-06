use ods;
drop table if exists  odstmp_bpms_bpm_check_opinion_common;
CREATE TABLE if not exists ods.odstmp_bpms_bpm_check_opinion_common (
  apply_no STRING,
  product_id STRING,
  product_version STRING,
  product_type STRING,
  id_ STRING COMMENT '意见ID',
  proc_def_id_ STRING COMMENT '流程定义ID',
  sup_inst_id_ STRING COMMENT '父流程实例ID',
  proc_inst_id_ STRING COMMENT '流程实例ID',
  task_id_ STRING COMMENT '任务ID',
  task_key_ STRING COMMENT '任务定义Key',
  task_name_ STRING COMMENT '任务名称',
  token_ STRING COMMENT '任务令牌',
  qualfieds_ STRING COMMENT '有审批资格用户ID串',
  qualfied_names_ STRING COMMENT '有审批资格用户名称串',
  auditor_ STRING COMMENT '执行人ID',
  auditor_name_ STRING COMMENT '执行人名',
  opinion_ STRING COMMENT '审批意见',
  status_ STRING COMMENT '审批状态。start=发起流程；awaiting_check=待审批；agree=同意；against=反对；return=驳回；abandon=弃权；retrieve=追回',
  status_name_ STRING,
  form_def_id_ STRING COMMENT '表单定义ID',
  form_name_ STRING COMMENT '表单名',
  create_time_ timestamp COMMENT '执行开始时间',
  assign_time_ timestamp COMMENT '任务分配用户时间',
  complete_time_ timestamp COMMENT '结束时间',
  dur_ms_ BIGINT COMMENT '持续时间(ms)',
  files_ STRING COMMENT '附件',
  interpose_ bigint comment '是否干预',
  support_mobile_ STRING COMMENT '支持手机端',
  task_key_new_ STRING,
  task_name_new_ STRING,
  rn BIGINT ) STORED AS parquet;
drop table if exists odstmp_bpms_bpm_check_opinion_common_complete;
insert overwrite table  odstmp_bpms_bpm_check_opinion_common
select *,ROW_NUMBER() OVER(PARTITION BY apply_no,task_key_new_ ORDER BY if(complete_time_='',create_time_,complete_time_) asc) rn from (
SELECT * from ods_bpms_bpm_check_opinion_common_1
union all
SELECT * from ods_bpms_bpm_check_opinion_common_2
)T;
drop table if exists ods_bpms_bpm_check_opinion_common;
ALTER TABLE odstmp_bpms_bpm_check_opinion_common RENAME TO ods_bpms_bpm_check_opinion_common;
drop table if exists odstmp_bpms_bpm_check_opinion_common_complete;
create table odstmp_bpms_bpm_check_opinion_common_complete as
select *
from ods_bpms_bpm_check_opinion_common where apply_no is not null  and (complete_time_ is not null or complete_time_ <>'');