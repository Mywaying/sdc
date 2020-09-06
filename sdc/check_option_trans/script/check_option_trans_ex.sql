use ods;
drop table if exists  odstmp_bpms_bpm_check_opinion_common_ex;
CREATE TABLE if not exists ods.odstmp_bpms_bpm_check_opinion_common_ex (
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
  status_ STRING COMMENT '审批状态。start=发起流程 awaiting_check=待审批 agree=同意 against=反对 return=驳回 abandon=弃权 retrieve=追回',
  status_name_ STRING,
  form_def_id_ STRING COMMENT '表单定义ID',
  form_name_ STRING COMMENT '表单名',
  create_time_ timestamp COMMENT '执行开始时间',
  assign_time_ timestamp COMMENT '任务分配用户时间',
  complete_time_ timestamp COMMENT '结束时间',
  dur_ms_ bigint COMMENT '持续时间(ms)',
  files_ STRING COMMENT '附件',
  interpose_ bigint comment '是否干预',
  support_mobile_ STRING COMMENT '支持手机端',
  task_key_new_ STRING,
  task_name_new_ STRING,
  is_outside_handle STRING COMMENT '是否外传办理',
  rn bigint
) STORED AS parquet;
insert overwrite table odstmp_bpms_bpm_check_opinion_common_ex
select * from odstmp_bpms_bpm_check_opinion_common_ex_1
union all
select * from odstmp_bpms_bpm_check_opinion_common_ex_2
union all
select * from odstmp_bpms_bpm_check_opinion_common_ex_3;
drop table if exists ods_bpms_bpm_check_opinion_common_ex;
ALTER TABLE odstmp_bpms_bpm_check_opinion_common_ex RENAME TO ods_bpms_bpm_check_opinion_common_ex;
