use ods;
drop table if exists  odstmp_bims_bpm_check_opinion_common_ex;
CREATE TABLE if not exists ods.odstmp_bims_bpm_check_opinion_common_ex (
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
  rn bigint
) STORED AS parquet;
with check_option_true as (
  select *
  from ods_bims_bpm_check_opinion_common where apply_no is not null  and (complete_time_ is not null or complete_time_ <>'')
)
insert overwrite table odstmp_bims_bpm_check_opinion_common_ex
select * from (
select
 bao.apply_no apply_no,
 bao.product_id product_id,
 CASE
	WHEN t2.PROC_DEF_KEY_ IN (
		'bizApply_cash',
		'bizApply_all'
	) THEN
		'v1.0'
	WHEN t2.PROC_DEF_KEY_ IN (
		'bizApply_all_cash',
		'bizApply_all_insure',
		'bizApply_zztfb'
	) THEN
		'v1.5'
	WHEN t2.PROC_DEF_KEY_ IN (
		'bizApplyFlowCash_v2',
		'bizApplyFlowIns_v2',
		'bizApply_mortgage',
		'bizApplyTransition',
		'bizApply_mfb',
		'bizApplyFlowInsurance_v2'
	) THEN
		'v2.0'
	WHEN t2.PROC_DEF_KEY_ IN (
		'bizApplyFlowCash_v2_5',
		'bizApply_guaranty',
		'bizApplyFlowIns_v2_5',
		'bizApplyFlowIns_mfb',
		'bizApplyFlowIns_zyb'
	) THEN
		'v2.5'
	ELSE
		case
						when bao.product_id = "NSL-JYB371" then "v1.0"
						when bao.product_id = "NSL-JYB377" then "v1.0"
						when bao.product_id = "NSL-JYB755" then "v1.0"
						when bao.product_id = "NSL-TFB371" then "v1.0"
						when bao.product_id = "SL-TFB371" then "v1.0"
						when bao.product_id = "SL-JYB371" then "v1.5"
						when bao.product_id = "SL-JYB374" then "v1.5"
		else ''
		end
	END product_version,
	CASE
   WHEN (
    bao.product_id LIKE '%SLY%'
    OR bao.product_id LIKE '%JSD%'
   )
   THEN '现金类产品'
   WHEN (
    bao.product_id LIKE '%JYB%'
    OR bao.product_id LIKE '%TFB%'
    or bao.product_id like "%MFB%"
    or bao.product_id like "%ZYB%"
  				or bao.product_id like "%PMB_YJY_ISR%"

   )
   THEN '保险类产品'
   WHEN (
   bao.product_id LIKE '%AJFW%'
   ) then '按揭服务'
 ELSE ''
 END product_type,
NULL ID_,
t2.PROC_DEF_ID_ PROC_DEF_ID_,
NULL SUP_INST_ID_,
t2.ID_ PROC_INST_ID_,
NULL TASK_ID_,
a.matter_key TASK_KEY_,
a.matter_name TASK_NAME_,
NULL TOKEN_,
NULL QUALFIEDS_,
NULL QUALFIED_NAMES_,
a.handle_user_id auditor_,
a.handle_user_name auditor_name_,
nvl(b.opinion_,a.remark) OPINION_,
b.status_,
(CASE WHEN b.status_="agree" THEN   "同意"
 WHEN b.status_="start" THEN      "提交"
 WHEN b.status_="end" THEN       "结束"
 WHEN b.status_="awaiting_check" THEN "待审批"
 WHEN b.status_="oppose" THEN     "反对"
 WHEN b.status_="abandon" THEN "弃权"
 WHEN b.status_="reject" THEN "驳回"
 WHEN b.status_="backToStart" THEN "驳回到发起人"
 WHEN b.status_="revoker" THEN "撤回"
 WHEN b.status_="revoker_to_start" THEN "撤回到发起人"
 WHEN b.status_="manual_end" THEN "人工终止"
 ELSE NULL END ) AS STATUS_NAME_,
a.from_key FORM_DEF_ID_,
a.from_name FORM_NAME_,
a.create_time CREATE_TIME_,
a.accept_time ASSIGN_TIME_,
nvl(a.handle_time,b.complete_time_) COMPLETE_TIME_,
datediff(a.create_time,a.handle_time) DUR_MS_, -- b.due_time
NULL FILES_,
NULL INTERPOSE_,
NULL SUPPORT_MOBILE_,
matter_key_new task_key_new_,
matter_name_new task_name_new_,
a.rn
from ods_bims_biz_order_matter_record_common a join (
SELECT
			apply_no,
			flow_instance_id,
   product_id,
   CASE
   WHEN (
    product_id LIKE '%SLY%'
    OR product_id LIKE '%JSD%'
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
	 ELSE ''
	 END product_type
		FROM
			ods_bims_biz_apply_order
)bao on bao.apply_no=a.apply_no
LEFT JOIN ods_bims_bpm_pro_inst t2 ON bao.flow_instance_id = t2.ID_
left join check_option_true b on b.apply_no=a.apply_no and b.task_key_new_=a.matter_key_new and b.rn=a.rn and (from_timestamp(b.complete_time_,'yyyyMMddmm')=from_timestamp(a.handle_time,'yyyyMMddmm') or a.handle_time is null )
union all
select
b.apply_no,
b.product_id,
b.product_version,
b.product_type,
b.id_,
b.proc_def_id_,
b.sup_inst_id_,
b.proc_inst_id_,
b.task_id_,
b.task_key_,
b.task_name_,
b.token_,
b.qualfieds_,
b.qualfied_names_,
b.auditor_,
b.auditor_name_,
b.opinion_,
b.status_,
b.status_name_,
b.form_def_id_,
b.form_name_,
b.create_time_,
b.assign_time_,
b.complete_time_,
b.dur_ms_,
b.files_,
b.interpose_,
b.support_mobile_,
b.task_key_new_,
b.task_name_new_,
b.rn
from check_option_true b
where LOWER(b.task_key_new_) not in(select lower(matter_key_new) from ods_bims_biz_order_matter_record_common a where a.apply_no=b.apply_no)
)T;
drop table if exists ods_bims_bpm_check_opinion_common_ex;
ALTER TABLE odstmp_bims_bpm_check_opinion_common_ex RENAME TO ods_bims_bpm_check_opinion_common_ex;
