use ods;
drop table if exists  odstmp_bpms_bpm_check_opinion_common_ex_3;
CREATE TABLE if not exists ods.odstmp_bpms_bpm_check_opinion_common_ex_3 (
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
insert overwrite table odstmp_bpms_bpm_check_opinion_common_ex_3
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
						when product_id = "NSL-JYB371" then "v1.0"
						when product_id = "NSL-JYB377" then "v1.0"
						when product_id = "NSL-JYB755" then "v1.0"
						when product_id = "NSL-TFB371" then "v1.0"
						when product_id = "SL-TFB371" then "v1.0"
						when product_id = "SL-JYB371" then "v1.5"
						when product_id = "SL-JYB374" then "v1.5"
		else ''
		end
	END product_version,
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
 END product_type,
NULL ID_,
t2.PROC_DEF_ID_ PROC_DEF_ID_,
NULL SUP_INST_ID_,
t2.ID_ PROC_INST_ID_,
NULL TASK_ID_,
b.flow_type TASK_KEY_,
b.flow_type_name TASK_NAME_,
NULL TOKEN_,
NULL QUALFIEDS_,
NULL QUALFIED_NAMES_,
b.handle_user_id auditor_,
b.handle_user_name auditor_name_,
b.remark OPINION_,
NULL status_,
NULL STATUS_NAME_,
NULL FORM_DEF_ID_,
NULL FORM_NAME_,
b.create_time CREATE_TIME_,
NULL ASSIGN_TIME_,
b.handle_time COMPLETE_TIME_,
datediff(create_time,handle_time) DUR_MS_, -- b.due_time
NULL FILES_,
NULL INTERPOSE_,
NULL SUPPORT_MOBILE_,
b.flow_type_new task_key_new_,
b.flow_type_name_new task_name_new_,
NULL,
b.rn
from ods_bpms_biz_order_flow_common b join (
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
			ods_bpms_biz_apply_order
)bao on bao.apply_no=b.apply_no
LEFT JOIN ods_bpms_bpm_pro_inst t2 ON bao.flow_instance_id = t2.ID_
where
  (select count(1) as num from odstmp_bpms_bpm_check_opinion_common_complete a where a.apply_no=b.apply_no and lower(a.task_key_new_)=LOWER(b.flow_type_new)) = 0
  and (b.handle_time is not null or b.handle_time <>'');