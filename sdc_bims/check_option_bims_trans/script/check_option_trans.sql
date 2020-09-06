use ods;
drop table if exists  odstmp_bims_bpm_check_opinion_common;
CREATE TABLE if not exists ods.odstmp_bims_bpm_check_opinion_common (
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

insert overwrite table  odstmp_bims_bpm_check_opinion_common
select *,ROW_NUMBER() OVER(PARTITION BY apply_no,task_key_new_ ORDER BY if(complete_time_='',create_time_,complete_time_) asc) rn from (
SELECT
bao.apply_no,
bao.product_id,
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
a.ID_,
a.PROC_DEF_ID_,
a.SUP_INST_ID_,
a.PROC_INST_ID_,
a.TASK_ID_,
a.TASK_KEY_,
a.TASK_NAME_,
a.TOKEN_,
a.QUALFIEDS_,
a.QUALFIED_NAMES_,
a.AUDITOR_,
a.AUDITOR_NAME_,
a.OPINION_,
a.STATUS_,
(CASE WHEN a.STATUS_="agree" THEN      "同意"
 WHEN a.STATUS_="start" THEN           "提交"
 WHEN a.STATUS_="end" THEN             "结束"
 WHEN a.STATUS_="awaiting_check" THEN  "待审批"
 WHEN a.STATUS_="oppose" THEN          "反对"
 WHEN a.STATUS_="abandon" THEN "弃权"
 WHEN a.STATUS_="reject" THEN "驳回"
 WHEN a.STATUS_="backToStart" THEN "驳回到发起人"
 WHEN a.STATUS_="revoker" THEN "撤回"
 WHEN a.STATUS_="revoker_to_start" THEN "撤回到发起人"
 WHEN a.STATUS_="manual_end" THEN "人工终止"
 ELSE NULL END )  AS STATUS_NAME_,
a.FORM_DEF_ID_,
a.FORM_NAME_,
a.CREATE_TIME_,
a.ASSIGN_TIME_,
a.COMPLETE_TIME_,
a.DUR_MS_,
a.FILES_,
a.INTERPOSE_,
a.SUPPORT_MOBILE_,
lower(task_key_)  task_key_new_,
case
when LOWER(task_key_)='pushloancommand' and task_name_='放款指令推送' then '出账指令推送'
when LOWER(task_key_)='sendloancommand' and task_name_='发送放款指令' then '出账审核'
when LOWER(task_key_)='sendloancommand' and task_name_='发送放款指令' then '出账审核'
when LOWER(task_key_)='householdapprove' and task_name_='下户' then '下户审批'
else task_name_ end task_name_new_
FROM
(select * from ods_bims_bpm_check_opinion where SUP_INST_ID_='0') a
  left join ods_bims_biz_apply_order bao on (a.PROC_INST_ID_=bao.flow_instance_id )
LEFT JOIN ods_bims_bpm_pro_inst t2 ON bao.flow_instance_id = t2.ID_
union all
SELECT
bao.apply_no,
bao.product_id,
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
a.ID_,
a.PROC_DEF_ID_,
a.SUP_INST_ID_,
a.PROC_INST_ID_,
a.TASK_ID_,
a.TASK_KEY_,
a.TASK_NAME_,
a.TOKEN_,
a.QUALFIEDS_,
a.QUALFIED_NAMES_,
a.AUDITOR_,
a.AUDITOR_NAME_,
a.OPINION_,
a.STATUS_,
(CASE WHEN a.STATUS_="agree" THEN      "同意"
 WHEN a.STATUS_="start" THEN           "提交"
 WHEN a.STATUS_="end" THEN             "结束"
 WHEN a.STATUS_="awaiting_check" THEN  "待审批"
 WHEN a.STATUS_="oppose" THEN          "反对"
 WHEN a.STATUS_="abandon" THEN "弃权"
 WHEN a.STATUS_="reject" THEN "驳回"
 WHEN a.STATUS_="backToStart" THEN "驳回到发起人"
 WHEN a.STATUS_="revoker" THEN "撤回"
 WHEN a.STATUS_="revoker_to_start" THEN "撤回到发起人"
 WHEN a.STATUS_="manual_end" THEN "人工终止"
 ELSE NULL END )  AS STATUS_NAME_,
a.FORM_DEF_ID_,
a.FORM_NAME_,
a.CREATE_TIME_,
a.ASSIGN_TIME_,
a.COMPLETE_TIME_,
a.DUR_MS_,
a.FILES_,
a.INTERPOSE_,
a.SUPPORT_MOBILE_,
lower(task_key_)  task_key_new_,
case
when LOWER(task_key_)='pushloancommand' and task_name_='放款指令推送' then '出账指令推送'
when LOWER(task_key_)='sendloancommand' and task_name_='发送放款指令' then '出账审核'
when LOWER(task_key_)='sendloancommand' and task_name_='发送放款指令' then '出账审核'
when LOWER(task_key_)='householdapprove' and task_name_='下户' then '下户审批'
else task_name_ end task_name_new_
FROM
(select * from ods_bims_bpm_check_opinion where SUP_INST_ID_<>'0') a left join ods_bims_biz_apply_order bao on (a.SUP_INST_ID_= bao.flow_instance_id )
LEFT JOIN ods_bims_bpm_pro_inst t2 ON bao.flow_instance_id = t2.ID_)T;
drop table if exists ods_bims_bpm_check_opinion_common;
ALTER TABLE odstmp_bims_bpm_check_opinion_common RENAME TO ods_bims_bpm_check_opinion_common;