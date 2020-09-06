use ods;
drop table if exists  odstmp_bpms_bpm_check_opinion_common_1;
CREATE TABLE if not exists ods.odstmp_bpms_bpm_check_opinion_common_1 (
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
  task_name_new_ STRING ) STORED AS parquet;
drop table if exists odstmp_bpms_bpm_check_opinion_common_1_complete;
insert overwrite table  odstmp_bpms_bpm_check_opinion_common_1
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
CASE
when LOWER(task_key_)='householdapprove' and task_name_='下户审批' then 'householdapprove'
when LOWER(task_key_)='householdsurvey' and task_name_='下户调查' then 'householdsurvey'
when LOWER(task_key_)='downhousesurvey'  then 'householdsurvey'
when LOWER(task_key_)='usertask3' and task_name_='业务报单' then 'applycheck'
when LOWER(task_key_)='applyorder' and task_name_='业务申请' then 'applyorder'
when LOWER(task_key_)='usertask1' and task_name_='业务申请' then 'applyorder'
when LOWER(task_key_)='mancheck' and task_name_='人工审批' then 'mancheck'
when LOWER(task_key_)='usertask8' and task_name_='人工审核' then 'mancheck2'
when LOWER(task_key_)='insuranceinfoconfirm' and task_name_='保单信息确认' then 'insuranceinfoconfirm'
when LOWER(task_key_)='usertask8' and task_name_='保单确认' then 'insuranceinfoconfirm'
when LOWER(task_key_)='usertask9' and task_name_='保单确认' then 'insuranceinfoconfirm'
when LOWER(task_key_)='internalturnfunds' and task_name_='内部转款-归还' then 'internalturnFunds'
when LOWER(task_key_)='policyapply' and task_name_='出保单申请' then 'policyapply'
when LOWER(task_key_)='billcheck' and task_name_='出账前审查' then 'billcheck'
when LOWER(task_key_)='notarization' and task_name_='办理公证' then 'notarization'
when LOWER(task_key_)='mortgagepass' and task_name_='办理抵押' then 'mortgagepass'
when LOWER(task_key_)='startnode' and task_name_='发起节点' then 'startnode'
when LOWER(task_key_)='sendloancommand' and task_name_='发送放款指令' then 'sendloancommand'
when LOWER(task_key_)='sendloancommand1' and task_name_='发送放款指令' then 'sendloancommand'
when LOWER(task_key_)='usertask5' and task_name_='发送放款指令' then 'sendloancommand'
when LOWER(task_key_)='sendloanrelesecommand' and task_name_='发送款项释放指令' then 'sendloanrelesecommand'
when LOWER(task_key_)='usertask5' and task_name_='发送贷款指令' then 'sendloancommand'
when LOWER(task_key_)='agreeloanmark' and task_name_='同贷信息登记' then 'agreeloanmark'
when LOWER(task_key_)='agreeloanmark_atone' and task_name_='同贷信息登记(赎楼贷款)' then 'agreeloanmark_atone'
when LOWER(task_key_)='agreeloanmark' and task_name_='同贷登记' then 'agreeloanmark'
when LOWER(task_key_)='pushoutcommand' and task_name_='外传指令推送' then 'pushoutcommand'
when LOWER(task_key_)='patch_ds' and task_name_='大数补件' then 'patch_ds'
when LOWER(task_key_)='mancheck' and task_name_='审批' then 'mancheck'
when LOWER(task_key_)='usertask4' and task_name_='审批' then 'mancheck'
when LOWER(task_key_)='checkresensure' and task_name_='审批结果确定' then 'checkresensure'
when LOWER(task_key_)='approvalresult' and task_name_='审批结果确认' then 'approvalresult'
when LOWER(task_key_)='checkmarsend' and task_name_='审批资料外传' then 'checkmarsend'
when LOWER(task_key_)='usertask7' and task_name_='审批资料外传' then 'checkmarsend'
when LOWER(task_key_)='checkmarpush' and task_name_='审批资料推送' then 'checkmarpush'
when LOWER(task_key_)='investigate' and task_name_='审查' then 'investigate'
when LOWER(task_key_)='usertask3' and task_name_='审查' then 'investigate'
when LOWER(task_key_)='transfertail' and task_name_='尾款划拨申请' then 'transfertail'
when LOWER(task_key_)='confirmtransfertail' and task_name_='尾款划拨确认' then 'confirmtransfertail'
when LOWER(task_key_)='usertask10' and task_name_='尾款释放' then 'transfertailrelese'
when LOWER(task_key_)='usertask9' and task_name_='尾款释放' then 'transfertailrelese'
when LOWER(task_key_)='prefile' and task_name_='归档' then 'prefile'
when LOWER(task_key_)='usertask6' and task_name_='归档' then 'prefile'
when LOWER(task_key_)='inputinfo' and task_name_='录入' then 'inputinfo'
when LOWER(task_key_)='inputinfocomplete' and task_name_='录入完成' then 'inputinfocomplete'
when LOWER(task_key_)='creditconfirm' and task_name_='征信确认' then 'creditconfirm'
when LOWER(task_key_)='houseappraisal' and task_name_='房产评估' then 'houseappraisal'
when LOWER(task_key_)='usertask2' and task_name_='报单' then 'applycheck'
when LOWER(task_key_)='applycheck' and task_name_='报审' then 'applycheck'
when LOWER(task_key_)='mortgageout' and task_name_='抵押出件' then 'mortgageout'
when LOWER(task_key_)='mortgageout_zz' and task_name_='抵押出件_郑州' then 'mortgageout_zz'
when LOWER(task_key_)='mortgagepass' and task_name_='抵押递件' then 'mortgagepass'
when LOWER(task_key_)='mortgagepass_zz' and task_name_='抵押递件_郑州' then 'mortgagepass_zz'
when LOWER(task_key_)='submitverifyapply' and task_name_='提交核保申请' then 'submitverifyapply'
when LOWER(task_key_)='usertask5' and task_name_='放款' then 'sendloancommand'
when LOWER(task_key_)='pushloancommand' and task_name_='放款指令推送' then 'pushloancommand'
when LOWER(task_key_)='transfermark' and task_name_='放款登记' then 'transfermark'
when LOWER(task_key_)='houseinspection' and task_name_='查房' then 'houseinspection'
when LOWER(task_key_)='queryarchive' and task_name_='查档' then 'queryarchive'
when LOWER(task_key_)='sendverifymaterial' and task_name_='核保资料外传' then 'sendverifymaterial'
when LOWER(task_key_)='usertask7' and task_name_='核保资料外传' then 'sendverifymaterial'
when LOWER(task_key_)='canclemortgage' and task_name_='注销抵押' then 'canclemortgage'
when LOWER(task_key_)='canclemortgage_zz' and task_name_='注销抵押_郑州' then 'canclemortgage_zz'
when LOWER(task_key_)='applyloan' and task_name_='申请贷款' then 'applyloan'
when LOWER(task_key_)='applyloan_atone' and task_name_='申请贷款(赎楼贷款)' then 'applyloan_atone'
when LOWER(task_key_)='paymentarrival' and task_name_='确认回款资金到账' then 'paymentarrival'
when LOWER(task_key_)='platformarrival' and task_name_='确认平台款到账' then 'platformarrival'
when LOWER(task_key_)='confirmarrival' and task_name_='确认资金到账' then 'confirmarrival'
when LOWER(task_key_)='fundsarrival' and task_name_='确认资金到账' then 'confirmarrival'
when LOWER(task_key_)='confirmtailarrial' and task_name_='确认资金到账（尾款）' then 'confirmtailarrial'
when LOWER(task_key_)='usertask4' and task_name_='终审' then 'mancheck'
when LOWER(task_key_)='endnode' and task_name_='结束节点' then 'endnode'
when LOWER(task_key_)='costconfirm' and task_name_='缴费确认' then 'costconfirm'
when LOWER(task_key_)='trustaccount' and task_name_='要件托管' then 'trustaccount'
when LOWER(task_key_)='overinsurance' and task_name_='解保' then 'overinsurance'
when LOWER(task_key_)='usertask1' and task_name_='订单生成/业务申请' then 'applyorder'
when LOWER(task_key_)='financialarchiveevent' and task_name_='财务归档' then 'financialarchiveevent'
when LOWER(task_key_)='accounttest' and task_name_='账户测试' then 'accounttest'
when LOWER(task_key_)='finalloan' and task_name_='贷款上报终审' then 'finalloan'
when LOWER(task_key_)='costmark' and task_name_='费率登记' then 'costmark'
when LOWER(task_key_)='costmark' and task_name_='费用登记' then 'costmark'
when LOWER(task_key_)='costitemmark' and task_name_='费项登记' then 'costitemmark'
when LOWER(task_key_)='uploadimg' and task_name_='资料反馈' then 'uploadimg'
when LOWER(task_key_)='usertask6' and task_name_='资料归档' then 'prefile'
when LOWER(task_key_)='datainspection' and task_name_='资料齐全' then 'datainspection'
when LOWER(task_key_)='' and task_name_='资金划拨申请' then 'transferapply'
when LOWER(task_key_)='transferapply' and task_name_='资金划拨申请' then 'transferapply'
when LOWER(task_key_)='usertask1' and task_name_='资金划拨申请' then 'transferapply'
when LOWER(task_key_)='' and task_name_='资金划拨确认' then 'transferconfirm'
when LOWER(task_key_)='transferconfirm' and task_name_='资金划拨确认' then 'transferconfirm'
when LOWER(task_key_)='usertask2' and task_name_='资金划拨确认' then 'transferconfirm'
when LOWER(task_key_)='' and task_name_='资金归还申请' then 'returnapply'
when LOWER(task_key_)='returnapply' and task_name_='资金归还申请' then 'returnapply'
when LOWER(task_key_)='usertask1' and task_name_='资金归还申请' then 'returnapply'
when LOWER(task_key_)='returnconfirm' and task_name_='资金归还确认' then 'returnconfirm'
when LOWER(task_key_)='usertask2' and task_name_='资金归还确认' then 'returnconfirm'
when LOWER(task_key_)='flooradvance' and task_name_='赎楼前垫资回款' then 'flooradvance'
when LOWER(task_key_)='randommark' and task_name_='赎楼登记' then 'randommark'
when LOWER(task_key_)='randomcashback' and task_name_='赎楼资金划回' then 'randomcashback'
when LOWER(task_key_)='randomreback' and task_name_='赎楼资金划回' then 'randomcashback'
when LOWER(task_key_)='transferout' and task_name_='过户出件' then 'transferout'
when LOWER(task_key_)='transferout_zz' and task_name_='过户出件_郑州' then 'transferout_zz'
when LOWER(task_key_)='transferin' and task_name_='过户递件' then 'transferin'
when LOWER(task_key_)='transferin_zz' and task_name_='过户递件_郑州' then 'transferin_zz'
when LOWER(task_key_)='repaymentconfirm' and task_name_='还款确认' then 'repaymentconfirm'
when LOWER(task_key_)='usertask5' and task_name_='通知机构放款' then 'sendloancommand'
when LOWER(task_key_)='usertask8' and task_name_='陆金所资金归还' then 'lureturnapply'
when LOWER(task_key_)='interview' and task_name_='面签' then 'interview'
when LOWER(task_key_)='usertask2' and task_name_='面签' then 'interview'
when LOWER(task_key_)='usertask10' and task_name_='预审批' then 'precheck'
when LOWER(task_key_)='usertask9' and task_name_='预审批' then 'precheck'
when LOWER(task_key_)='prerandom' and task_name_='预约赎楼' then 'prerandom'
when LOWER(task_key_)='appointinterview' and task_name_='预约面签' then 'appointinterview'
when LOWER(task_key_)='getcancelmaterial' and task_name_='领取注销资料' then 'getcancelmaterial'
when LOWER(task_key_)='agreeloanresult'  then 'agreeloanresult'
when LOWER(task_key_)='inputinfocomplete'  then 'inputinfocomplete'
when LOWER(task_key_)='usertask6' and task_name_='运营主管审批' then 'operation_supervisor'
when LOWER(task_key_)='usertask3' and task_name_='归档' then 'prefile'
when LOWER(task_key_)='usertask2' and task_name_='总部风险审批' then 'management_division'
when LOWER(task_key_)='usertask5' and task_name_='科技' then 'technology'
when LOWER(task_key_)='usertask4' and task_name_='总部运营审批' then 'manager_operation'
when LOWER(task_key_)='usertask13' and task_name_='业务申请' then 'applyorder'
when LOWER(task_key_)='usertask7' and task_name_='运营主管审批' then 'operation_supervisor'
when LOWER(task_key_)='usertask11' and task_name_='集中作业中心审批' then 'centralized_operation'
when LOWER(task_key_)='usertask10' and task_name_='风险部审批' then 'riskmanager'
when LOWER(task_key_)='usertask14' and task_name_='集中作业中心主管审批' then 'centralized_operation_manager'
when LOWER(task_key_)='usertask8' and task_name_='市场主管审批' then 'marketing_supervisor'
when LOWER(task_key_)='usertask12' and task_name_='资金岗审批' then 'capitalduty'
when LOWER(task_key_)='usertask9' and task_name_='销管审批' then 'sales_operation'
ELSE
task_key_
end task_key_new_,
case
when LOWER(task_key_)='householdapprove' and task_name_='下户审批' then '下户审批'
when LOWER(task_key_)='householdsurvey' and task_name_='下户调查' then '下户调查/下户'
when LOWER(task_key_)='downhousesurvey'  then '下户调查/下户'
when LOWER(task_key_)='usertask3' and task_name_='业务报单' then '报审/报单'
when LOWER(task_key_)='applyorder' and task_name_='业务申请' then '业务申请'
when LOWER(task_key_)='usertask1' and task_name_='业务申请' then '业务申请'
when LOWER(task_key_)='mancheck' and task_name_='人工审批' then '审批'
when LOWER(task_key_)='usertask8' and task_name_='人工审核' then '人工审核'
when LOWER(task_key_)='insuranceinfoconfirm' and task_name_='保单信息确认' then '保单信息确认'
when LOWER(task_key_)='usertask8' and task_name_='保单确认' then '保单确认'
when LOWER(task_key_)='usertask9' and task_name_='保单确认' then '保单确认'
when LOWER(task_key_)='internalturnfunds' and task_name_='内部转款-归还' then '内部转款-归还'
when LOWER(task_key_)='policyapply' and task_name_='出保单申请' then '出保单申请'
when LOWER(task_key_)='billcheck' and task_name_='出账前审查' then '出账前审查'
when LOWER(task_key_)='notarization' and task_name_='办理公证' then '办理公证'
when LOWER(task_key_)='mortgagepass' and task_name_='办理抵押' then '抵押递件'
when LOWER(task_key_)='startnode' and task_name_='发起节点' then '发起节点'
when LOWER(task_key_)='sendloancommand' and task_name_='发送放款指令' then '发送放款指令'
when LOWER(task_key_)='sendloancommand1' and task_name_='发送放款指令' then '发送放款指令'
when LOWER(task_key_)='usertask5' and task_name_='发送放款指令' then '发送放款指令'
when LOWER(task_key_)='sendloanrelesecommand' and task_name_='发送款项释放指令' then '发送款项释放指令'
when LOWER(task_key_)='usertask5' and task_name_='发送贷款指令' then '发送放款指令'
when LOWER(task_key_)='agreeloanmark' and task_name_='同贷信息登记' then '同贷信息登记/同贷登记'
when LOWER(task_key_)='agreeloanmark_atone' and task_name_='同贷信息登记(赎楼贷款)' then '同贷信息登记(赎楼贷款)'
when LOWER(task_key_)='agreeloanmark' and task_name_='同贷登记' then '同贷信息登记/同贷登记'
when LOWER(task_key_)='pushoutcommand' and task_name_='外传指令推送' then '外传指令推送/审批资料推送'
when LOWER(task_key_)='patch_ds' and task_name_='大数补件' then '大数补件'
when LOWER(task_key_)='mancheck' and task_name_='审批' then '审批'
when LOWER(task_key_)='usertask4' and task_name_='审批' then '审批'
when LOWER(task_key_)='checkresensure' and task_name_='审批结果确定' then '审批结果确定/审批结果确认'
when LOWER(task_key_)='approvalresult' and task_name_='审批结果确认' then '审批结果确定/审批结果确认'
when LOWER(task_key_)='checkmarsend' and task_name_='审批资料外传' then '审批资料外传'
when LOWER(task_key_)='usertask7' and task_name_='审批资料外传' then '审批资料外传'
when LOWER(task_key_)='checkmarpush' and task_name_='审批资料推送' then '外传指令推送/审批资料推送'
when LOWER(task_key_)='investigate' and task_name_='审查' then '审查'
when LOWER(task_key_)='usertask3' and task_name_='审查' then '审查'
when LOWER(task_key_)='transfertail' and task_name_='尾款划拨申请' then '尾款划拨申请'
when LOWER(task_key_)='confirmtransfertail' and task_name_='尾款划拨确认' then '尾款划拨确认'
when LOWER(task_key_)='usertask10' and task_name_='尾款释放' then '尾款释放'
when LOWER(task_key_)='usertask9' and task_name_='尾款释放' then '尾款释放'
when LOWER(task_key_)='prefile' and task_name_='归档' then '归档'
when LOWER(task_key_)='usertask6' and task_name_='归档' then '归档'
when LOWER(task_key_)='inputinfo' and task_name_='录入' then '录入'
when LOWER(task_key_)='inputinfocomplete' and task_name_='录入完成' then '录入完成'
when LOWER(task_key_)='creditconfirm' and task_name_='征信确认' then '征信确认'
when LOWER(task_key_)='houseappraisal' and task_name_='房产评估' then '房产评估'
when LOWER(task_key_)='usertask2' and task_name_='报单' then '报审/报单'
when LOWER(task_key_)='applycheck' and task_name_='报审' then '报审/报单'
when LOWER(task_key_)='mortgageout' and task_name_='抵押出件' then '抵押出件'
when LOWER(task_key_)='mortgageout_zz' and task_name_='抵押出件_郑州' then '抵押出件_郑州'
when LOWER(task_key_)='mortgagepass' and task_name_='抵押递件' then '抵押递件'
when LOWER(task_key_)='mortgagepass_zz' and task_name_='抵押递件_郑州' then '抵押递件_郑州'
when LOWER(task_key_)='submitverifyapply' and task_name_='提交核保申请' then '提交核保申请'
when LOWER(task_key_)='usertask5' and task_name_='放款' then '发送放款指令'
when LOWER(task_key_)='pushloancommand' and task_name_='放款指令推送' then '放款指令推送'
when LOWER(task_key_)='transfermark' and task_name_='放款登记' then '放款登记'
when LOWER(task_key_)='houseinspection' and task_name_='查房' then '查房'
when LOWER(task_key_)='queryarchive' and task_name_='查档' then '查档'
when LOWER(task_key_)='sendverifymaterial' and task_name_='核保资料外传' then '核保资料外传'
when LOWER(task_key_)='usertask7' and task_name_='核保资料外传' then '核保资料外传'
when LOWER(task_key_)='canclemortgage' and task_name_='注销抵押' then '注销抵押'
when LOWER(task_key_)='canclemortgage_zz' and task_name_='注销抵押_郑州' then '注销抵押_郑州'
when LOWER(task_key_)='applyloan' and task_name_='申请贷款' then '申请贷款'
when LOWER(task_key_)='applyloan_atone' and task_name_='申请贷款(赎楼贷款)' then '申请贷款(赎楼贷款)'
when LOWER(task_key_)='paymentarrival' and task_name_='确认回款资金到账' then '确认回款资金到账'
when LOWER(task_key_)='platformarrival' and task_name_='确认平台款到账' then '确认平台款到账'
when LOWER(task_key_)='confirmarrival' and task_name_='确认资金到账' then '确认资金到账'
when LOWER(task_key_)='fundsarrival' and task_name_='确认资金到账' then '确认资金到账'
when LOWER(task_key_)='confirmtailarrial' and task_name_='确认资金到账（尾款）' then '确认资金到账(尾款)'
when LOWER(task_key_)='usertask4' and task_name_='终审' then '审批'
when LOWER(task_key_)='endnode' and task_name_='结束节点' then '结束节点'
when LOWER(task_key_)='costconfirm' and task_name_='缴费确认' then '缴费确认'
when LOWER(task_key_)='trustaccount' and task_name_='要件托管' then '要件托管'
when LOWER(task_key_)='overinsurance' and task_name_='解保' then '解保'
when LOWER(task_key_)='usertask1' and task_name_='订单生成/业务申请' then '业务申请'
when LOWER(task_key_)='financialarchiveevent' and task_name_='财务归档' then '财务归档'
when LOWER(task_key_)='accounttest' and task_name_='账户测试' then '账户测试'
when LOWER(task_key_)='finalloan' and task_name_='贷款上报终审' then '贷款上报终审'
when LOWER(task_key_)='costmark' and task_name_='费率登记' then '费率登记/费用登记'
when LOWER(task_key_)='costmark' and task_name_='费用登记' then '费率登记/费用登记'
when LOWER(task_key_)='costitemmark' and task_name_='费项登记' then '费项登记'
when LOWER(task_key_)='uploadimg' and task_name_='资料反馈' then '资料反馈'
when LOWER(task_key_)='usertask6' and task_name_='资料归档' then '归档'
when LOWER(task_key_)='datainspection' and task_name_='资料齐全' then '资料齐全'
when LOWER(task_key_)='' and task_name_='资金划拨申请' then '资金划拨申请'
when LOWER(task_key_)='transferapply' and task_name_='资金划拨申请' then '资金划拨申请'
when LOWER(task_key_)='usertask1' and task_name_='资金划拨申请' then '资金划拨申请'
when LOWER(task_key_)='' and task_name_='资金划拨确认' then '资金划拨确认'
when LOWER(task_key_)='transferconfirm' and task_name_='资金划拨确认' then '资金划拨确认'
when LOWER(task_key_)='usertask2' and task_name_='资金划拨确认' then '资金划拨确认'
when LOWER(task_key_)='' and task_name_='资金归还申请' then '资金归还申请'
when LOWER(task_key_)='returnapply' and task_name_='资金归还申请' then '资金归还申请'
when LOWER(task_key_)='usertask1' and task_name_='资金归还申请' then '资金归还申请'
when LOWER(task_key_)='returnconfirm' and task_name_='资金归还确认' then '资金归还确认'
when LOWER(task_key_)='usertask2' and task_name_='资金归还确认' then '资金归还确认'
when LOWER(task_key_)='flooradvance' and task_name_='赎楼前垫资回款' then '赎楼前垫资回款'
when LOWER(task_key_)='randommark' and task_name_='赎楼登记' then '赎楼登记'
when LOWER(task_key_)='randomcashback' and task_name_='赎楼资金划回' then '赎楼资金划回'
when LOWER(task_key_)='randomreback' and task_name_='赎楼资金划回' then '赎楼资金划回'
when LOWER(task_key_)='transferout' and task_name_='过户出件' then '过户出件'
when LOWER(task_key_)='transferout_zz' and task_name_='过户出件_郑州' then '过户出件_郑州'
when LOWER(task_key_)='transferin' and task_name_='过户递件' then '过户递件'
when LOWER(task_key_)='transferin_zz' and task_name_='过户递件_郑州' then '过户递件_郑州'
when LOWER(task_key_)='repaymentconfirm' and task_name_='还款确认' then '还款确认'
when LOWER(task_key_)='usertask5' and task_name_='通知机构放款' then '发送放款指令'
when LOWER(task_key_)='usertask8' and task_name_='陆金所资金归还' then '陆金所资金归还'
when LOWER(task_key_)='interview' and task_name_='面签' then '面签'
when LOWER(task_key_)='usertask2' and task_name_='面签' then '面签'
when LOWER(task_key_)='usertask10' and task_name_='预审批' then '预审批'
when LOWER(task_key_)='usertask9' and task_name_='预审批' then '预审批'
when LOWER(task_key_)='prerandom' and task_name_='预约赎楼' then '预约赎楼'
when LOWER(task_key_)='appointinterview' and task_name_='预约面签' then '预约面签'
when LOWER(task_key_)='getcancelmaterial' and task_name_='领取注销资料' then '领取注销资料'
when LOWER(task_key_)='agreeloanresult'  then '确认银行放款'
when LOWER(task_key_)='inputinfocomplete'  then '录入完成'
when LOWER(task_key_)='usertask6' and task_name_='运营主管审批' then '运营主管审批'
when LOWER(task_key_)='usertask3' and task_name_='归档' then '归档'
when LOWER(task_key_)='usertask2' and task_name_='总部风险审批' then '总部风险审批'
when LOWER(task_key_)='usertask5' and task_name_='科技' then '科技'
when LOWER(task_key_)='usertask4' and task_name_='总部运营审批' then '总部运营审批'
when LOWER(task_key_)='usertask13' and task_name_='业务申请' then '业务申请'
when LOWER(task_key_)='usertask7' and task_name_='运营主管审批' then '运营主管审批'
when LOWER(task_key_)='usertask11' and task_name_='集中作业中心审批' then '集中作业中心审批'
when LOWER(task_key_)='usertask10' and task_name_='风险部审批' then '风险部审批'
when LOWER(task_key_)='usertask14' and task_name_='集中作业中心主管审批' then '集中作业中心主管审批'
when LOWER(task_key_)='usertask8' and task_name_='市场主管审批' then '市场主管审批'
when LOWER(task_key_)='usertask12' and task_name_='资金岗审批' then '资金岗审批'
when LOWER(task_key_)='usertask9' and task_name_='销管审批' then '销管审批'
ELSE
 task_name_
end task_name_new_
FROM
(select * from ods_bpms_bpm_check_opinion where SUP_INST_ID_='0') a
left join ods_bpms_biz_apply_order bao on (a.PROC_INST_ID_=bao.flow_instance_id )
LEFT JOIN ods_bpms_bpm_pro_inst t2 ON bao.flow_instance_id = t2.ID_;
drop table if exists ods_bpms_bpm_check_opinion_common_1;
ALTER TABLE odstmp_bpms_bpm_check_opinion_common_1 RENAME TO ods_bpms_bpm_check_opinion_common_1;