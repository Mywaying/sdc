use ods;
drop table if exists  odstmp_bpms_biz_order_matter_record_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_order_matter_record_common (
  id STRING,
  apply_no STRING,
  house_no STRING,
  create_user_id STRING,
  update_user_id STRING,
  create_time TIMESTAMP,
  update_time TIMESTAMP,
  rev bigint,
  delete_flag STRING,
  matter_key STRING,
  matter_name STRING,
  matter_key_new STRING,
  matter_name_new STRING,
  relate_type STRING,
  relate_id STRING,
  status_ STRING,
  read_ STRING,
  handle_time TIMESTAMP,
  handle_user_id STRING,
  handle_user_name STRING,
  assignee_id_ STRING,
  accept_time TIMESTAMP,
  due_time TIMESTAMP,
  remark STRING,
  data_ STRING,
  from_key STRING,
  from_name STRING,
  operator_location STRING,
  operator_address STRING,
  operator_device STRING,
  next_matter_key STRING,
  next_handle_time TIMESTAMP,
  node_remark STRING,
  is_outside_handle STRING,
 rn bigint)stored as parquet;

insert overwrite table  odstmp_bpms_biz_order_matter_record_common
select *,ROW_NUMBER() OVER(PARTITION BY apply_no,matter_key_new ORDER BY if(handle_time='',create_time,handle_time) asc)
rn from (SELECT
id,
apply_no,
house_no,
create_user_id,
update_user_id,
create_time,
update_time,
rev,
delete_flag,
matter_key,
matter_name,
CASE
when LOWER(matter_key)='householdapprove' and matter_name='下户审批' then 'householdapprove'
when LOWER(matter_key)='householdsurvey' and matter_name='下户调查' then 'householdsurvey'
when LOWER(matter_key)='downhousesurvey'  then 'householdsurvey'
when LOWER(matter_key)='usertask3' and matter_name='业务报单' then 'applycheck'
when LOWER(matter_key)='applyorder' and matter_name='业务申请' then 'applyorder'
when LOWER(matter_key)='usertask1' and matter_name='业务申请' then 'applyorder'
when LOWER(matter_key)='mancheck' and matter_name='人工审批' then 'mancheck'
when LOWER(matter_key)='usertask8' and matter_name='人工审核' then 'mancheck2'
when LOWER(matter_key)='insuranceinfoconfirm' and matter_name='保单信息确认' then 'insuranceinfoconfirm'
when LOWER(matter_key)='usertask8' and matter_name='保单确认' then 'insuranceinfoconfirm'
when LOWER(matter_key)='usertask9' and matter_name='保单确认' then 'insuranceinfoconfirm'
when LOWER(matter_key)='internalturnfunds' and matter_name='内部转款-归还' then 'internalturnFunds'
when LOWER(matter_key)='policyapply' and matter_name='出保单申请' then 'policyapply'
when LOWER(matter_key)='billcheck' and matter_name='出账前审查' then 'billcheck'
when LOWER(matter_key)='notarization' and matter_name='办理公证' then 'notarization'
when LOWER(matter_key)='mortgagepass' and matter_name='办理抵押' then 'mortgagepass'
when LOWER(matter_key)='startnode' and matter_name='发起节点' then 'startnode'
when LOWER(matter_key)='sendloancommand' and matter_name='发送放款指令' then 'sendloancommand'
when LOWER(matter_key)='sendloancommand1' and matter_name='发送放款指令' then 'sendloancommand'
when LOWER(matter_key)='usertask5' and matter_name='发送放款指令' then 'sendloancommand'
when LOWER(matter_key)='sendloanrelesecommand' and matter_name='发送款项释放指令' then 'sendloanrelesecommand'
when LOWER(matter_key)='usertask5' and matter_name='发送贷款指令' then 'sendloancommand'
when LOWER(matter_key)='agreeloanmark' then 'agreeloanmark'
when LOWER(matter_key)='agreeloanmark_atone' and matter_name='同贷信息登记(赎楼贷款)' then 'agreeloanmark_atone'
when LOWER(matter_key)='agreeloanmark' and matter_name='同贷登记' then 'agreeloanmark'
when LOWER(matter_key)='pushoutcommand' and matter_name='外传指令推送' then 'pushoutcommand'
when LOWER(matter_key)='patch_ds' and matter_name='大数补件' then 'patch_ds'
when LOWER(matter_key)='mancheck' and matter_name='审批' then 'mancheck'
when LOWER(matter_key)='usertask4' and matter_name='审批' then 'mancheck'
when LOWER(matter_key)='checkresensure' and matter_name='审批结果确定' then 'checkresensure'
when LOWER(matter_key)='approvalresult' and matter_name='审批结果确认' then 'approvalresult'
when LOWER(matter_key)='checkmarsend' and matter_name='审批资料外传' then 'checkmarsend'
when LOWER(matter_key)='usertask7' and matter_name='审批资料外传' then 'checkmarsend'
when LOWER(matter_key)='checkmarpush' and matter_name='审批资料推送' then 'checkmarpush'
when LOWER(matter_key)='investigate' and matter_name='审查' then 'investigate'
when LOWER(matter_key)='usertask3' and matter_name='审查' then 'investigate'
when LOWER(matter_key)='transfertail' and matter_name='尾款划拨申请' then 'transfertail'
when LOWER(matter_key)='confirmtransfertail' and matter_name='尾款划拨确认' then 'confirmtransfertail'
when LOWER(matter_key)='usertask10' and matter_name='尾款释放' then 'transfertailrelese'
when LOWER(matter_key)='usertask9' and matter_name='尾款释放' then 'transfertailrelese'
when LOWER(matter_key)='prefile' and matter_name='归档' then 'prefile'
when LOWER(matter_key)='usertask6' and matter_name='归档' then 'prefile'
when LOWER(matter_key)='inputinfo' and matter_name='录入' then 'inputinfo'
when LOWER(matter_key)='inputinfocomplete' and matter_name='录入完成' then 'inputinfocomplete'
when LOWER(matter_key)='creditconfirm' and matter_name='征信确认' then 'creditconfirm'
when LOWER(matter_key)='houseappraisal' and matter_name='房产评估' then 'houseappraisal'
when LOWER(matter_key)='usertask2' and matter_name='报单' then 'applycheck'
when LOWER(matter_key)='applycheck' and matter_name='报审' then 'applycheck'
when LOWER(matter_key)='applycheck' and matter_name='费项登记' then 'costitemmark'
when LOWER(matter_key)='mortgageout' then 'mortgageout'
when LOWER(matter_key)='mortgageout_zz' and matter_name='抵押出件_郑州' then 'mortgageout_zz'
when LOWER(matter_key)='mortgagepass' then 'mortgagepass'
when LOWER(matter_key)='mortgagepass_zz' and matter_name='抵押递件_郑州' then 'mortgagepass_zz'
when LOWER(matter_key)='submitverifyapply' and matter_name='提交核保申请' then 'submitverifyapply'
when LOWER(matter_key)='usertask5' and matter_name='放款' then 'sendloancommand'
when LOWER(matter_key)='pushloancommand' and matter_name='放款指令推送' then 'pushloancommand'
when LOWER(matter_key)='transfermark' and matter_name='放款登记' then 'transfermark'
when LOWER(matter_key)='houseinspection' and matter_name='查房' then 'houseinspection'
when LOWER(matter_key)='queryarchive' and matter_name='查档' then 'queryarchive'
when LOWER(matter_key)='sendverifymaterial' and matter_name='核保资料外传' then 'sendverifymaterial'
when LOWER(matter_key)='usertask7' and matter_name='核保资料外传' then 'sendverifymaterial'
when LOWER(matter_key)='canclemortgage' then 'canclemortgage'
when LOWER(matter_key)='canclemortgage_zz' and matter_name='注销抵押_郑州' then 'canclemortgage_zz'
when LOWER(matter_key)='applyloan' and matter_name='申请贷款' then 'applyloan'
when LOWER(matter_key)='applyloan_atone' and matter_name='申请贷款(赎楼贷款)' then 'applyloan_atone'
when LOWER(matter_key)='paymentarrival' and matter_name='确认回款资金到账' then 'paymentarrival'
when LOWER(matter_key)='platformarrival' and matter_name='确认平台款到账' then 'platformarrival'
when LOWER(matter_key)='confirmarrival' and matter_name='确认资金到账' then 'confirmarrival'
when LOWER(matter_key)='fundsarrival' and matter_name='确认资金到账' then 'confirmarrival'
when LOWER(matter_key)='confirmtailarrial' and matter_name='确认资金到账（尾款）' then 'confirmtailarrial'
when LOWER(matter_key)='usertask4' and matter_name='终审' then 'mancheck'
when LOWER(matter_key)='endnode' and matter_name='结束节点' then 'endnode'
when LOWER(matter_key)='costconfirm' and matter_name='缴费确认' then 'costconfirm'
when LOWER(matter_key)='trustaccount' and matter_name='要件托管' then 'trustaccount'
when LOWER(matter_key)='overinsurance' and matter_name='解保' then 'overinsurance'
when LOWER(matter_key)='usertask1' and matter_name='订单生成/业务申请' then 'applyorder'
when LOWER(matter_key)='financialarchiveevent' and matter_name='财务归档' then 'financialarchiveevent'
when LOWER(matter_key)='accounttest' and matter_name='账户测试' then 'accounttest'
when LOWER(matter_key)='finalloan' and matter_name='贷款上报终审' then 'finalloan'
when LOWER(matter_key)='costmark' then 'costmark'
when LOWER(matter_key)='costmark' and matter_name='费用登记' then 'costmark'
when LOWER(matter_key)='costitemmark' and matter_name='费项登记' then 'costitemmark'
when LOWER(matter_key)='uploadimg' and matter_name='资料反馈' then 'uploadimg'
when LOWER(matter_key)='uploadimg' and matter_name='资料反馈' then 'uploadimg'
when LOWER(matter_key)='usertask6' and matter_name='资料归档' then 'prefile'
when LOWER(matter_key)='datainspection' and matter_name='资料齐全' then 'datainspection'
when LOWER(matter_key)='' and matter_name='资金划拨申请' then 'transferapply'
when LOWER(matter_key)='transferapply' and matter_name='资金划拨申请' then 'transferapply'
when LOWER(matter_key)='usertask1' and matter_name='资金划拨申请' then 'transferapply'
when LOWER(matter_key)='' and matter_name='资金划拨确认' then 'transferconfirm'
when LOWER(matter_key)='transferconfirm' and matter_name='资金划拨确认' then 'transferconfirm'
when LOWER(matter_key)='usertask2' and matter_name='资金划拨确认' then 'transferconfirm'
when LOWER(matter_key)='' and matter_name='资金归还申请' then 'returnapply'
when LOWER(matter_key)='returnapply' and matter_name='资金归还申请' then 'returnapply'
when LOWER(matter_key)='usertask1' and matter_name='资金归还申请' then 'returnapply'
when LOWER(matter_key)='returnconfirm' and matter_name='资金归还确认' then 'returnconfirm'
when LOWER(matter_key)='usertask2' and matter_name='资金归还确认' then 'returnconfirm'
when LOWER(matter_key)='flooradvance' and matter_name='赎楼前垫资回款' then 'flooradvance'
when LOWER(matter_key)='randommark' and matter_name='赎楼登记' then 'randommark'
when LOWER(matter_key)='randomcashback' and matter_name='赎楼资金划回' then 'randomcashback'
when LOWER(matter_key)='randomreback' and matter_name='赎楼资金划回' then 'randomcashback'
when LOWER(matter_key)='transferout' then 'transferout'
when LOWER(matter_key)='transferout_zz' and matter_name='过户出件_郑州' then 'transferout_zz'
when LOWER(matter_key)='transferin' then 'transferin'
when LOWER(matter_key)='transferin_zz' and matter_name='过户递件_郑州' then 'transferin_zz'
when LOWER(matter_key)='repaymentconfirm' and matter_name='还款确认' then 'repaymentconfirm'
when LOWER(matter_key)='usertask5' and matter_name='通知机构放款' then 'sendloancommand'
when LOWER(matter_key)='usertask8' and matter_name='陆金所资金归还' then 'lureturnapply'
when LOWER(matter_key)='interview' and matter_name='面签' then 'interview'
when LOWER(matter_key)='usertask2' and matter_name='面签' then 'interview'
when LOWER(matter_key)='usertask10' and matter_name='预审批' then 'precheck'
when LOWER(matter_key)='usertask9' and matter_name='预审批' then 'precheck'
when LOWER(matter_key)='prerandom' then 'prerandom'
when LOWER(matter_key)='appointinterview' and matter_name='预约面签' then 'appointinterview'
when LOWER(matter_key)='getcancelmaterial' then 'getcancelmaterial'
when LOWER(matter_key)='custreceivableconfirm' then 'custreceivableconfirm'
when LOWER(matter_key)='other' then 'other'
when LOWER(matter_key)='principaltransfer' then 'principaltransfer'
when LOWER(matter_key)='principaltransferconfirm' then 'principaltransferconfirm'
when LOWER(matter_key)='verifyinsurance' then 'verifyinsurance'
when LOWER(matter_key)='agreeloanresult' then 'agreeloanresult'
ELSE
matter_key
end matter_key_new,
case
 when LOWER(matter_key)='householdapprove' and matter_name='下户审批' then '下户审批'
when LOWER(matter_key)='householdsurvey' and matter_name='下户调查' then '下户调查/下户'
when LOWER(matter_key)='downhousesurvey'  then '下户调查/下户'
when LOWER(matter_key)='usertask3' and matter_name='业务报单' then '报审/报单'
when LOWER(matter_key)='applyorder' and matter_name='业务申请' then '业务申请'
when LOWER(matter_key)='usertask1' and matter_name='业务申请' then '业务申请'
when LOWER(matter_key)='mancheck' and matter_name='人工审批' then '审批'
when LOWER(matter_key)='usertask8' and matter_name='人工审核' then '人工审核'
when LOWER(matter_key)='insuranceinfoconfirm' and matter_name='保单信息确认' then '保单信息确认'
when LOWER(matter_key)='usertask8' and matter_name='保单确认' then '保单确认'
when LOWER(matter_key)='usertask9' and matter_name='保单确认' then '保单确认'
when LOWER(matter_key)='internalturnfunds' and matter_name='内部转款-归还' then '内部转款-归还'
when LOWER(matter_key)='policyapply' and matter_name='出保单申请' then '出保单申请'
when LOWER(matter_key)='billcheck' and matter_name='出账前审查' then '出账前审查'
when LOWER(matter_key)='notarization' and matter_name='办理公证' then '办理公证'
when LOWER(matter_key)='mortgagepass' then '抵押递件'
when LOWER(matter_key)='startnode' and matter_name='发起节点' then '发起节点'
when LOWER(matter_key)='sendloancommand' and matter_name='发送放款指令' then '发送放款指令'
when LOWER(matter_key)='sendloancommand1' and matter_name='发送放款指令' then '发送放款指令'
when LOWER(matter_key)='usertask5' and matter_name='发送放款指令' then '发送放款指令'
when LOWER(matter_key)='sendloanrelesecommand' and matter_name='发送款项释放指令' then '发送款项释放指令'
when LOWER(matter_key)='usertask5' and matter_name='发送贷款指令' then '发送放款指令'
when LOWER(matter_key)='agreeloanmark' then '同贷信息登记/同贷登记'
when LOWER(matter_key)='agreeloanmark_atone' and matter_name='同贷信息登记(赎楼贷款)' then '同贷信息登记(赎楼贷款)'
when LOWER(matter_key)='agreeloanmark' and matter_name='同贷登记' then '同贷信息登记/同贷登记'
when LOWER(matter_key)='pushoutcommand' and matter_name='外传指令推送' then '外传指令推送/审批资料推送'
when LOWER(matter_key)='patch_ds' and matter_name='大数补件' then '大数补件'
when LOWER(matter_key)='mancheck' and matter_name='审批' then '审批'
when LOWER(matter_key)='usertask4' and matter_name='审批' then '审批'
when LOWER(matter_key)='checkresensure' and matter_name='审批结果确定' then '审批结果确定/审批结果确认'
when LOWER(matter_key)='approvalresult' and matter_name='审批结果确认' then '审批结果确定/审批结果确认'
when LOWER(matter_key)='checkmarsend' and matter_name='审批资料外传' then '审批资料外传'
when LOWER(matter_key)='usertask7' and matter_name='审批资料外传' then '审批资料外传'
when LOWER(matter_key)='checkmarpush' and matter_name='审批资料推送' then '外传指令推送/审批资料推送'
when LOWER(matter_key)='investigate' and matter_name='审查' then '审查'
when LOWER(matter_key)='usertask3' and matter_name='审查' then '审查'
when LOWER(matter_key)='transfertail' and matter_name='尾款划拨申请' then '尾款划拨申请'
when LOWER(matter_key)='confirmtransfertail' and matter_name='尾款划拨确认' then '尾款划拨确认'
when LOWER(matter_key)='usertask10' and matter_name='尾款释放' then '尾款释放'
when LOWER(matter_key)='usertask9' and matter_name='尾款释放' then '尾款释放'
when LOWER(matter_key)='prefile' and matter_name='归档' then '归档'
when LOWER(matter_key)='usertask6' and matter_name='归档' then '归档'
when LOWER(matter_key)='inputinfo' and matter_name='录入' then '录入'
when LOWER(matter_key)='inputinfocomplete' and matter_name='录入完成' then '录入完成'
when LOWER(matter_key)='creditconfirm' and matter_name='征信确认' then '征信确认'
when LOWER(matter_key)='houseappraisal' and matter_name='房产评估' then '房产评估'
when LOWER(matter_key)='usertask2' and matter_name='报单' then '报审/报单'
when LOWER(matter_key)='applycheck' and matter_name='报审' then '报审/报单'
when LOWER(matter_key)='applycheck' and matter_name='费项登记' then 'costitemmark'
when LOWER(matter_key)='mortgageout' then '抵押出件'
when LOWER(matter_key)='mortgageout_zz' and matter_name='抵押出件_郑州' then '抵押出件_郑州'
when LOWER(matter_key)='mortgagepass' and matter_name='抵押递件' then '抵押递件'
when LOWER(matter_key)='mortgagepass_zz' and matter_name='抵押递件_郑州' then '抵押递件_郑州'
when LOWER(matter_key)='submitverifyapply' and matter_name='提交核保申请' then '提交核保申请'
when LOWER(matter_key)='usertask5' and matter_name='放款' then '发送放款指令'
when LOWER(matter_key)='pushloancommand' and matter_name='放款指令推送' then '放款指令推送'
when LOWER(matter_key)='transfermark' and matter_name='放款登记' then '放款登记'
when LOWER(matter_key)='houseinspection' and matter_name='查房' then '查房'
when LOWER(matter_key)='queryarchive' and matter_name='查档' then '查档'
when LOWER(matter_key)='sendverifymaterial' and matter_name='核保资料外传' then '核保资料外传'
when LOWER(matter_key)='usertask7' and matter_name='核保资料外传' then '核保资料外传'
when LOWER(matter_key)='canclemortgage' then '注销抵押'
when LOWER(matter_key)='canclemortgage_zz' and matter_name='注销抵押_郑州' then '注销抵押_郑州'
when LOWER(matter_key)='applyloan' and matter_name='申请贷款' then '申请贷款'
when LOWER(matter_key)='applyloan_atone' and matter_name='申请贷款(赎楼贷款)' then '申请贷款(赎楼贷款)'
when LOWER(matter_key)='paymentarrival' and matter_name='确认回款资金到账' then '确认回款资金到账'
when LOWER(matter_key)='platformarrival' and matter_name='确认平台款到账' then '确认平台款到账'
when LOWER(matter_key)='confirmarrival' and matter_name='确认资金到账' then '确认资金到账'
when LOWER(matter_key)='fundsarrival' and matter_name='确认资金到账' then '确认资金到账'
when LOWER(matter_key)='confirmtailarrial' and matter_name='确认资金到账（尾款）' then '确认资金到账(尾款)'
when LOWER(matter_key)='usertask4' and matter_name='终审' then '审批'
when LOWER(matter_key)='endnode' and matter_name='结束节点' then '结束节点'
when LOWER(matter_key)='costconfirm' and matter_name='缴费确认' then '缴费确认'
when LOWER(matter_key)='trustaccount' and matter_name='要件托管' then '要件托管'
when LOWER(matter_key)='overinsurance' and matter_name='解保' then '解保'
when LOWER(matter_key)='usertask1' and matter_name='订单生成/业务申请' then '业务申请'
when LOWER(matter_key)='financialarchiveevent' and matter_name='财务归档' then '财务归档'
when LOWER(matter_key)='accounttest' and matter_name='账户测试' then '账户测试'
when LOWER(matter_key)='finalloan' and matter_name='贷款上报终审' then '贷款上报终审'
when LOWER(matter_key)='costmark' then '费率登记/费用登记'
when LOWER(matter_key)='costmark' and matter_name='费用登记' then '费率登记/费用登记'
when LOWER(matter_key)='costitemmark' and matter_name='费项登记' then '费项登记'
when LOWER(matter_key)='uploadimg' and matter_name='资料反馈' then '资料反馈'
when LOWER(matter_key)='usertask6' and matter_name='资料归档' then '归档'
when LOWER(matter_key)='datainspection' and matter_name='资料齐全' then '资料齐全'
when LOWER(matter_key)='' and matter_name='资金划拨申请' then '资金划拨申请'
when LOWER(matter_key)='transferapply' and matter_name='资金划拨申请' then '资金划拨申请'
when LOWER(matter_key)='usertask1' and matter_name='资金划拨申请' then '资金划拨申请'
when LOWER(matter_key)='' and matter_name='资金划拨确认' then '资金划拨确认'
when LOWER(matter_key)='transferconfirm' and matter_name='资金划拨确认' then '资金划拨确认'
when LOWER(matter_key)='usertask2' and matter_name='资金划拨确认' then '资金划拨确认'
when LOWER(matter_key)='' and matter_name='资金归还申请' then '资金归还申请'
when LOWER(matter_key)='returnapply' and matter_name='资金归还申请' then '资金归还申请'
when LOWER(matter_key)='usertask1' and matter_name='资金归还申请' then '资金归还申请'
when LOWER(matter_key)='returnconfirm' and matter_name='资金归还确认' then '资金归还确认'
when LOWER(matter_key)='usertask2' and matter_name='资金归还确认' then '资金归还确认'
when LOWER(matter_key)='flooradvance' and matter_name='赎楼前垫资回款' then '赎楼前垫资回款'
when LOWER(matter_key)='randommark' and matter_name='赎楼登记' then '赎楼登记'
when LOWER(matter_key)='randomcashback' and matter_name='赎楼资金划回' then '赎楼资金划回'
when LOWER(matter_key)='randomreback' and matter_name='赎楼资金划回' then '赎楼资金划回'
when LOWER(matter_key)='transferout' then '过户出件'
when LOWER(matter_key)='transferout_zz' and matter_name='过户出件_郑州' then '过户出件_郑州'
when LOWER(matter_key)='transferin' then '过户递件'
when LOWER(matter_key)='transferin_zz' and matter_name='过户递件_郑州' then '过户递件_郑州'
when LOWER(matter_key)='repaymentconfirm' and matter_name='还款确认' then '还款确认'
when LOWER(matter_key)='usertask5' and matter_name='通知机构放款' then '发送放款指令'
when LOWER(matter_key)='usertask8' and matter_name='陆金所资金归还' then '陆金所资金归还'
when LOWER(matter_key)='interview' and matter_name='面签' then '面签'
when LOWER(matter_key)='usertask2' and matter_name='面签' then '面签'
when LOWER(matter_key)='usertask10' and matter_name='预审批' then '预审批'
when LOWER(matter_key)='usertask9' and matter_name='预审批' then '预审批'
when LOWER(matter_key)='prerandom' then '预约赎楼'
when LOWER(matter_key)='appointinterview' and matter_name='预约面签' then '预约面签'
when LOWER(matter_key)='getcancelmaterial' then '领取注销资料'
when LOWER(matter_key)='custreceivableconfirm' then '客户回款确认'
when LOWER(matter_key)='other' then '其他'
when LOWER(matter_key)='principaltransfer' then '本息划拨申请'
when LOWER(matter_key)='principaltransferconfirm' then '本息划拨确认'
when LOWER(matter_key)='verifyinsurance' then '核保'
when LOWER(matter_key)='agreeloanresult' then '确认银行放款'
ELSE
 matter_name
end matter_name_new,
relate_type,
relate_id,
status_,
read_,
handle_time,
handle_user_id,
handle_user_name,
assignee_id_,
accept_time,
due_time,
remark,
data_,
from_key,
from_name,
operator_location,
operator_address,
operator_device,
next_matter_key,
next_handle_time,
node_remark,
is_outside_handle
FROM
ods_bpms_biz_order_matter_record a)T;
drop table if exists ods_bpms_biz_order_matter_record_common;
ALTER TABLE odstmp_bpms_biz_order_matter_record_common RENAME TO ods_bpms_biz_order_matter_record_common;