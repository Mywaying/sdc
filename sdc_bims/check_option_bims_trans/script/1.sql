with check_option_true as (
  select *
  from ods_bpms_bpm_check_opinion_common where STATUS_NAME_<>'人工终止'
),
check_option_transitional as (
  select *
  from ods_bpms_bpm_check_opinion_common where STATUS_NAME_<>'人工终止'
),
check_flow as (
select *,
CASE
when LOWER(flow_type)='bldy_type'  then 'mortgagepass'
when LOWER(flow_type)='bldy_zz_type'  then 'mortgagepass_zz'
when LOWER(flow_type)='cd_type'  then 'queryarchive'
when LOWER(flow_type)='dycj_type'  then 'mortgageout'
when LOWER(flow_type)='dydj_type'  then 'mortgagepass'
when LOWER(flow_type)='fydj_type'  then 'costmark'
when LOWER(flow_type)='ghcj_type'  then 'transferout'
when LOWER(flow_type)='ghdj_type'  then 'transferin'
when LOWER(flow_type)='gh_type'  then 'transferin'
when LOWER(flow_type)='gh_zz_type'  then 'transferin_zz'
when LOWER(flow_type)='gz_type'  then 'notarization'
when LOWER(flow_type)='hstdxx_type'  then 'hstdxx_type'
when LOWER(flow_type)='hstdxx_type'  then 'agreeloanmark'
when LOWER(flow_type)='mq_type'  then 'interview'
when LOWER(flow_type)='qxz_type'  then 'qxz_type'
when LOWER(flow_type)='qzjzxcl_type'  then 'qzjzxcl_type'
when LOWER(flow_type)='sldj_type'  then 'randommark'
when LOWER(flow_type)='sqaj_type'  then 'sqaj_type'
when LOWER(flow_type)='sqdk_type'  then 'applyloan'
when LOWER(flow_type)='tdxxdj_type'  then 'agreeloanmark'
when LOWER(flow_type)='yjtg_type'  then 'trustaccount'
when LOWER(flow_type)='yysl_type'  then 'prerandom'
when LOWER(flow_type)='zhcs_type'  then 'accounttest'
when LOWER(flow_type)='zhtg_type'  then 'zhtg_type'
when LOWER(flow_type)='zxdy_type'  then 'canclemortgage'
when LOWER(flow_type)='zxdy_zz_type'  then 'canclemortgage_zz'
ELSE
flow_type
end flow_type_new_,
d.NAME_ flow_type_name,
CASE
when LOWER(flow_type)='bldy_type'  then '抵押递件'
when LOWER(flow_type)='bldy_zz_type'  then '抵押递件_郑州'
when LOWER(flow_type)='cd_type'  then '查档'
when LOWER(flow_type)='dycj_type'  then '抵押出件'
when LOWER(flow_type)='dydj_type'  then '抵押递件'
when LOWER(flow_type)='fydj_type'  then '费率登记/费用登记'
when LOWER(flow_type)='ghcj_type'  then '过户出件'
when LOWER(flow_type)='ghdj_type'  then '过户递件'
when LOWER(flow_type)='gh_type'  then '过户递件'
when LOWER(flow_type)='gh_zz_type'  then '过户递件_郑州'
when LOWER(flow_type)='gz_type'  then '办理公证'
when LOWER(flow_type)='hstdxx_type'  then '回款来源登记'
when LOWER(flow_type)='hstdxx_type'  then '同贷信息登记/同贷登记'
when LOWER(flow_type)='mq_type'  then '面签'
when LOWER(flow_type)='qxz_type'  then '取新证'
when LOWER(flow_type)='qzjzxcl_type'  then '取证及注销材料'
when LOWER(flow_type)='sldj_type'  then '赎楼登记'
when LOWER(flow_type)='sqaj_type'  then '申请按揭'
when LOWER(flow_type)='sqdk_type'  then '申请贷款'
when LOWER(flow_type)='tdxxdj_type'  then '同贷信息登记/同贷登记'
when LOWER(flow_type)='yjtg_type'  then '要件托管'
when LOWER(flow_type)='yysl_type'  then '预约赎楼'
when LOWER(flow_type)='zhcs_type'  then '账户测试'
when LOWER(flow_type)='zhtg_type'  then '账户托管'
when LOWER(flow_type)='zxdy_type'  then '注销抵押'
when LOWER(flow_type)='zxdy_zz_type'  then '注销抵押_郑州'
ELSE
''
end flow_type_name_new
from ods_bpms_biz_order_flow bc
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bpms_sys_dic_common where TYPE_ID_='10000030350009' or TYPE_ID_='10000041100002'  )d
    on d.KEY_=lower(bc.`flow_type`)
)
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
b.status_,
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
b.flow_type_name_new task_name_new_
from check_flow b join (
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
		WHERE
			product_id IN (
				'DDKD_NJY_SER',
				'SSD_NJY_SER',
				'DDKD_NSL_NJY_SER',
				'DDKD_NJY_OTH_CPGD',
				'XSD_YJY_OTH',
				'PMB_YJY_ISR',
				'BXFW_NSL_NJY_ISR',
        'JSD_NSL_NJY_SER'
			)
)bao on bao.apply_no=b.apply_no
LEFT JOIN ods_bpms_bpm_pro_inst t2 ON bao.flow_instance_id = t2.ID_
where (b.handle_time is not null or b.handle_time <>'')
and LOWER(b.flow_key_new) not in(select lower(TASK_KEY_) from check_option_true a where a.apply_no=b.apply_no);