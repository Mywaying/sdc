use ods;
drop table if  exists ods.odstmp_bpms_biz_order_flow_common;
CREATE TABLE ods.odstmp_bpms_biz_order_flow_common (
id STRING COMMENT '主键id',
apply_no STRING COMMENT '订单编号',
flow_type STRING COMMENT '流转类型',
flow_type_name STRING COMMENT '流转类型名称',
handle_time TIMESTAMP COMMENT '办理时间',
handle_user_id STRING COMMENT '办理人id',
handle_user_name STRING COMMENT '办理人姓名',
remark STRING COMMENT '备注',
create_user_id STRING COMMENT '创建人id',
update_user_id STRING COMMENT '更新人id',
create_time TIMESTAMP COMMENT '记录创建时间',
update_time TIMESTAMP COMMENT '记录更新时间',
rev bigint comment '版本号',
delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
flow_type_new STRING COMMENT '新流转类型',
flow_type_name_new STRING COMMENT '新流转类型名称',
rn bigint)  STORED AS parquet;
insert overwrite table odstmp_bpms_biz_order_flow_common
select *,ROW_NUMBER() OVER(PARTITION BY apply_no,flow_type_new ORDER BY if(handle_time='',create_time,handle_time) asc) rn from (
select
a.id,
a.apply_no,
a.flow_type,
b.NAME_ flow_type_name,
a.handle_time,
a.handle_user_id,
a.handle_user_name,
a.remark,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
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
when LOWER(flow_type)='slzjhh_pc'  then 'randomcashback'
when LOWER(flow_type)='tdsdj_pc'  then 'agreeloanmark'
when LOWER(flow_type)='jb_pc'  then 'overinsurance'
when LOWER(flow_type)='wksf_pc'  then 'transfertailrelese'
when LOWER(flow_type)='qdzjdz_pc'  then 'confirmarrival'
when LOWER(flow_type)='agreeloanresult'  then 'agreeloanresult'
ELSE
flow_type
end flow_type_new,
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
when LOWER(flow_type)='slzjhh_pc'  then '赎楼资金划回'
when LOWER(flow_type)='tdsdj_pc'  then '同贷登记'
when LOWER(flow_type)='jb_pc'  then '解保'
when LOWER(flow_type)='wksf_pc'  then '尾款释放'
when LOWER(flow_type)='qdzjdz_pc'  then '确认资金到账'
when LOWER(flow_type)='agreeloanresult'  then '确认银行放款'

ELSE
b.NAME_
end flow_type_name_new
FROM ods_bpms_biz_order_flow a
left join (
select DISTINCT KEY_,NAME_new_ NAME_ from ods_bpms_sys_dic_common where TYPE_ID_='10000030350009' or TYPE_ID_='10000041100002' or TYPE_ID_='10000033420071')b
on b.KEY_=lower(a.flow_type))T;
drop table if exists ods_bpms_biz_order_flow_common;
ALTER TABLE odstmp_bpms_biz_order_flow_common RENAME TO ods_bpms_biz_order_flow_common;