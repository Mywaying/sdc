use ods;
drop table if  exists ods.odstmp_bims_biz_query_docking_result_common;
CREATE TABLE ods.odstmp_bims_biz_query_docking_result_common (
   id STRING COMMENT '主键编号',
   apply_no STRING COMMENT '业务申请编号',
   bus_type STRING COMMENT '业务类型',
   node_key STRING COMMENT '流程节点key',
   req_id STRING COMMENT '请求查询ID',
   result STRING COMMENT '结果',
   `describe` STRING COMMENT '对接查询结果描述',
   remark STRING COMMENT '备注',
   `data` STRING COMMENT '数据，JSON存储',
   ext1 STRING COMMENT '扩展字段1',
   ext2 STRING COMMENT '扩展字段2',
   ext3 STRING COMMENT '扩展字段3',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   update_time TIMESTAMP COMMENT '记录更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
   unique_id STRING COMMENT '查询条件',
   bus_type_name STRING COMMENT '业务类型名',
   investigateflag STRING COMMENT '下户类型',
   investigateflag_name STRING COMMENT '下户类型名',
   rejectreason STRING COMMENT '风险返回的面签审批结果拒绝原因',
   risklevel STRING COMMENT '风险级别',
   calcreditamt STRING COMMENT '借款金额',
   rn bigint
   )  STORED AS parquet;
insert overwrite table odstmp_bims_biz_query_docking_result_common
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no,bus_type ORDER BY create_time asc) rn from (
select
a.`id`,
a.`apply_no`,
a.`bus_type`,
a.`node_key`,
a.`req_id`,
a.`result`,
a.`describe`,
a.`remark`,
regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),
a.`ext1`,
a.`ext2`,
a.`ext3`,
a.`create_user_id`,
a.`update_user_id`,
a.`create_time`,
a.`update_time`,
a.`rev`,
a.`delete_flag`,
a.`unique_id`,
b.`NAME_` bus_type_name,
case when bus_type='DY020' then
get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.investigateFlag')
else NULL end investigateflag,
case when bus_type='DY020' then
regexp_replace(regexp_replace(regexp_replace(regexp_replace(get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.investigateFlag'),'0','不下户'),'1','房产下户'),'2','经营下户'),'3','房产经营均下户')
else NULL end investigateflag_name,
case when bus_type='QUERY_INTERVIEW' then
get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'), '$.rejectReason')
else NULL end rejectreason,
case when bus_type='DY020' then
get_json_object(strleft(REPLACE(get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'$.productSelected'),'[',''),locate('}',REPLACE(get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'$.productSelected'),'[',''))),'$.riskLevel')
else NULL end risklevel,
case when bus_type='DY020' then
get_json_object(strleft(REPLACE(get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'$.productSelected'),'[',''),locate('}',REPLACE(get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(a.`data`,'""','"'),'"{"','{"'),'"}"','"}'),'}"','}'),'$.productSelected'),'[',''))),'$.calCreditAmt')
else NULL end calcreditamt
from ods_bims_biz_query_docking_result a
left join (
select DISTINCT KEY_,NAME_new_ NAME_ from ods_bims_sys_dic_common where type_id_='10000022881903' or type_id_='10000039360006' or type_id_='10000039360008' or type_id_='10000039360009' or type_id_='10000039360010' or type_id_='10000047750219' or type_id_='10000026360013' or type_id_='10000027530016' or type_id_='10000027530016' or type_id_='10000027530011' or type_id_='10000027530018' or type_id_='10000027530014' or type_id_='10000047750219' or type_id_='10000096080009' or type_id_='10000026360013')b
on b.KEY_=lower(a.bus_type))T;
drop table if exists ods_bims_biz_query_docking_result_common;
ALTER TABLE odstmp_bims_biz_query_docking_result_common RENAME TO ods_bims_biz_query_docking_result_common;