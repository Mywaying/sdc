drop table if exists dim.dim_company;
create table dim.dim_company (
	company_name_3_level string comment "三级公司",
	company_id_3_level string comment "三级公司id",
	company_name_2_level string comment "二级公司",
	company_id_2_level string comment "二级公司id",
	company_name_1_level string comment "一级公司",
	company_id_1_level string comment "一级公司id",
	s_key bigint comment "代理键",
	etl_update_time timestamp
);
insert overwrite table dim.dim_company
select 
a.*
,row_number() over (order by a.company_name_3_level, a.company_id_3_level) s_key
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from ( 
SELECT 
org1.NAME_ company_name_3_level
,org1.CODE_ company_id_3_level
,org2.NAME_ company_name_2_level
,org2.CODE_ company_id_2_level
,org3.NAME_ company_name_1_level
,org3.CODE_ company_id_1_level
FROM ods.ods_bpms_sys_org org1
left join ods.ods_bpms_sys_org org2 on org1.PARENT_ID_ = org2.ID_
left join ods.ods_bpms_sys_org org3 on org2.PARENT_ID_ = org3.ID_
where org1.GRADE_ = '3'

union all

SELECT 
org1.NAME_ company_name_3_level
,org1.CODE_ company_id_3_level
,org1.NAME_ company_name_2_level
,org1.CODE_ company_id_2_level
,org2.NAME_ company_name_1_level
,org2.CODE_ company_id_1_level
FROM ods.ods_bpms_sys_org org1
left join ods.ods_bpms_sys_org org2 on org1.PARENT_ID_ = org2.ID_
where org1.GRADE_ = '2'
) as a;