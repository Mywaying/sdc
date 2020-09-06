use ods;
drop table if  exists ods.odstmp_bpms_biz_appoint_info_common;
CREATE TABLE ods.odstmp_bpms_biz_appoint_info_common (   id_ STRING COMMENT '主键id',
   apply_no STRING COMMENT '订单编号',
   matter_name STRING COMMENT '事项名称',
   matter_key STRING COMMENT '事项key',
   appoint_time TIMESTAMP COMMENT '预约时间',
   appoint_end_time TIMESTAMP,
   appoint_user_name STRING COMMENT '预约人员名称',
   appoint_user_id STRING COMMENT '预约人员id',
   appoint_area_name STRING COMMENT '预约地址所在区域',
   appoint_address STRING COMMENT '预约地址',
   remark STRING COMMENT '备注',
   create_user_id STRING COMMENT '创建人',
   update_user_id STRING COMMENT '更新人',
   create_time TIMESTAMP COMMENT '创建时间',
   update_time TIMESTAMP COMMENT '更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '删除标识',
   urgent_weight bigint comment '加急权重（0表示不紧急）',
   is_nearly STRING COMMENT '是否最近预约信息（0：否，1：是）',
   lock_user_id STRING COMMENT '锁定人员编号',
   lock_user_name STRING COMMENT '锁定人员名称',
   rn bigint
)  STORED AS parquet;
insert overwrite table odstmp_bpms_biz_appoint_info_common
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY is_nearly desc) rn from
(
select
a.id_,
a.apply_no,
a.matter_name,
a.matter_key,
a.appoint_time,
a.appoint_end_time,
a.appoint_user_name,
a.appoint_user_id,
a.appoint_area_name,
a.appoint_address,
a.remark,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
a.urgent_weight,
a.is_nearly,
a.lock_user_id,
a.lock_user_name
FROM ods_bpms_biz_appoint_info a)T;
drop table if exists ods_bpms_biz_appoint_info_common;
ALTER TABLE odstmp_bpms_biz_appoint_info_common RENAME TO ods_bpms_biz_appoint_info_common;