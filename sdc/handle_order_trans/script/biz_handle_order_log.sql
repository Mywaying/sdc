use ods;
drop table if  exists ods.odstmp_bpms_biz_handle_order_log_common;
CREATE TABLE ods.odstmp_bpms_biz_handle_order_log_common (   id STRING COMMENT 'id',
   appoint_id STRING COMMENT '预约信息id',
   operate_type STRING COMMENT '操作类型',
   apply_no STRING COMMENT '订单编号',
   matter_key STRING COMMENT '事项key',
   matter_name STRING COMMENT '事项名称',
   operate_user_id STRING COMMENT '操作人id',
   operate_user_name STRING COMMENT '操作人名称',
   target_user_id STRING COMMENT '目标人id',
   target_user_name STRING COMMENT '目标人名称',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '创建时间',
   update_time TIMESTAMP COMMENT '更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
   remark STRING COMMENT '备注信息',
   rn bigint
)  STORED AS parquet;

insert overwrite table odstmp_bpms_biz_handle_order_log_common
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no,operate_type ORDER BY create_time asc) rn from
(
select
a.id,
a.appoint_id,
a.operate_type,
a.apply_no,
a.matter_key,
a.matter_name,
a.operate_user_id,
a.operate_user_name,
a.target_user_id,
a.target_user_name,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
a.remark
FROM ods_bpms_biz_handle_order_log a)T;
drop table if exists ods_bpms_biz_handle_order_log_common;
ALTER TABLE odstmp_bpms_biz_handle_order_log_common RENAME TO ods_bpms_biz_handle_order_log_common;