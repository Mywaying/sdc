use ods;
drop table if  exists ods.odstmp_bpms_biz_order_remark_common;
CREATE TABLE ods.odstmp_bpms_biz_order_remark_common (   id STRING,
   apply_no STRING COMMENT '订单号',
   content STRING COMMENT '备注内容',
   receiver_name STRING COMMENT '提醒人员姓名列表，逗号隔开',
   create_user_id STRING COMMENT '创建人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   content_tag STRING COMMENT '备注标签加急销急',
   rn bigint
)  STORED AS parquet;

insert overwrite table odstmp_bpms_biz_order_remark_common
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no,content_tag ORDER BY create_time asc) rn from
(
select
a.id,
a.apply_no,
a.content,
a.receiver_name,
a.create_user_id,
a.create_time,
case when instr(a.content,'加急')>0  then '加急'
when instr(a.content,'销急')>0  then '销急' else '其它'
end as content_tag
FROM ods_bpms_biz_order_remark a)T;
drop table if exists ods_bpms_biz_order_remark_common;
ALTER TABLE odstmp_bpms_biz_order_remark_common RENAME TO ods_bpms_biz_order_remark_common;