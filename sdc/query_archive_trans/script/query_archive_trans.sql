use ods;
drop table if exists  odstmp_bpms_biz_query_archive;
CREATE TABLE ods.odstmp_bpms_biz_query_archive (
   id STRING COMMENT '主键id',
   apply_no STRING COMMENT '订单编号',
   house_no STRING COMMENT '房产信息编号关联房产信息',
   cert_no STRING COMMENT '房产/土地证编号',
   cert_type STRING COMMENT '产证类型',
   cert_type_name STRING COMMENT '产证类型名称',
   query_time timestamp COMMENT '查询时间',
   query_result STRING COMMENT '查档结果',
   query_result_name STRING COMMENT '查档结果名称',
   query_user_id STRING COMMENT '查档人id',
   query_user_name STRING COMMENT '查档人姓名',
   remark STRING COMMENT '备注',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   update_time timestamp COMMENT '记录更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
   rn bigint
)STORED AS  parquet;
insert overwrite table odstmp_bpms_biz_query_archive
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time asc) rn from (
  SELECT
    a.id,
    a.apply_no,
    a.house_no,
    a.cert_no,
    a.cert_type,
    regexp_replace(regexp_replace(regexp_replace(a.cert_type, '03', '不动产权证书'), '01', '房屋所有权证'), '02',
                   '国有土地使用证')                                                                       cert_type_name,
    a.query_time,
    a.query_result,
    regexp_replace(regexp_replace(regexp_replace(a.query_result, '1', '有效'), '2', '抵押'), '3', '查封') query_result_name,
    a.query_user_id,
    a.query_user_name,
    a.remark,
    a.create_user_id,
    a.update_user_id,
    a.create_time,
    a.update_time,
    a.rev,
    a.delete_flag
  FROM
    ods_bpms_biz_query_archive a
  where a.apply_no is NOT NULL and a.apply_no <> ''
  UNION all
  select
    a.id,
    bao.apply_no,
    a.house_no,
    a.cert_no,
    a.cert_type,
    a.cert_type_name,
    a.query_time,
    a.query_result,
    a.query_result_name,
    a.query_user_id,
    a.query_user_name,
    a.remark,
    a.create_user_id,
    a.update_user_id,
    a.create_time,
    a.update_time,
    a.rev,
    a.delete_flag
  from
    (SELECT
       id,
       apply_no,
       house_no,
       cert_no,
       cert_type,
       regexp_replace(regexp_replace(regexp_replace(cert_type, '03', '不动产权证书'), '01', '房屋所有权证'), '02',
                      '国有土地使用证')                                                                     cert_type_name,
       query_time,
       query_result,
       regexp_replace(regexp_replace(regexp_replace(query_result, '1', '有效'), '2', '抵押'), '3', '查封') query_result_name,
       query_user_id,
       query_user_name,
       remark,
       create_user_id,
       update_user_id,
       create_time,
       update_time,
       rev,
       delete_flag
     FROM
       ods_bpms_biz_query_archive
     where house_no <> '' and house_no IS NOT NULL AND (apply_no = '' or apply_no is null)) a
    left join (select
                 apply_no,
                 house_no
               from ods_bpms_biz_apply_order
               where house_no <> '' and house_no is not null) bao on bao.house_no = a.house_no)T;
drop table if exists ods_bpms_biz_query_archive_common;
ALTER TABLE odstmp_bpms_biz_query_archive RENAME TO ods_bpms_biz_query_archive_common;