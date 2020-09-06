use ods;
drop table if  exists ods.odstmp_bpms_biz_write_off_detail_common;
CREATE TABLE ods.odstmp_bpms_biz_write_off_detail_common (
   id STRING,
   write_no STRING COMMENT '核销编号',
   apply_no STRING COMMENT '业务编号',
   cost_id STRING COMMENT '交易流水号',
   trans_type STRING COMMENT '交易类型',
   fee_type STRING COMMENT '费用类型',
   fee_amount DOUBLE COMMENT '交易金额',
   remark STRING COMMENT '备注',
   create_time TIMESTAMP COMMENT '创建时间',
   create_user_id STRING COMMENT '创建人',
   update_time TIMESTAMP COMMENT '更新时间',
   update_user_id STRING COMMENT '更新人',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '删除标识',
   trans_type_name STRING COMMENT '交易类型名',
   fee_type_name STRING COMMENT '费用类型名'
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_write_off_detail_common
SELECT
bowd.id
,bowd.write_no
,bowd.apply_no
,bowd.cost_id
,bowd.trans_type
,bowd.fee_type
,bowd.fee_amount
,bowd.remark
,bowd.create_time
,bowd.create_user_id
,bowd.update_time
,bowd.update_user_id
,bowd.rev
,bowd.delete_flag
,bcd.tran_type_desc
,bcid.split_item_name
FROM
ods_bpms_biz_write_off_detail bowd
left join ods_bpms_c_tran_def bcd on bcd.tran_type=bowd.trans_type
left join ods_bpms_c_split_item_def bcid on bcid.trans_type_key=bowd.trans_type and bcid.split_item_key=bowd.fee_type;
drop table if exists ods_bpms_biz_write_off_detail_common;
ALTER TABLE odstmp_bpms_biz_write_off_detail_common RENAME TO ods_bpms_biz_write_off_detail_common;