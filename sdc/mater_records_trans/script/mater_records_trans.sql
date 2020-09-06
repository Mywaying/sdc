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
  rn bigint,
  status_option STRING,
  status_name_option STRING,
  status_name_ STRING,
  is_outside_handle_name STRING
) stored as parquet;
insert overwrite table odstmp_bpms_biz_order_matter_record_common
select a.*,b.status_,b.status_name_,
(CASE WHEN a.STATUS_="NORMAL" THEN      "同意"
 WHEN a.STATUS_="agree" THEN           "同意"
 WHEN a.STATUS_="reject" THEN "驳回"
 ELSE NULL END ),
(CASE WHEN a.is_outside_handle="N" THEN      "否"
 WHEN a.is_outside_handle="Y" THEN           "是"
 ELSE NULL END )
from ods_bpms_biz_order_matter_record_common a
left join odstmp_bpms_bpm_check_opinion_common_complete b on b.apply_no=a.apply_no and b.task_key_new_=a.matter_key_new and b.rn=a.rn and from_timestamp(b.complete_time_,'yyyyMMddmm')=from_timestamp(a.handle_time,'yyyyMMddmm');

drop table if exists ods_bpms_biz_order_matter_record_common;
ALTER TABLE odstmp_bpms_biz_order_matter_record_common RENAME TO ods_bpms_biz_order_matter_record_common;