use ods;
drop table if exists  odstmp_bims_biz_order_matter_record_common;
CREATE TABLE if not exists ods.odstmp_bims_biz_order_matter_record_common (
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
 rn bigint);

insert overwrite table  odstmp_bims_biz_order_matter_record_common
select *,ROW_NUMBER() OVER(PARTITION BY apply_no,matter_key_new ORDER BY if(handle_time='',create_time,handle_time) asc)
rn from (SELECT
id,
apply_no,
house_no,
create_user_id,
update_user_id,
create_time,
update_time,
rev,
delete_flag,
matter_key,
matter_name,
lower(matter_key)  matter_key_new,
case
when LOWER(matter_key)='pushloancommand' and matter_name='放款指令推送' then '出账指令推送'
when LOWER(matter_key)='sendloancommand' and matter_name='发送放款指令' then '出账审核'
when LOWER(matter_key)='sendloancommand' and matter_name='发送放款指令' then '出账审核'
when LOWER(matter_key)='householdapprove' and matter_name='下户' then '下户审批'
else matter_name end matter_name_new,
relate_type,
relate_id,
status_,
read_,
handle_time,
handle_user_id,
handle_user_name,
assignee_id_,
accept_time,
due_time,
remark,
data_,
from_key,
from_name,
operator_location,
operator_address,
operator_device,
next_matter_key,
next_handle_time,
node_remark
FROM
ods_bims_biz_order_matter_record a)T;
drop table if exists ods_bims_biz_order_matter_record_common;
ALTER TABLE odstmp_bims_biz_order_matter_record_common RENAME TO ods_bims_biz_order_matter_record_common;