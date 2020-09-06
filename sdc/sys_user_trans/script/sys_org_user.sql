use ods;
drop table if exists ods.odstmp_bpms_sys_org_user_common;
CREATE TABLE ods.odstmp_bpms_sys_org_user_common (
  id_ STRING,
  org_id_ STRING,
  org_name_ STRING,
  org_type STRING,
  org_code STRING,
  user_id_ STRING,
  account_ STRING,
  fullname_ STRING,
  mobile_ STRING,
  id_card_no STRING,
  is_master_ bigint comment '0:非主部门1:主部门',
  rel_id_ STRING,
  user_post STRING,
  post_create_time TIMESTAMP,
  post_update_time TIMESTAMP,
  update_time TIMESTAMP COMMENT '更新时间',
  create_time TIMESTAMP COMMENT '创建时间',
  email_ STRING,
  weixin_ STRING,
  create_time_ TIMESTAMP,
  address_ STRING,
  photo_ STRING,
  sex_ STRING,
  status_ bigint
)
STORED AS parquet;

insert overwrite table odstmp_bpms_sys_org_user_common
select a.id_,
a.org_id_,
d.name_ org_name_,
d.org_type_ org_type,
d.code_ org_code,
a.user_id_,
b.account_,
b.fullname_,
b.mobile_,
b.id_card_no,
a.is_master_,
a.rel_id_,
c.rel_name_ user_post,
c.create_time post_create_time,
c.update_time post_update_time,
a.update_time,
a.create_time,
b.email_,
b.weixin_,
b.create_time_,
b.address_,
b.photo_,
b.sex_,
b.status_
from ods_bpms_sys_org_user a
left join ods_bpms_sys_user b on b.id_=a.user_id_
left join ods_bpms_sys_org_rel c on c.id_=a.rel_id_ 
left join ods_bpms_sys_org d on d.id_=a.org_id_;

drop table if exists ods_bpms_sys_org_user_common;
ALTER TABLE odstmp_bpms_sys_org_user_common RENAME TO ods_bpms_sys_org_user_common;