use ods;

CREATE TABLE if not exists ods.odstmp_bims_sys_user_common (
  user_id STRING,
  org_id STRING,
  job_name STRING,
  org_code STRING,
  org_name STRING,
  fullname_ STRING COMMENT '姓名',
  account_ STRING COMMENT '账号',
  password_ STRING COMMENT '密码',
  email_ STRING COMMENT '邮箱',
  mobile_ STRING COMMENT '手机号码',
  weixin_ STRING COMMENT '微信号',
  create_time_ timestamp COMMENT '创建时间',
  address_ STRING COMMENT '地址',
  photo_ STRING COMMENT '头像',
  sex_ STRING COMMENT '性别：男，女，未知',
  from_ STRING COMMENT '来源',
  status_ bigint comment '0:禁用，1正常',
  update_time TIMESTAMP COMMENT '更新时间',
  id_card_no STRING COMMENT '身份证号' )
  STORED AS parquet;

insert overwrite table odstmp_bims_sys_user_common  SELECT
b.user_id_ user_id,
a.ID_ org_id,
b.rel_name_ job_name,
a.`CODE_` org_code,
a.NAME_ org_name,
s.fullname_,
s.account_,
s.password_,
s.email_,
s.mobile_,
s.weixin_,
s.create_time_,
s.address_,
s.photo_,
s.sex_,
s.from_,
s.status_,
s.update_time,
s.id_card_no
FROM
  ods_bims_sys_org a
left join
  (SELECT
    a.org_id_,
    b.REL_NAME_,
    a.USER_ID_
  FROM
    ods_bims_sys_org_user a,
    ods_bims_sys_org_rel b
  WHERE a.REL_ID_ = b.ID_) b
on a.ID_ = b.org_id_
LEFT JOIN ods_bims_sys_user s on s.id_=b.USER_ID_;

drop table if exists ods_bims_sys_user_common;
ALTER TABLE odstmp_bims_sys_user_common RENAME TO ods_bims_sys_user_common;
