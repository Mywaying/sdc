use ods;
drop table if exists odstmp_bpms_biz_account_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_account_common (
  id STRING,
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号关联房产信息',
  remark STRING,
  name STRING COMMENT '户名',
  number STRING COMMENT '账号',
  open_bank_no STRING COMMENT '开户行行号',
  open_bank STRING COMMENT '开户行',
  type STRING COMMENT '账户类型',
  application STRING COMMENT '用途',
  pay_type STRING COMMENT '支付方式',
  reserve_phone STRING COMMENT '账户预留手机',
  create_user_id STRING COMMENT '创建人id',
  update_user_id STRING COMMENT '更新人id',
  create_time timestamp COMMENT '记录创建时间',
  update_time timestamp COMMENT '记录更新时间',
  rev bigint comment '版本号',
  is_platform_return_accountnt STRING COMMENT '是否选定为平台款还款账户',
  delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
  rn bigint
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_account_common
select T.*,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from
( SELECT
a.id,
a.apply_no,
a.house_no,
a.remark,
a.name,
a.number,
a.open_bank_no,
a.open_bank,
a.type,
a.application,
a.pay_type,
a.reserve_phone,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.is_platform_return_account,
a.delete_flag
FROM
( select * from ods_bpms_biz_account where apply_no<>'' and apply_no is not null ) a
union all
SELECT
a1.id,
c1.apply_no,
a1.house_no,
a1.remark,
a1.name,
a1.number,
a1.open_bank_no,
a1.open_bank,
a1.type,
a1.application,
a1.pay_type,
a1.reserve_phone,
a1.create_user_id,
a1.update_user_id,
a1.create_time,
a1.update_time,
a1.rev,
a1.is_platform_return_account,
a1.delete_flag
FROM
( select * from ods_bpms_biz_account where (apply_no<>'' and apply_no is not null) and(house_no<>'' and house_no is not null ) ) a1
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c1 ON c1.house_no = a1.house_no
union all
select
b.id,
c.apply_no apply_no,
b.house_no,
b.remark,
b.name,
b.number,
b.open_bank_no,
b.open_bank,
b.type,
b.application,
b.pay_type,
b.reserve_phone,
b.create_user_id,
b.update_user_id,
b.create_time,
b.update_time,
b.rev,
b.is_platform_return_account,
b.delete_flag from ( SELECT
*
FROM
ods_bpms_biz_account
WHERE
(house_no <> '' AND house_no IS NOT NULL ) and (apply_no ='' or apply_no is null )
) b
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c ON c.house_no = b.house_no
)T;
drop table if exists ods_bpms_biz_account_common;
ALTER TABLE odstmp_bpms_biz_account_common RENAME TO ods_bpms_biz_account_common;