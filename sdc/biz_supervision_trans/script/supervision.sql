use ods;
drop table if exists odstmp_bpms_biz_supervision_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_supervision_common (
   id STRING COMMENT 'ID',
   apply_no STRING COMMENT '订单编号',
   house_no STRING COMMENT '房产信息编号关联房产信息',
   organization STRING COMMENT '监管机构',
   amount DOUBLE COMMENT '监管金额(元)',
   remark STRING,
   account_agree_no STRING COMMENT '资金监管协议号',
   create_user_id STRING COMMENT '创建人id',
   update_user_id STRING COMMENT '更新人id',
   create_time TIMESTAMP COMMENT '记录创建时间',
   update_time TIMESTAMP COMMENT '记录更新时间',
   rev bigint comment '版本号',
   delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
   amount_type STRING COMMENT '监管资金类型',
   organization_type STRING COMMENT '监管机构类型',
   `date` TIMESTAMP COMMENT '监管日期',
   application STRING COMMENT '用途',
  rn bigint
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_supervision_common
select T.*,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from
( SELECT
a.id,
a.apply_no,
a.house_no,
a.organization,
a.amount,
a.remark,
a.account_agree_no,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
a.amount_type,
a.organization_type,
a.`date`,
a.application
FROM
( select * from ods_bpms_biz_supervision where apply_no<>'' and apply_no is not null ) a
union all
SELECT
a1.id,
c1.apply_no,
a1.house_no,
a1.organization,
a1.amount,
a1.remark,
a1.account_agree_no,
a1.create_user_id,
a1.update_user_id,
a1.create_time,
a1.update_time,
a1.rev,
a1.delete_flag,
a1.amount_type,
a1.organization_type,
a1.`date`,
a1.application
FROM
( select * from ods_bpms_biz_supervision where (apply_no<>'' and apply_no is not null) and(house_no<>'' and house_no is not null ) ) a1
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c1 ON c1.house_no = a1.house_no
union all
select
b.id,
c.apply_no apply_no,
b.house_no,
b.organization,
b.amount,
b.remark,
b.account_agree_no,
b.create_user_id,
b.update_user_id,
b.create_time,
b.update_time,
b.rev,
b.delete_flag,
b.amount_type,
b.organization_type,
b.`date`,
b.application
from ( SELECT
*
FROM
ods_bpms_biz_supervision
WHERE
(house_no <> '' AND house_no IS NOT NULL ) and (apply_no ='' or apply_no is null )
) b
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c ON c.house_no = b.house_no
)T;
drop table if exists ods_bpms_biz_supervision_common;
ALTER TABLE odstmp_bpms_biz_supervision_common RENAME TO ods_bpms_biz_supervision_common;