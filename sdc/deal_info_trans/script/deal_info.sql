use ods;
drop table if exists odstmp_bpms_biz_deal_info_common;
CREATE TABLE if not exists ods.odstmp_bpms_biz_deal_info_common (
  id STRING COMMENT 'ID',
  apply_no STRING COMMENT '订单编号',
  house_no STRING COMMENT '房产信息编号关联房产信息',
  deal_no STRING COMMENT '交易合同编号',
  trading_price DOUBLE COMMENT '成交价(元)',
  earnest_money DOUBLE COMMENT '交易定金(元)',
  first_payment_paid DOUBLE COMMENT '已付首付款(元)',
  first_payment_unpaid DOUBLE COMMENT '未付首付款(元)',
  handing_room_deposit DOUBLE COMMENT '交房押金(元)',
  remark STRING COMMENT '备注',
  actual_trading_price DOUBLE COMMENT '实际成交价',
  create_user_id STRING COMMENT '创建人id',
  update_user_id STRING COMMENT '更新人id',
  create_time timestamp COMMENT '记录创建时间',
  update_time timestamp COMMENT '记录更新时间',
  rev bigint comment '版本号',
  delete_flag STRING COMMENT '记录删除标识(1：删除；0：有效记录)',
  down_payment DOUBLE COMMENT '首付款金额',
  transfer_price DOUBLE COMMENT '过户（网签）价格',
  rn bigint
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_deal_info_common
select T.*,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from
( SELECT
a.id,
a.apply_no,
a.house_no,
a.deal_no,
a.trading_price,
a.earnest_money,
a.first_payment_paid,
a.first_payment_unpaid,
a.handing_room_deposit,
a.remark,
a.actual_trading_price,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
a.down_payment,
a.transfer_price
FROM
( select * from ods_bpms_biz_deal_info where apply_no<>'' and apply_no is not null ) a
union all
SELECT
a1.id,
c1.apply_no,
a1.house_no,
a1.deal_no,
a1.trading_price,
a1.earnest_money,
a1.first_payment_paid,
a1.first_payment_unpaid,
a1.handing_room_deposit,
a1.remark,
a1.actual_trading_price,
a1.create_user_id,
a1.update_user_id,
a1.create_time,
a1.update_time,
a1.rev,
a1.delete_flag,
a1.down_payment,
a1.transfer_price
FROM
( select * from ods_bpms_biz_deal_info where (apply_no<>'' and apply_no is not null) and(house_no<>'' and house_no is not null ) ) a1
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c1 ON c1.house_no = a1.house_no
union all
select
b.id,
c.apply_no apply_no,
b.house_no,
b.deal_no,
b.trading_price,
b.earnest_money,
b.first_payment_paid,
b.first_payment_unpaid,
b.handing_room_deposit,
b.remark,
b.actual_trading_price,
b.create_user_id,
b.update_user_id,
b.create_time,
b.update_time,
b.rev,
b.delete_flag,
b.down_payment,
b.transfer_price from ( SELECT
*
FROM
ods_bpms_biz_deal_info
WHERE
(house_no <> '' AND house_no IS NOT NULL ) and (apply_no ='' or apply_no is null )
) b
LEFT JOIN ( select apply_no, house_no from ods_bpms_biz_apply_order where house_no <> '' AND house_no IS NOT NULL ) c ON c.house_no = b.house_no
)T;
drop table if exists ods_bpms_biz_deal_info_common;
ALTER TABLE odstmp_bpms_biz_deal_info_common RENAME TO ods_bpms_biz_deal_info_common;