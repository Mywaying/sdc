use ods;
drop table if  exists ods.odstmp_bpms_order_fees_common;
create table odstmp_bpms_order_fees_common as
SELECT
a.id,
a.apply_no,
a.fee_define_no,
a.fee_type,
a.fee_type_used,
a.fee_name,
a.receive_or_pay,
a.charge_type,
a.fee_rate,
a.rate_min,
a.rate_max,
a.fee_amount,
a.amount_min,
a.fee_value,
a.amount_max,
a.payment_way,
a.payment_party,
a.payment_time,
a.remark,
a.create_user_id,
a.update_user_id,
a.create_time,
a.update_time,
a.rev,
a.delete_flag,
b.fee_metadata_name,
b.fee_metadata_code,
b.fee_metadata_type
FROM
ods_bpms_biz_fee_detial a
LEFT JOIN ods_bpms_biz_base_fee_metadata b ON a.fee_define_no = b.fee_metadata_code;
drop table if exists ods_bpms_order_fees_common;
ALTER TABLE odstmp_bpms_order_fees_common RENAME TO ods_bpms_order_fees_common;
