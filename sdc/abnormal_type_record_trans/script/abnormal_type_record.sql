use ods;
drop table if  exists ods.odstmp_bpms_biz_abnormal_type_record_common;
create table odstmp_bpms_biz_abnormal_type_record_common as
SELECT
a.id,
a.apply_no,
a.inst_id,
a.abnormal_type,
a.create_user_id,
b.fullname_ create_user_name,
a.create_time,
a.update_user_id,
a.update_time,
sd.NAME_ abnormal_type_name
FROM
ods_bpms_biz_abnormal_type_record a
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bpms_sys_dic_common where  TYPE_ID_='10000097980709' ) sd on sd.KEY_=a.abnormal_type
left join ods_bpms_sys_user b on b.id_=a.create_user_id;
drop table if exists ods_bpms_biz_abnormal_type_record_common;
ALTER TABLE odstmp_bpms_biz_abnormal_type_record_common RENAME TO ods_bpms_biz_abnormal_type_record_common;