use ods;
create table odstmp_bims_sys_dic_common as
select ID_,TYPE_ID_, lower(KEY_)KEY_,NAME_,regexp_replace(regexp_replace(regexp_replace(NAME_,'卖家','卖方'),'买家','买方'),'极速放款','审批即放全款')NAME_new_,PARENT_ID_,SN_,status_
,ROW_NUMBER() OVER(PARTITION BY KEY_ ORDER BY ID_ asc) rn
from ods_bims_sys_dic;

drop table if exists ods_bims_sys_dic_common;
ALTER TABLE odstmp_bims_sys_dic_common RENAME TO ods_bims_sys_dic_common;