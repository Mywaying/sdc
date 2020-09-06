use ods;
drop table if exists odstmp_bpms_biz_channel_common;
CREATE TABLE ods.odstmp_bpms_biz_channel_common (
  id STRING COMMENT 'ID',
  apply_no STRING COMMENT '申请编号',
  channel_name STRING COMMENT '渠道名称',
  channel_type STRING COMMENT '渠道类型',
  channel_mode STRING,
  contact STRING COMMENT '联络人',
  channel_phone STRING COMMENT '渠道电话',
  rebate_man STRING COMMENT '返佣人',
  rebate_bank_id STRING COMMENT '返佣开户行id',
  rebate_bank STRING COMMENT '返佣开户行',
  rebate_card_no STRING COMMENT '返佣账号',
  create_user_id STRING COMMENT '创建人id',
  update_user_id STRING COMMENT '更新人id',
  create_time timestamp,
  update_time timestamp,
  rev         bigint comment ' 版本号',
  delete_flag STRING,
  tag STRING,
  rebate_account_type STRING,
  rebate_type STRING,
  remark STRING,
  channel_tag STRING COMMENT '渠道标签_v3',
  channel_tag_new STRING,
  channel_type_name STRING COMMENT '渠道类型名称'
) STORED AS parquet;
insert overwrite table odstmp_bpms_biz_channel_common SELECT
bc.id,
bc.apply_no,
bc.channel_name,
bc.channel_type,
bc.channel_mode,
bc.contact,
bc.channel_phone,
bc.rebate_man,
bc.rebate_bank_id,
bc.rebate_bank,
bc.rebate_card_no,
bc.create_user_id,
bc.update_user_id,
bc.create_time,
bc.update_time,
bc.rev,
bc.delete_flag,
bc.tag,
bc.rebate_account_type,
bc.rebate_type,
bc.remark,
bc.channel_tag,
case when (bc.channel_tag is not null and bc.channel_tag<>'')
then ( case when bc.channel_tag = "commonChannel" then "普通渠道"
when bc.channel_tag = "specificChannel" then "特定渠道"
else " " end )
else ( case when bc.tag = 'agency' then '代理机构'
when bc.tag = 'league' then '加盟业务专用'
when bc.tag = 'normal' then '普通渠道'
when bc.tag = 'special' then '特定渠道' end
)
end channel_tag_new,
d.`NAME_` channel_type_name
FROM
ods_bpms_biz_channel bc
left join ( select DISTINCT KEY_, NAME_new_ NAME_ from ods_bpms_sys_dic_common where TYPE_ID_='10000026330035' or TYPE_ID_='10000026340035'  )d on d.KEY_=lower(bc.`channel_type`);
drop table if exists ods_bpms_biz_channel_common;
ALTER TABLE odstmp_bpms_biz_channel_common
RENAME TO ods_bpms_biz_channel_common;