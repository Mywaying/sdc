use ods;
drop table if exists ods_house_detail_common;
create table if not exists ods_house_detail_common (
  id string COMMENT ' id',
  city string COMMENT '城市',
  title string COMMENT '标题',
  area string COMMENT '面积',
  address1 string COMMENT '区域',
  address2 string COMMENT '区域２',
  address string COMMENT '详细地址',
  community string COMMENT '小区名',
  bedroom        bigint comment ' 卧室数',
  livingroom     bigint comment ' 客厅数',
  toilet         bigint comment ' 卫生间数',
  buildstructure string COMMENT '户型结构',
  buildingtype string COMMENT '建筑类型',
  housetype string COMMENT '交易权属性',
  direction string COMMENT '朝向',
  floornum       bigint comment ' 楼层',
  totalfloornum  bigint comment ' 总楼层',
  isfive         bigint comment ' 是否满五',
  istwo          bigint comment ' 是否满两年',
  isunique       bigint comment ' 是否唯一',
  isdecoration   bigint comment ' 是否精装修',
  islift         bigint comment ' 是否配电梯',
  rights string COMMENT '产权所属',
  rightslimit string COMMENT '产权年限',
  ispledge       bigint comment ' 抵押信息',
  issubway       bigint comment ' 是否地铁',
  releasetime    timestamp COMMENT ' 发布时间 / 挂牌时间',
  updatetime     timestamp COMMENT ' 更新时间',
  longitude      double COMMENT ' 经度',
  latitude       double COMMENT ' 纬度',
  unitprice      double COMMENT ' 均价',
  crawldate      timestamp COMMENT ' 采集时间',
  url string COMMENT '数据源',
  price          double COMMENT ' 总价',
  houseinfo string COMMENT '房源特色描述(briefinfo)',
  buildage       bigint comment ' 建筑年代',
  decoration string COMMENT '装修类别',
  characteristics string COMMENT '房屋年限描述',
  communityprice double COMMENT ' 小区平均价'
) stored as parquet;

insert overwrite table ods_house_detail_common
select
  cast (`_id` as string),
  `city`,
  `title`,
  `area`,
  `address1`,
  `address2`,
  dc.`address`,
  `community`,
  cast(`bedroom` as bigint),
  cast(`livingroom` as bigint),
  cast(`toilet` as bigint),
  `buildstructure`,
  `buildingtype`,
  `housetype`,
  `direction`,
  NULL `floornum`,
  NULL `totalfloornum`,
  cast(`isfive` as bigint),
  cast(`istwo` as bigint),
  cast(`isunique` as bigint),
  cast(`isdecoration` as bigint),
  cast(`islift` as bigint),
  `rights`,
  NULL `rightslimit`,
  NULL `ispledge`,
  cast(`issubway` as bigint),
  `releasetime`,
  NULL `updatetime`,
  NULL `longitude`,
  NULL `latitude`,
  cast(`unitprice` as double),
  `crawldate`,
  `url`,
  cast(`price` as double),
  `houseinfo`,
  cast(`buildage` as bigint),
  `decoration`,
  `characteristics`,
  cast(`communityprice` as double)
from ods_ddjf_house_detail_fang_common dc