set hive.execution.engine=spark;
use ods;
create table odstmp_ddjf_house_detail_fang as
select
bao.`_id`,
bao.title,
bao.city,
bao.price,
split(bao.housetype,'室|厅|卫')[0] `bedroom`,
split(bao.housetype,'室|厅|卫')[1] `livingroom`,
split(bao.housetype,'室|厅|卫')[2] `toilet`,
bao.housetype `housestyle`,
regexp_replace(bao.area,'平米','') `area`,
regexp_replace(bao.unitprice,'元/平米','') `unitprice`,
bao.direction,
bao.floor,
case when instr(decoration,'装修')>0 then '1' else '0' end `isdecoration`,
bao.decoration,
bao.community,
bao.address1,
bao.address2,
concat(city,address1,address2,community) address,
case when instr(characteristics,'满二')>0 then '1' else '0' end `isfive`,
case when instr(characteristics,'满五')>0 then '1' else '0' end `istwo`,
case when instr(characteristics,'唯一')>0 then '1' else '0' end `isunique`,
bao.characteristics,
bao.intermediary,
bao.intermediarycompany,
bao.intermediaryphone,
bao.communityprice,
bao.yoy,
bao.qoq,
bao.crawldate,
bao.url,
regexp_replace(wx_house.buildage,'年','') `buildage`,
regexp_replace(regexp_replace(wx_house.lift,'年','1'),'无','0') `islift`,
case when instr(wx_sales.salespoint,'地铁')>0 then '1' else '0' end `issubway`,
wx_house.lift,
wx_house.releasetime,
wx_house.rights,
wx_house.buildingtype,
wx_house.housetype,
wx_house.buildstructure,
wx_sales.salesmating,
wx_sales.salespoint,
wx_sales.salesowner,
wx_sales.salespay,
wx_sales.salesserve,
bao.houseinfo,
bao.saleinfo,
bao.communityinfo,
bao.trendinfo
from ods_ddjf_house_detail_fang bao
lateral view json_tuple(houseinfo,'建筑年代','有无电梯','挂牌时间','产权性质','建筑类别','住宅类别','建筑结构') wx_house as
buildage,lift,releasetime,rights,buildingtype,housetype,buildstructure
lateral view json_tuple(saleInfo,'小区配套','核心卖点','业主心态','税费分析','服务介绍') wx_sales as
salesmating,salespoint,salesowner,salespay,salesserve;
drop table if exists ods_ddjf_house_detail_fang_common;
ALTER TABLE odstmp_ddjf_house_detail_fang RENAME TO ods_ddjf_house_detail_fang_common;