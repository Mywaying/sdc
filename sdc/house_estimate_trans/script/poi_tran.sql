use ods;
create table if not exists tmp_house_poi_info_add (community string,address string);
with tmp_address as (
   SELECT community,concat(city,address1,address2,community) address from ods.ods_ddjf_house_detail_fang
)
insert overwrite table tmp_house_poi_info_add
select community,address from (select community,address  from tmp_address group by community,address)T
where T.address not in (select address from house_poi_info);
insert into table house_poi_info
select row_number() over(order by t1.community) + t2.max_id as id,t1.community,t1.address,NULL,NULL
from (select community,address from tmp_house_poi_info_add) t1
cross join (select coalesce(max(id),0) max_id from house_poi_info) t2;
insert overwrite table ods_house_poi_info select * from house_poi_info;
refresh ods_house_poi_info;