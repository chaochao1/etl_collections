#!/bin/sh
#!/bin/sh

set -x
judge_bit=$1
start_date=$2
end_date=$3
hour_start=$4
hour_end=$5
site_from=$6




sudo -u hive hive   <<EOF

use default;


drop table if exists z_liushaowu_0011;
create table z_liushaowu_0011 as
select t.*,case when substr(cookie_id,$judge_bit,1)='A' then 10
when substr(cookie_id,$judge_bit,1)='B' then 11
when substr(cookie_id,$judge_bit,1)='C' then 12 
when substr(cookie_id,$judge_bit,1)='D' then 13
when substr(cookie_id,$judge_bit,1)='E' then 14
when substr(cookie_id,$judge_bit,1)='F' then 15
else substr(cookie_id,$judge_bit,1) end cookie_2 from dw_sheinside_fact_log_detail t 
where dt<='$end_date' and dt>='$start_date' and hour <$hour_end and hour >= $hour_start and site_from='$site_from'
;

drop table if exists z_liushaowu_001_a1;
create table z_liushaowu_001_a1 as
select * from z_liushaowu_0011 where cookie_2%2=1;

drop table if exists z_liushaowu_001_b1;
create table z_liushaowu_001_b1 as
select * from z_liushaowu_0011 where cookie_2%2=0;


drop table if exists z_liushaowu_abtest_by_day_num_cookie_id;
create table z_liushaowu_abtest_by_day_num_cookie_id as
select * from
(
   select dt,'A' test_type,count(distinct cookie_id) cookie_id_num from z_liushaowu_001_a1 group by dt 
   union all
  select dt,'B' test_type,count(distinct cookie_id) cookie_id_num from z_liushaowu_001_b1 group by dt 
) t;



drop table if exists z_liushaowu_abtest_by_day_num_proall;
create table z_liushaowu_abtest_by_day_num_proall as
select * from
(select dt,'A' test_type, count(distinct cookie_id) prod_all from (select dt, cookie_id  from z_liushaowu_001_a1 where  
request_type='GET'  and request_body rlike '.+-p-[0-9]+-cat-[0-9]+.*' union all select  dt,cookie_id  from z_liushaowu_001_a1 
where  request_type='GET'  and request_body rlike '.+-c-[0-9]+.*' union all select dt, cookie_id  from z_liushaowu_001_a1 where  
request_type='GET'  and request_body rlike '.+-vc-[0-9]+.*'  union all select dt, cookie_id  from z_liushaowu_001_a1 where request_type='GET'  
and request_body rlike '^/(byPrice|new_arrival|discount|attribute|lowest|highest|best|popular|alpha|pre-sale|limited-time|daily|top-rated|is-stock).+') t  group by dt
union all
select dt,'B' test_type, count(distinct cookie_id) prod_all from (select dt, cookie_id  from z_liushaowu_001_b1 where  
request_type='GET'  and request_body rlike '.+-p-[0-9]+-cat-[0-9]+.*' union all select  dt,cookie_id  from z_liushaowu_001_b1 
where  request_type='GET'  and request_body rlike '.+-c-[0-9]+.*' union all select dt, cookie_id  from z_liushaowu_001_b1 where  
request_type='GET'  and request_body rlike '.+-vc-[0-9]+.*'  union all select dt, cookie_id  from z_liushaowu_001_b1 where request_type='GET'  
and request_body rlike '^/(byPrice|new_arrival|discount|attribute|lowest|highest|best|popular|alpha|pre-sale|limited-time|daily|top-rated|is-stock).+') b  group by dt
) a ;



drop table if exists z_liushaowu_abtest_by_day_checkout;
create table z_liushaowu_abtest_by_day_checkout as
select t.* from
(select dt,('A') test_type ,count(distinct cookie_id) checkout_all from z_liushaowu_001_a1 where  request_type='GET' and (request_body like '%model=order&action=checkout%' or request_body like '%checkout.php%'or request_body like '%login_register.php?return=place_order%'
or request_body like '%model=order&action=mobile_place_order%') group by dt
union all
select dt,('B') test_type ,count(distinct cookie_id) checkout_all from z_liushaowu_001_b1 where  request_type='GET' and (request_body like '%model=order&action=checkout%' or request_body like '%checkout.php%'or request_body like '%login_register.php?return=place_order%'
or request_body like '%model=order&action=mobile_place_order%') group by dt
) t;


drop table if exists z_liushaowu_abtest_by_day_num_cart;
create table z_liushaowu_abtest_by_day_num_cart as
select * from
(select dt,'A' test_type, count(distinct cookie_id) car_all from z_liushaowu_001_a1 where  request_type='POST' 
and (post_param like '%cart_goods_add%' or post_param like '%goods_cart_add%' or post_param like '%action=add%')  group by dt
union all
select dt,'B' test_type, count(distinct cookie_id) car_all from z_liushaowu_001_b1 where  request_type='POST' 
and (post_param like '%cart_goods_add%' or post_param like '%goods_cart_add%' or post_param like '%action=add%') group by dt
) t;


drop table if exists z_liushaowu_abtest_by_day;
create table z_liushaowu_abtest_by_day
(dt string,test_type string,num_cookie_id int,num_proall int,num_cart int,num_checkout int);


insert overwrite table z_liushaowu_abtest_by_day 
select t1.dt,
t1.test_type,
t1.cookie_id_num,
t2.prod_all,
t3.car_all,
t4.checkout_all
 from z_liushaowu_abtest_by_day_num_cookie_id t1
left join z_liushaowu_abtest_by_day_num_proall t2 on t1.dt=t2.dt and t1.test_type = t2.test_type 
left join z_liushaowu_abtest_by_day_num_cart t3 on t1.dt=t3.dt  and t1.test_type = t3.test_type
left join z_liushaowu_abtest_by_day_checkout t4 on t1.dt=t4.dt and t1.test_type = t4.test_type; 







drop table if exists z_liushaowu_abtest_by_hour_num_cookie_id;
create table z_liushaowu_abtest_by_hour_num_cookie_id as
select * from
(select dt,'A' test_type,hour, count(distinct cookie_id) cookie_id_num from z_liushaowu_001_a1 group by dt,hour
union all
select dt,'B' test_type,hour, count(distinct cookie_id) cookie_id_num from z_liushaowu_001_b1 group by dt,hour
) t;

drop table if exists z_liushaowu_abtest_by_hour_num_proall;
create table z_liushaowu_abtest_by_hour_num_proall as
select * from
( 
  select dt,test_type,hour,count(distinct cookie_id) prod_all from 
  (
  select dt,'A' test_type,hour, cookie_id  from z_liushaowu_001_a1 where  request_type='GET'  and request_body rlike '.+-p-[0-9]+-cat-[0-9]+.*' 
  union all 
  select  dt,'A' test_type, hour,cookie_id  from z_liushaowu_001_a1 where  request_type='GET'  and request_body rlike '.+-c-[0-9]+.*' 
  union all 
  select dt,'A' test_type, hour, cookie_id  from z_liushaowu_001_a1 where  request_type='GET'  and request_body rlike '.+-vc-[0-9]+.*'  
  union all 
  select dt, 'A' test_type,hour, cookie_id  from z_liushaowu_001_a1 where request_type='GET'  and request_body rlike '^/(byPrice|new_arrival|discount|attribute|lowest|highest|best|popular|alpha|pre-sale|limited-time|daily|top-rated|is-stock).+') a group by dt,test_type, hour
   union all
   select dt,test_type, hour, count(distinct cookie_id) prod_all from 
   (
   select dt,'B' test_type,hour, cookie_id  from z_liushaowu_001_b1 where  request_type='GET'  and request_body rlike '.+-p-[0-9]+-cat-[0-9]+.*' 
   union all 
   select dt,'B' test_type, hour,cookie_id  from z_liushaowu_001_b1 where  request_type='GET'  and request_body rlike '.+-c-[0-9]+.*' 
   union all 
   select dt,'B' test_type,hour, cookie_id  from z_liushaowu_001_b1 where  request_type='GET'  and request_body rlike '.+-vc-[0-9]+.*'  
   union all 
   select dt,'B' test_type,hour, cookie_id  from z_liushaowu_001_b1 where request_type='GET'  and request_body rlike '^/(byPrice|new_arrival|discount|attribute|lowest|highest|best|popular|alpha|pre-sale|limited-time|daily|top-rated|is-stock).+') b group by dt, test_type,hour
) t;




drop table if exists z_liushaowu_abtest_by_hour_num_cart;
create table z_liushaowu_abtest_by_hour_num_cart as
select * from
(select dt,hour,'A' test_type, count(distinct cookie_id) car_all from z_liushaowu_001_a1 where  request_type='POST' 
and (post_param like '%cart_goods_add%' or post_param like '%goods_cart_add%' or post_param like '%action=add%')  group by dt,hour
union all
select dt,hour,'B' test_type, count(distinct cookie_id) car_all from z_liushaowu_001_b1 where  request_type='POST' 
and (post_param like '%cart_goods_add%' or post_param like '%goods_cart_add%' or post_param like '%action=add%') group by dt,hour
) t;


drop table if exists z_liushaowu_abtest_by_hour_checkout;
create table z_liushaowu_abtest_by_hour_checkout as
select * from
(
select dt,'A' test_type, hour, count(distinct cookie_id) checkout_all from z_liushaowu_001_a1 
where  request_type='GET' and (request_body like '%model=order&action=checkout%' or request_body like '%checkout.php%'or request_body like '%login_register.php?return=place_order%'
or request_body like '%model=order&action=mobile_place_order%') group by dt, hour
union all
select dt,'B' test_type,hour, count(distinct cookie_id) checkout_all from z_liushaowu_001_b1 where  request_type='GET' and (request_body like '%model=order&action=checkout%' or request_body like '%checkout.php%'or request_body like '%login_register.php?return=place_order%'
or request_body like '%model=order&action=mobile_place_order%') group by dt,hour
) t;

drop table if exists z_liushaowu_abtest_by_hour;
create table z_liushaowu_abtest_by_hour
(dt string,hour int,test_type string,num_cookie_id int,num_proall int,num_cart int,num_checkout int);

insert overwrite table z_liushaowu_abtest_by_hour 
select t1.dt,
t1.hour,
t1.test_type,
t1.cookie_id_num,
t2.prod_all,
t3.car_all,
t4.checkout_all
 from z_liushaowu_abtest_by_hour_num_cookie_id t1
left join z_liushaowu_abtest_by_hour_num_proall t2 on t1.dt=t2.dt and t1.test_type = t2.test_type and t1.hour = t2.hour
left join z_liushaowu_abtest_by_hour_num_cart t3 on t1.dt=t3.dt and t1.test_type = t3.test_type and t1.hour = t3.hour
left join z_liushaowu_abtest_by_hour_checkout t4 on t1.dt=t4.dt and t1.test_type = t4.test_type and t1.hour = t4.hour;


EOF

