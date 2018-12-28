 select count(1)   
from sis_dw.dw_fact_order_ref_105 a
     inner join sis_dw.dw_fact_order b
        on a.order_id = b.order_id
             where a.add_time >= to_date('2015/5/2','YYYY/MM/DD')
                   and a.add_time<to_date('2015/5/3','YYYY/MM/DD')
               and refund_type in (10,11,16,26,29,30);
                
           
        
   select count(1)   
from sis_dw.dw_fact_order_ref_105 a
     inner join sis_dw.dw_fact_order b
        on a.order_id = b.order_id
             where a.add_time >= to_date('2015/5/2','YYYY/MM/DD')
                   and a.add_time<to_date('2015/5/3','YYYY/MM/DD');
                   
    select count(1) from sis_dw.dw_fact_order b 
                     where a.add_time >= to_date('2015/5/2','YYYY/MM/DD')
                        and a.add_time<to_date('2015/5/3','YYYY/MM/DD');
    
                   
                   
                   select * from dm_order_refund_week@dblink;
                   
                   dm_order_refund_day
                   
 
 select * from dm_order_refund_day@dblink;
              
 
 
  select  trunc(a.add_time),
              a.refund_type,
              count(1),
              11,
              12,
              13
           from sis_dw.dw_fact_order_ref_105 a
        inner join sis_dw.dw_fact_order b
             on a.order_id = b.order_id
        where a.add_time >= to_date('2015/5/2','YYYY/MM/DD')
                   and a.add_time<to_date('2015/5/3','YYYY/MM/DD')
         and a.refund_type in (10,11,16,26,29,30)
         group by trunc(a.add_time),
              a.refund_type;
             
             
                   
    
                   
                   
                   
                   
               
               
               
               
 select to_char(to_date(a.add_time,'YYYY/MM/DD'),'IYIW') from sis_dw.dw_fact_order_ref_105 a;
 select TO_CHAR(TO_DATE('1997/12/31','YYYY/MM/DD'), 'YYIW') from dual;  
 select a.add_time from sis_dw.dw_fact_order_ref_105 a;
 
 select to_date('2015/5/1','YYYY/MM/DD') from dual;
 
 
 select   trunc(a.add_time),
             a.refund_type,
             count(1)
        from sis_dw.dw_fact_order_ref_105 a
       inner join sis_dw.dw_fact_order b
          on a.order_id = b.order_id
              where a.add_time >= to_date('2015/5/2','YYYY/MM/DD')
                   and a.add_time<to_date('2015/5/15','YYYY/MM/DD')
         and a.refund_type in (10,11,16,26,29,30)
       group by  trunc(a.add_time),
                a.refund_type
                order by  trunc(a.add_time), a.refund_type;
                
                
  select t1.add_date,
             case
               when refund_type_reason = '不合适' then
                '无理由'
               else
                refund_type_reason
             end,
            t1.refund_count
        from (
            select   to_date(a.add_time,'YYYY/MM/DD') add_date,
             a.refund_type,
             count(1) refund_count
        from sis_dw.dw_fact_order_ref_105 a
       inner join sis_dw.dw_fact_order b
          on a.order_id = b.order_id
              where a.add_time >= to_date('2015/5/2','YYYY/MM/DD')
                   and a.add_time<to_date('2015/5/15','YYYY/MM/DD')
         and a.refund_type in (10,11,16,26,29,30)
       group by  to_date(a.add_time,'YYYY/MM/DD'),
                a.refund_type
        ) t1
        left join sis_dw.dw_dim_order_refund_type t2
          on t1.refund_type = t2.id;               
                
                
         select count(1)   
             from sis_dw.dw_fact_bill_105 a 
             inner join sis_dw.dw_fact_order b
                on a.order_id = b.order_id
             where a.add_time >= v_date 
               and a.add_time <= (v_date+1);
                


create table TMP_DM_ORDER_REFUND_QUA_DAY
(
  ADD_DATE     DATE,
  REFUND_TYPE  NUMBER(10),
  T_REFUND_CNT NUMBER(10),
  REFUND_QUA_CNT NUMBER(10),
  REFUND_CNT NUMBER(10),
  ORDER_CNT NUMBER(10)
)    


create table DM_ORDER_REFUND_QUA_DAY
(
  ADD_DATE      DATE,
  REFUND_REASON VARCHAR2(10),
  T_REFUND_CNT NUMBER(10),
  REFUND_QUA_CNT NUMBER(10),
  REFUND_CNT NUMBER(10),
  ORDER_CNT NUMBER(10)
)     
                

select * from DM_ORDER_REFUND_QUA_DAY;



     select nvl(sum(refund_qua_cnt), 0)
					
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE(to_date('2015/5/2','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2015/5/2','YYYY/MM/DD'));
                
                
                

 
 drop table TMP_DM_ORDER_REFUND_QUA_DAY;
 drop table DM_ORDER_REFUND_QUA_DAY;
 
 
 
/*当天订单数*/
 select count(1) 

        from sis_dw.dw_fact_order a
 where a.add_time >= to_date('2015/5/2','YYYY/MM/DD')
                        and a.add_time<to_date('2015/5/3','YYYY/MM/DD');
 
 select add_date, refund_qua_cnt from DM_ORDER_REFUND_QUA_DAY group by add_date;
 
 select * from DM_ORDER_REFUND_QUA_DAY;
 
 
select distinct add_date,REFUND_QUA_CNT,REFUND_CNT,ORDER_CNT from DM_ORDER_REFUND_QUA_DAY where add_date = to_date('2015/5/2','YYYY/MM/DD') ;



create table TMP_DM_ORD_REFUND_QUA_NUM_DAY
(
  ADD_DATE      DATE,
  REFUND_QUA_CNT NUMBER(10),
  REFUND_CNT NUMBER(10),
  ORDER_CNT NUMBER(10)
)

create table DM_ORD_REFUND_QUA_NUM_DAY
(
  ADD_DATE      DATE,
  REFUND_QUA_CNT NUMBER(10),
  REFUND_CNT NUMBER(10),
  ORDER_CNT NUMBER(10)
)

drop table TMP_DM_ORD_REFUND_QUA_NUM_DAY;
drop table  DM_ORD_REFUND_QUA_NUM_DAY;
drop table TMP_DM_ORDER_REFUND_QUA_DAY;
drop table DM_ORDER_REFUND_QUA_DAY;

select * from DM_ORD_REFUND_QUA_NUM_DAY;

/*按周统计*/

     select nvl(sum(refund_qua_cnt), 0)
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE(to_date('2015/5/8','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2015/5/8','YYYY/MM/DD'));
          
          
select nvl(sum(refund_cnt), 0)
				
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
	       TO_CHAR(TO_DATE(to_date('2015/5/8','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2015/5/8','YYYY/MM/DD'));
          
          
 select nvl(sum(order_cnt), 0)
				
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
	       TO_CHAR(TO_DATE(to_date('2015/5/8','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2015/5/8','YYYY/MM/DD'));
          
          
          create table TMP_DM_ORDER_REFUND_QUA_DAY
(
  ADD_DATE     DATE,
  REFUND_TYPE  NUMBER(10),
  T_REFUND_CNT NUMBER(10)
)    


create table DM_ORDER_REFUND_QUA_DAY
(
  ADD_DATE      DATE,
  REFUND_REASON VARCHAR2(10),
  T_REFUND_CNT NUMBER(10)
) 
          
          
          create table TMP_DM_ORD_REFUND_QUA_NUM_DAY
(
  ADD_DATE      DATE,
  REFUND_QUA_CNT NUMBER(10),
  refund_pdc_cnt  NUMBER(10),
  refund_pst_cnt NUMBER(10),
  REFUND_CNT NUMBER(10),
  ORDER_CNT NUMBER(10)
)

create table DM_ORD_REFUND_QUA_NUM_DAY
(
  ADD_DATE      DATE,
  REFUND_QUA_CNT NUMBER(10),
  refund_pdc_cnt  NUMBER(10),
  refund_pst_cnt NUMBER(10),
  REFUND_CNT NUMBER(10),
  ORDER_CNT NUMBER(10)
)


 select TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW')  static_week,refund_reason, nvl(sum(T_REFUND_CNT), 0) refund_cnt
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE(to_date('2015/5/8','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2015/5/8','YYYY/MM/DD'))
          group by TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW'),refund_reason;
          
 select static_week,refund_reason sum(refund_cnt) from  
    (
    select TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW')  static_week,refund_reason, nvl(sum(T_REFUND_CNT), 0) refund_cnt
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE(to_date('2015/5/8','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2015/5/8','YYYY/MM/DD'))
          group by TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW'),refund_reason) t group by static_week,refund_reason sum(refund_cnt) ;      
          
          
    select nvl(sum(T_REFUND_CNT), 0)
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE(to_date('2015/5/8','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2015/5/8','YYYY/MM/DD'))
          ;
          
 select * from DM_ORDER_REFUND_QUA_DAY;
 
 select * from dm_order_refund_week@dblink;
  
  select * from dm_order_refund_week;        
          
          
     select nvl(sum(REFUND_CNT), 0)
					from dm_order_refund_day a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE(to_date('2014/5/27','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from to_date('2014/5/27','YYYY/MM/DD'));      
          
          
          
          
          
     select nvl(sum(REFUND_QUA_CNT), 0)
				
					from DM_ORD_REFUND_QUA_NUM_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE('2015/5/8', 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from TO_DATE('2015/5/8','YYYY/MM/DD'));
          
     select nvl(sum(order_cnt), 0)
					into week_order_cnt
					from DM_ORDER_REFUND_QUA_DAY a
					left join sis_dw.dw_dim_date c
					on a.add_date = c.full_date_key
					where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
					and extract(year from full_date_key) = extract(year from v_date);
          
          
       select c.static_week,
				c.week_of_year,
				sum(a.refund_cnt),
				a.refund_reason
				from dm_order_refund_day@dblink a
				left join sis_dw.dw_dim_date c
				on a.add_date = c.full_date_key
				where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
				TO_CHAR(TO_DATE('2015/5/8', 'YYYY/MM/DD'), 'IYIW')
				and extract(year from full_date_key) = extract(year from TO_DATE('2015/5/8','YYYY/MM/DD'))
				group by c.static_week, c.week_of_year, a.refund_reason;
          
        
  select * from dm_order_refund_day;
select * from  DM_ORDER_REFUND_QUA_DAY;
          
select * from  DM_ORD_REFUND_QUA_NUM_DAY;

select TO_CHAR(TO_DATE(to_date('2014/5/27','YYYY/MM/DD'), 'YYYY/MM/DD'), 'IYIW') from dual;

select * from dm_order_refund_week;

select * from dm_order_refund_rate_week;



 create table tmp_dm_ord_refund_qua_week
(
  WEEK_OF_YEAR  VARCHAR2(10),
  REFUND_REASON VARCHAR2(10),
  REFUND_CNT    NUMBER(10),
  STATIC_WEEK   VARCHAR2(30)
)


create table DM_ORD_REFUND_qua_WEEK
(
  WEEK_OF_YEAR  VARCHAR2(7),
  REFUND_REASON VARCHAR2(10),
  REFUND_CNT    NUMBER(10),
  STATIC_WEEK   VARCHAR2(30)
)


drop table tmp_dm_ord_refund_qua_week;
drop table DM_ORDER_REFUND_qua_WEEK;

execute immediate 'truncate table tmp_dm_ord_refund_qua_week';

insert into  tmp_dm_ord_refund_qua_week 
(STATIC_WEEK, WEEK_OF_YEAR,REFUND_REASON,REFUND_CNT )
select
     c.static_week,
     c.week_of_year,
     a.refund_reason,
     a.t_refund_cnt 
from  DM_ORDER_REFUND_QUA_DAY a 
         left join sis_dw.dw_dim_date c 
     on a.add_date = c.full_date_key
				where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
				TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
				and extract(year from full_date_key) = extract(year from v_date)
	   group by c.static_week, c.week_of_year, a.refund_reason;
     commit;


select 
     c.static_month,a.refund_reason,sum(a.t_refund_cnt) refund_cnt
from    DM_ORDER_REFUND_QUA_DAY a 
         left join sis_dw.dw_dim_date c 
     on a.add_date = c.full_date_key
     where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
				TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
				and extract(year from full_date_key) = extract(year from v_date)
          group by  c.static_month,a.refund_reason;
          
          
          
          
 insert into  tmp_DM_ORD_REFUND_qua_month   
 (static_month,REFUND_REASON,REFUND_CNT )     
select 
     c.static_month,a.refund_reason,sum(a.t_refund_cnt) refund_cnt
from    DM_ORDER_REFUND_QUA_DAY a 
         left join sis_dw.dw_dim_date c 
       on a.add_date = c.full_date_key
 where TO_CHAR(TO_DATE('2015/5/8', 'YYYY/MM/DD'), 'IYIW')
				and extract(year from full_date_key) = extract(year from TO_DATE('2015/5/8','YYYY/MM/DD'))
          group by  c.static_month,a.refund_reason;
          
          
          
          truncate table DM_ORD_REFUND_qua_WEEK
          truncate table   DM_ORD_REFUND_qua_month
          
          select * from DM_ORD_REFUND_qua_WEEK;
          select * from DM_ORD_REFUND_qua_month;
          
          
          
           select nvl(sum(refund_cnt), 0)
      into week_cnt
      from dm_order_refund_day@dblink a
      left join sis_dw.dw_dim_date c
        on a.add_date = c.full_date_key
      where TO_CHAR(TO_DATE('2015/5/8', 'YYYY/MM/DD'), 'IYIW')
				and extract(year from full_date_key) = extract(year from TO_DATE('2015/5/8','YYYY/MM/DD'));
          
          
create table DM_ORDER_REFUND_CNT_RATE
(
  Problem_type      VARCHAR2(10),
  count_type        VARCHAR2(10),
  static_date		VARCHAR2(10),
  refund_reason     VARCHAR2(15),
  refund_cnt        NUMBER(10),
  refund_rate		NUMBER(10),
  order_rate 		NUMBER(10)  
)      
         
          
          
select count( a.order_id ) 
       from sis_dw.dw_fact_order a
       left join sis_dw.dw_dim_date c
       on  trunc( a.add_time) = c.full_date_key
       where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE('2015/5/15', 'YYYY/MM/DD'), 'IYIW')
          
          
          and extract(year from  TO_DATE(full_date_key,'YYYY/MM/DD')) = extract(year from TO_DATE('2015/5/15','YYYY/MM/DD'));
          
   select extract(year from TO_DATE('2015/5/15','YYYY/MM/DD')) from dual;
     select extract(year from TO_DATE('2015/5/15','YYYY/MM/DD')) from dual;      
          select count( a.order_id ) 
       from sis_dw.dw_fact_order a
       left join sis_dw.dw_dim_date c
       on  trunc( a.add_time) = c.full_date_key
       where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
					TO_CHAR(TO_DATE('2012/3/15', 'YYYY/MM/DD'), 'IYIW')
          and extract(year from full_date_key) = extract(year from TO_DATE('2012/3/15','YYYY/MM/DD'));
          
          
     select count(a.order_id)
  from sis_dw.dw_fact_order a
  where TO_DATE( a.add_time, 'YYYY/MM/DD')
  
  select add_time from  sis_dw.dw_fact_order where rownum<40 ;
    select TO_DATE( add_time, 'YYYY/MM/DD') from  sis_dw.dw_fact_order where rownum<40;
      select trunc(add_time) from  sis_dw.dw_fact_order where rownum<40 order by add_time desc;
  
  select full_date_key from  sis_dw.dw_dim_date where rownum<40;


select extract(month from TO_DATE('2012/3/15','YYYY/MM/DD')) from dual;
























