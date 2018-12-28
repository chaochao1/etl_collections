select c.static_week,
       c.week_of_year,
       a.refund_reason,
       sum(a.t_refund_cnt) refund_cnt
  from DM_ORDER_REFUND_QUA_DAY a
  left join sis_dw.dw_dim_date c
    on a.add_date = c.full_date_key
 where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
       TO_CHAR(TO_DATE(to_date('2015/5/8', 'YYYY/MM/DD'), 'YYYY/MM/DD'),
               'IYIW')
   and extract(year from full_date_key) =
       extract(year from to_date('2015/5/8', 'YYYY/MM/DD'))
 group by c.static_week, c.week_of_year, a.refund_reason;
commit;
     


 select c.static_week, c.week_of_year, a.refund_reason, sum(a.t_refund_cnt)
        from DM_ORDER_REFUND_QUA_DAY a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
       TO_CHAR(TO_DATE(to_date('2015/5/8', 'YYYY/MM/DD'), 'YYYY/MM/DD'),
               'IYIW')
   and extract(year from full_date_key) =
       extract(year from to_date('2015/5/8', 'YYYY/MM/DD'))
       group by c.static_week, c.week_of_year, a.refund_reason;

select * from DM_ORD_REFUND_qua_month






select * from DM_ORD_REFUND_qua_WEEK








     
     
         delete from dm_ord_refund_qua_week t1 
     where exists (select t1.*
              from dm_order_refund_week t1
             inner join tmp_dm_ord_refund_qua_week t2
                on t1.static_week = t2.static_week);
                
 create table DM_ORD_REFUND_qua_month
(
  static_month VARCHAR2(10),
  REFUND_REASON VARCHAR2(10),
  REFUND_CNT    NUMBER(10)
)

create table tmp_DM_ORD_REFUND_qua_month
(
  static_month VARCHAR2(10),
  REFUND_REASON VARCHAR2(10),
  REFUND_CNT    NUMBER(10)

)       


drop table DM_ORD_REFUND_qua_year;
     drop table tmp_DM_ORD_REFUND_qua_year;
                
                
