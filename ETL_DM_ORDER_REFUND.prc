CREATE OR REPLACE PROCEDURE ETL_DM_ORDER_REFUND(start_time   date,
                                                end_time     date,
                                                v_etl_number int,
                                                status_flag  varchar) IS
  /*by sunchao*/
  v_sql     VARCHAR2(1000);
  v_insert  INT := 0;
  v_update  INT := 0;
  v_delete  INT := 0;
  v_date    date := start_time;
  day_cnt   int := 0;
  week_cnt  int := 0;
  month_cnt int := 0;

BEGIN
  LOOP
  
    select count(1)
      into day_cnt
      from sis_dw.dw_fact_order_refund_105 a
     inner join sis_dw.dw_fact_order b
        on a.order_id = b.order_id
     where b.pay_time  >=
           v_date
       and  b.pay_time  <
           (v_date + 1)
       and refund_type in (10, 11, 12, 13, 14, 16, 29, 30);
  
    execute immediate 'truncate table  tmp_dm_order_refund_day';
    /*
    insert into tmp_dm_order_refund_day (add_date,refund_type,refund_cnt,refund_reason)
    select FROM_UNIXTIME( add_time, '%Y-%m-%d'),refund_type,count(1),refund_type_reason from bi_dw.dw_fact_order_refund a left join bi_dw.dw_dim_order_refund_type b on a.refund_type = b.id
    where FROM_UNIXTIME( add_time, '%Y-%m-%d')>=v_date and FROM_UNIXTIME( add_time, '%Y-%m-%d')<date_add(v_date, INTERVAL 1 day) and refund_type_reason in (
    '材质','污渍','色差','尺码','超时','发错货','不合适','破损')
    group by FROM_UNIXTIME(add_time, '%Y-%m-%d'),refund_type;*/
  
    insert into tmp_dm_order_refund_day
      (add_date, refund_type, refund_count)
      select  b.pay_time,
             a.refund_type,
             count(1)
        from sis_dw.dw_fact_order_refund_105 a
       inner join sis_dw.dw_fact_order b
          on a.order_id = b.order_id
       where  b.pay_time  >=
             v_date
         and  b.pay_time <
             v_date + 1
         and a.refund_type in (10, 11, 12, 13, 14, 16, 29, 30)
       group by  b.pay_time,
                a.refund_type;
  
    commit;
  
    delete from dm_order_refund_day
     where add_date in (select add_date from tmp_dm_order_refund_day);
    commit;
  
    insert into dm_order_refund_day
      (add_date, refund_reason, refund_cnt, refund_rate)
      select add_date,
             case
               when refund_type_reason = '不合适' then
                '无理由'
               else
                refund_type_reason
             end,
             refund_count,
             round(refund_count / day_cnt, 4)
        from tmp_dm_order_refund_day t1
        left join sis_dw.dw_dim_order_refund_type t2
          on t1.refund_type = t2.id;
    commit;
  
    /*
      退货原因占比
      按周
    */
  
    select nvl(sum(refund_cnt), 0)
      into week_cnt
      from sis_dm.dm_order_refund_day a
      left join sis_dw.dw_dim_date c
        on a.add_date = c.full_date_key
     where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
           TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
       and extract(year from full_date_key) = extract(year from v_date);
  
    /*drop table if exists tmp_dm_order_refund_week;*/
    execute immediate 'truncate table tmp_dm_order_refund_week';
    insert into sis_dm.tmp_dm_order_refund_week
      (static_week, week_of_year, refund_cnt, refund_reason)
      select c.static_week,
             c.week_of_year,
             sum(a.refund_cnt),
             a.refund_reason
        from sis_dm.dm_order_refund_day a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
             TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
         and extract(year from full_date_key) = extract(year from v_date)
       group by c.static_week, c.week_of_year, a.refund_reason;
    commit;
  
    delete from dm_order_refund_week t1
     where exists (select t1.*
              from dm_order_refund_week t1
             inner join tmp_dm_order_refund_week t2
                on t1.static_week = t2.static_week);
    /*??????????????#and t1.week_of_year=t2.week_of_year;*/
    commit;
  
    insert into dm_order_refund_week
      (static_week, week_of_year, refund_reason, refund_cnt, refund_rate)
      select static_week,
             week_of_year,
             refund_reason,
             refund_cnt,
             round(refund_cnt / week_cnt, 4)
        from tmp_dm_order_refund_week;
    commit;
  
    /*
      退货原因占比
      按月
    */
  
    select sum(refund_cnt)
      into month_cnt
      from sis_dm.dm_order_refund_day a
      left join sis_dw.dw_dim_date c
        on a.add_date = c.full_date_key
     where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
           TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
       and extract(year from full_date_key) = extract(year from v_date);
  
    execute immediate 'truncate table tmp_dm_order_refund_month';
  
    insert into tmp_dm_order_refund_month
      (static_month, refund_cnt, refund_reason)
      select static_month, sum(refund_cnt), refund_reason
        from sis_dm.dm_order_refund_day a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
             TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
         and extract(year from full_date_key) = extract(year from v_date)
       group by static_month, refund_reason,refund_cnt;
    commit;
  
    delete from sis_dm.dm_order_refund_month
     where static_month in
           (select static_month from tmp_dm_order_refund_month);
    commit;
  
    insert into dm_order_refund_month
      (static_month, refund_reason, refund_cnt, refund_rate)
      select static_month,
             refund_reason,
             refund_cnt,
             round(refund_cnt / month_cnt, 4)
        from tmp_dm_order_refund_month
       group by static_month, refund_reason, refund_cnt;
    commit;
  
    /*
      退货率
        按天
    */
    execute immediate 'truncate  table  tmp_dm_order_refund_rate_day';
  
    insert into tmp_dm_order_refund_rate_day
      (add_date, order_cnt)
      select  pay_time,
             count(1)
        from sis_dw.dw_fact_order
       where pay_time >=
             v_date
         and pay_time   <  v_date + 1
         and total_all != 0
         and is_delete = 0
         and status = 1
       group by pay_time;
  
    /*#and site_from in ('www', 'm', 'fr', 'es', 'de', 'ru', 'us', 'ios', 'it');*/
    commit;
  
    delete from dm_order_refund_rate_day
     where add_date in (select add_date from tmp_dm_order_refund_rate_day);
    commit;
  
    insert into dm_order_refund_rate_day
      (add_date, order_cnt, order_refund_rate)
      select add_date, order_cnt, round(day_cnt / order_cnt, 4)
        from tmp_dm_order_refund_rate_day;
    commit;
  
    /*
      退货率
        按周
    */
  
    execute immediate 'truncate table tmp_dm_order_refund_rate_week';
  
    insert into tmp_dm_order_refund_rate_week
      (static_week, week_of_year, order_cnt)
      select static_week, week_of_year, sum(order_cnt)
        from dm_order_refund_rate_day a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
             TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
         and extract(year from full_date_key) = extract(year from v_date)
       group by extract(year from full_date_key),static_week, week_of_year,order_cnt;
    commit;
  
    delete from dm_order_refund_rate_week t1
     where exists (select t1.*
              from dm_order_refund_rate_week t1
             inner join tmp_dm_order_refund_rate_week t2
                on t1.static_week = t2.static_week); /*#and t1.week_of_year=t2.week_of_year;*/
    commit;
  
    insert into dm_order_refund_rate_week
      (static_week, week_of_year, order_cnt, order_refund_rate)
      select static_week,
             week_of_year,
             order_cnt,
             round(week_cnt / order_cnt, 4)
        from tmp_dm_order_refund_rate_week;
    commit;
  
    /*
      退货率
        按月
    */
  
    execute immediate 'truncate table tmp_dm_order_refund_rate_month';
  
    insert into tmp_dm_order_refund_rate_month
      (static_month, order_cnt)
      select static_month, sum(order_cnt)
        from dm_order_refund_rate_day a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where extract(month from full_date_key) = extract(month from v_date)
         and extract(year from full_date_key) = extract(year from v_date)
       group by static_month;
    commit;
  
    delete from dm_order_refund_rate_month
     where static_month in
           (select static_month from tmp_dm_order_refund_rate_month);
    commit;
  
    insert into dm_order_refund_rate_month
      (static_month, order_cnt, order_refund_rate)
      select static_month, order_cnt, round(month_cnt / order_cnt, 4)
        from tmp_dm_order_refund_rate_month;
    commit;
  
    v_date := v_date + 1;
  
    IF v_date > end_time THEN
      EXIT;
    END IF;
  END LOOP;

EXCEPTION
  WHEN OTHERS THEN
    sis_dw.etl_md_etl_log_detail(v_etl_number,
                                 'dw_micen_account_msg_d',
                                 SQLERRM,
                                 v_insert,
                                 v_update,
                                 v_delete);
END ETL_DM_ORDER_REFUND;
/
