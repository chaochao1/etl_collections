create or replace procedure ETL_DM_SUPPLIER_REFUND_RATE(start_time   date,
                                                        end_time     date,
                                                        v_etl_number number,
                                                        status_flag  varchar2) is
  v_sql    VARCHAR2(1000);
  v_insert INT := 0;
  v_update INT := 0;
  v_delete INT := 0;

  v_date    date := start_time;
  day_cnt   int := 0;
  week_cnt  int := 0;
  month_cnt int := 0;

begin
  LOOP
    execute immediate 'truncate table tmp_dm_sup_refund_rate_day';
  
    insert into tmp_dm_sup_refund_rate_day
      (add_date, supplier_id, supplier_name, order_cnt)
      select t3.pay_time,
             t1.supplier_id,
             t1.name supplier_name,
             count(distinct t2.order_id) order_cnt
        from sis_dw.dw_DIM_SUPPLIER t1
       inner join sis_dw.DW_FACT_ORDER_GOODS t2
          on t1.supplier_id = t2.supplier_id
       inner join sis_dw.DW_FACT_ORDER t3
          on t2.order_id = t3.order_id
       where t3.pay_time >= v_date
         and t3.pay_time
             <
             v_date + 1
         and t3.total_all != 0
         and t3.is_delete = 0
         and t3.status = 1
       group by t3.pay_time,
                t1.supplier_id,
                t1.name;
    commit;
  
    insert into tmp_dm_sup_refund_rate_day
      (add_date, supplier_id, supplier_name, order_refund_cnt)
      select t3.pay_time,
             t1.supplier_id,
             t1.name supplier_name,
             count(distinct t2.order_id) order_refund_cnt
        from sis_dw.dw_DIM_SUPPLIER t1
       inner join sis_dw.DW_FACT_ORDER_GOODS t2
          on t1.supplier_id = t2.supplier_id
       inner join sis_dw.DW_FACT_ORDER t3
          on t2.order_id = t3.order_id
       inner join sis_dw.dw_fact_order_refund_105 t4
          on t2.order_id = t4.order_id
       where t3.pay_time >= v_date
         and t3.pay_time < v_date+1
         and t3.total_all != 0
         and t3.is_delete = 0
         and t3.status = 1
         and refund_type in (10, 11, 16, 29, 30)
       group by t3.pay_time,
                t1.supplier_id,
                t1.name;
  
    commit;
  
    delete from dm_sup_refund_rate_day
     where add_date in (select add_date from tmp_dm_sup_refund_rate_day);
    commit;
  
    insert into dm_sup_refund_rate_day
      (add_date, supplier_id, supplier_name, order_refund_cnt, order_cnt)
      select add_date,
             supplier_id,
             supplier_name,
             sum(order_refund_cnt),
             sum(order_cnt)
        from tmp_dm_sup_refund_rate_day t1
       group by add_date, supplier_id, supplier_name;
    commit;
  
    /*
      退货原因占比
      按周
    */
    /*
    select sum(refund_cnt) into week_cnt from bi_dm.dm_order_refund_day a left join bi_dw.dw_dim_date c on a.add_date = c.full_date_key
    where weekofyear(full_date_key)=weekofyear(v_date);
    
    
    #drop table if exists tmp_dm_order_refund_week;
    
    insert into tmp_dm_order_refund_week (week_of_year,refund_cnt,refund_reason)
    #create table  if not exists tmp_dm_order_refund_day as
    select week_of_year,sum(refund_cnt),refund_reason from bi_dm.dm_order_refund_day a left join bi_dw.dw_dim_date c on a.add_date = c.full_date_key
    where weekofyear(full_date_key)=weekofyear(v_date)
    group by week_of_year,refund_reason;
    commit;
    
    delete from dm_order_refund_week 
    where week_of_year in (select week_of_year from tmp_dm_order_refund_week);
    commit;
    
    insert into dm_order_refund_week (week_of_year,refund_reason,refund_cnt,refund_rate)
    select week_of_year,refund_reason,refund_cnt,round(refund_cnt/week_cnt,4) from tmp_dm_order_refund_week
    group by week_of_year,refund_reason;
    commit;
    
    /*
      退货原因占比
      按月
    */
    /*
    select sum(refund_cnt) into month_cnt from bi_dm.dm_order_refund_day a left join bi_dw.dw_dim_date c on a.add_date = c.full_date_key
    where month(full_date_key)=month(v_date) ;
    
    
    truncate table tmp_dm_order_refund_month;
    
    insert into tmp_dm_order_refund_month (static_month,refund_cnt,refund_reason)
    select static_month,sum(refund_cnt),refund_reason from bi_dm.dm_order_refund_day a left join bi_dw.dw_dim_date c on a.add_date = c.full_date_key
    where month(full_date_key)=month(v_date)
    group by static_month,refund_reason;
    commit;
    
    delete from dm_order_refund_month 
    where static_month in (select static_month from tmp_dm_order_refund_month);
    commit;
    
    insert into dm_order_refund_month (static_month,refund_reason,refund_cnt,refund_rate)
    select static_month,refund_reason,refund_cnt,round(refund_cnt/month_cnt,4) from tmp_dm_order_refund_month
    group by static_month,refund_reason;
    commit;
    
    /*
      退货率
        按天
    */
    /*
    drop table if exists tmp_dm_order_refund_rate_day;
    create table  if not exists tmp_dm_order_refund_rate_day as
    select FROM_UNIXTIME( pay_time, '%Y-%m-%d') add_date,count(1) order_cnt from bi_dw.dw_fact_order
    where FROM_UNIXTIME( pay_time, '%Y-%m-%d')>=v_date and FROM_UNIXTIME( pay_time, '%Y-%m-%d')<date_add(v_date, INTERVAL 1 day)
    and total_all!=0
    and is_delete=0
    and status=1
    group by FROM_UNIXTIME( pay_time, '%Y-%m-%d');
    #and site_from in ('www', 'm', 'fr', 'es', 'de', 'ru', 'us', 'ios', 'it');
    commit;
    
    delete from dm_order_refund_rate_day 
    where add_date in (select add_date from tmp_dm_order_refund_rate_day);
    commit;
    
    insert into dm_order_refund_rate_day(add_date,order_cnt,order_refund_rate)
    select add_date,order_cnt,round(day_cnt/order_cnt,4) from tmp_dm_order_refund_rate_day;
    commit;
    
    /*
      退货率
        按周
    */
    /*
    truncate table tmp_dm_order_refund_rate_week;
    
    insert into tmp_dm_order_refund_rate_week(week_of_year,order_cnt)
    select week_of_year,sum(order_cnt) from dm_order_refund_rate_day a left join bi_dw.dw_dim_date c on a.add_date = c.full_date_key
    where weekofyear(full_date_key)=weekofyear(v_date)
    group by week_of_year;
    commit;
    
    delete from dm_order_refund_rate_week 
    where week_of_year in (select week_of_year from tmp_dm_order_refund_rate_week);
    commit;
    
    insert into dm_order_refund_rate_week(week_of_year,order_cnt,order_refund_rate)
    select week_of_year,order_cnt,round(week_cnt/order_cnt,4) from tmp_dm_order_refund_rate_week;
    commit;
    
    /*
      退货率
        按月
    */
  
    execute immediate '  truncate table tmp_dm_sup_refund_rate_month';
  
    insert into tmp_dm_sup_refund_rate_month
      (static_month,
       supplier_id,
       supplier_name,
       order_cnt,
       order_refund_cnt,
       order_refund_rate)
      select static_month,
             supplier_id,
             supplier_name,
             sum(order_cnt),
             sum(order_refund_cnt),
             round(sum(order_refund_cnt) / sum(order_cnt), 4)
        from dm_sup_refund_rate_day a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where extract(month from full_date_key) = extract(month from v_date)
         and extract(year from full_date_key) =
             extract(year from full_date_key)
       group by static_month, supplier_id, supplier_name;
    commit;
  
    delete from dm_sup_refund_rate_month a
     where exists (select 1
              from tmp_dm_sup_refund_rate_month t1
             where a.static_month = t1.static_month
               and a.supplier_id = t1.supplier_id);
  
    /*#where static_month in (
    select static_month from tmp_dm_supplier_refund_rate_month);*/
    commit;
  
    insert into dm_sup_refund_rate_month
      (static_month,
       supplier_id,
       supplier_name,
       order_cnt,
       order_refund_cnt,
       order_refund_rate)
      select static_month,
             supplier_id,
             supplier_name,
             order_cnt,
             order_refund_cnt,
             order_refund_rate
        from tmp_dm_sup_refund_rate_month;
        commit;
  
    v_date := v_date + 1;
    if v_date > end_time then
      exit;
    end if;
  end loop;

EXCEPTION
  WHEN OTHERS THEN
    sis_dw.etl_md_etl_log_detail(v_etl_number,
                                 'dw_micen_account_msg_d',
                                 SQLERRM,
                                 v_insert,
                                 v_update,
                                 v_delete);
  
end ETL_DM_SUPPLIER_REFUND_RATE;
/
