create or replace procedure ETL_DM_OPT_ORDER_QUA_REFUND(start_time   date,
                                                        end_time     date,
                                                        v_etl_number int,
                                                        status_flag  varchar) IS
  /*by sunchao
  modify by liushaowu 20150513
  */
  v_sql               VARCHAR2(1000);
  v_insert            INT := 0;
  v_update            INT := 0;
  v_delete            INT := 0;
  v_date              date := start_time;
  day_refund_cnt      int := 0; /*每日退款订单总数*/
  day_order_cnt       int := 0; /*每日订单总数*/
  week_refund_qua_cnt int := 0; /*每周退款订单总数*/
  day_refund_qua_cnt  int := 0;

  week_cnt        int := 0;
  week_order_cnt  int := 0;
  month_cnt       int := 0;
  month_order_cnt int := 0;

BEGIN
  LOOP
    select count(distinct order_id)
      into day_refund_qua_cnt
      from sis_dw.dw_fact_order_ref_105 a
     where a.add_time >= v_date
       and a.add_time < (v_date + 1)
       and refund_type in (10, 11, 16, 26, 29, 30);
  
    select count(distinct order_id)
      into day_refund_cnt
      from sis_dw.dw_fact_order_ref_105 a
     where a.add_time >= v_date
       and a.add_time < (v_date + 1);
  
    select count(distinct order_id)
      into day_order_cnt
      from sis_dw.dw_fact_order a
     where a.add_time >= v_date
       and a.add_time < (v_date + 1);
  
    execute immediate 'truncate table  TMP_DM_ORDER_REFUND_QUA_DAY';
  
    /*按天统计各原因质量问题*/
  
    insert into TMP_DM_ORDER_REFUND_QUA_DAY
      (add_date, refund_type, t_refund_cnt)
      select trunc(a.add_time), a.refund_type, count(distinct order_id)
        from sis_dw.dw_fact_order_ref_105 a
       where a.add_time >= v_date
         and a.add_time < v_date + 1
         and a.refund_type in (10, 11, 16, 26, 29, 30)
       group by trunc(a.add_time), a.refund_type;
    commit;
  
    /*  insert into TMP_DM_ORDER_REFUND_QUA_DAY
     (add_date, refund_type, refund_qua_cnt,refund_cnt,order_cnt)
     select  trunc(a.add_time),
             a.refund_type,
             count(1)
          from sis_dw.dw_fact_order_ref_105 a
       inner join sis_dw.dw_fact_order b
            on a.order_id = b.order_id
      where  a.add_time >=
            v_date
        and  a.add_time <
            v_date + 1
        and a.refund_type in (10,11,16,26,29,30)
        group by trunc(a.add_time),
             a.refund_type;
    commit;    
    */
  
    delete from DM_ORDER_REFUND_QUA_DAY
     where add_date in (select add_date from TMP_DM_ORDER_REFUND_QUA_DAY);
    commit;
  
    /*插入线上数据库*/
    insert into DM_ORDER_REFUND_QUA_DAY
      (add_date, refund_reason, t_refund_cnt)
      select trunc(add_date),
             case
               when refund_type_reason = '不合适' then
                '无理由'
               else
                t2.refund_type_reason
             end,
             t_refund_cnt
        from TMP_DM_ORDER_REFUND_QUA_DAY t1
        left join sis_dw.dw_dim_order_refund_type t2
          on t1.refund_type = t2.id;
    commit;
  
    /*按周进行统计*/
  
    select nvl(sum(a.t_refund_cnt), 1)
      into week_cnt
      from DM_ORDER_REFUND_QUA_DAY a
      left join sis_dw.dw_dim_date c
        on trunc(a.add_date) = c.full_date_key
     where to_char(To_Date(c.full_date_key, 'YYYY/MM/DD'), 'IYIW') =
           to_char(To_Date(v_date, 'YYYY/MM/DD'), 'IYIW')
       and extract(year from full_date_key) = extract(year from v_date);
  
    select count(a.order_id)
      into week_order_cnt
      from sis_dw.dw_fact_order a
      left join sis_dw.dw_dim_date c
        on trunc(a.add_time) = c.full_date_key
     where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
           TO_CHAR(TO_DATE('2015/5/15', 'YYYY/MM/DD'), 'IYIW')
       and extract(year from full_date_key) = extract(year from v_date);
  
    execute immediate 'TMP_DM_ORDER_REFUND_CNT_RATE';
  
    insert into TMP_DM_ORDER_REFUND_CNT_RATE
      (Problem_type,
       count_type,
       static_date,
       refund_reason,
       refund_cnt,
       refund_rate,
       order_rate)
      select '质量问题',
             'week',
             c.static_week,
             a.refund_reason,
             sum(a.t_refund_cnt),
             (sum(a.t_refund_cnt) / week_cnt),
             (sum(a.t_refund_cnt) / week_order_cnt)
        from DM_ORDER_REFUND_QUA_DAY a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where TO_CHAR(TO_DATE(full_date_key, 'YYYY/MM/DD'), 'IYIW') =
             TO_CHAR(TO_DATE(v_date, 'YYYY/MM/DD'), 'IYIW')
         and extract(year from full_date_key) = extract(year from v_date)
       group by '质量问题', 'week', c.static_week, a.refund_reason;
    commit;
  
    delete from DM_ORDER_REFUND_CNT_RATE t1
     where exists (select t1.*
              from DM_ORDER_REFUND_CNT_RATE t1
             inner join TMP_DM_ORDER_REFUND_CNT_RATE t2
                on t1.problem_type = t2.problem_type
               and t1.count_type = t2.count_type
               and t1.static_date = t2.static_date);
  
    commit;
  
    /*插入线上数据库*/
  
    insert into DM_ORDER_REFUND_CNT_RATE
      (Problem_type,
       count_type,
       static_date,
       refund_reason,
       refund_cnt,
       refund_rate,
       order_rate)
      select Problem_type,
       count_type,
       static_date,
       refund_reason,
       refund_cnt,
       refund_rate,
       order_rate
        from TMP_DM_ORDER_REFUND_CNT_RATE;
    commit;
  
    /*按月进行统计*/
  
    select nvl(sum(a.t_refund_cnt), 0)
      into month_cnt
      from DM_ORDER_REFUND_QUA_DAY a
      left join sis_dw.dw_dim_date c
        on trunc(a.add_date) = c.full_date_key
     where extract(month from full_date_key) = extract(month from v_date)
       and extract(year from full_date_key) = extract(year from v_date);
  
    select count(a.order_id)
      into month_order_cnt
      from sis_dw.dw_fact_order a
      left join sis_dw.dw_dim_date c
        on trunc(a.add_time) = c.full_date_key
     where extract(month from full_date_key) = extract(month from v_date)
       and extract(year from full_date_key) = extract(year from v_date);
  
    execute immediate 'TMP_DM_ORDER_REFUND_CNT_RATE';
  
    insert into TMP_DM_ORDER_REFUND_CNT_RATE
      (Problem_type,
       count_type,
       static_date,
       refund_reason,
       refund_cnt,
       refund_rate,
       order_rate)
      select '质量问题',
             'month',
             c.static_month,
             a.refund_reason,
             sum(a.t_refund_cnt),
             (sum(a.t_refund_cnt) / week_cnt),
             (sum(a.t_refund_cnt) / week_order_cnt)
        from DM_ORDER_REFUND_QUA_DAY a
        left join sis_dw.dw_dim_date c
          on a.add_date = c.full_date_key
       where extract(month from full_date_key) = extract(month from v_date)
         and extract(year from full_date_key) = extract(year from v_date)
       group by '质量问题', 'month', c.static_month, a.refund_reason;
    commit;
  
  
  
  
    delete from DM_ORDER_REFUND_CNT_RATE t1
     where exists (select t1.*
              from DM_ORDER_REFUND_CNT_RATE t1
             inner join TMP_DM_ORDER_REFUND_CNT_RATE t2
                on t1.problem_type = t2.problem_type
               and t1.count_type = t2.count_type
               and t1.static_date = t2.static_date);
    commit;
    
    
    insert into DM_ORDER_REFUND_CNT_RATE 
           (Problem_type,
       count_type,
       static_date,
       refund_reason,
       refund_cnt,
       refund_rate,
       order_rate)
      select
       Problem_type,
       count_type,
       static_date,
       refund_reason,
       refund_cnt,
       refund_rate,
       order_rate
       from TMP_DM_ORDER_REFUND_CNT_RATE;
  
    sis_dw.etl_md_etl_log_detail(v_etl_number,
                                 'DM_ORDER_REFUND_CNT_RATE' ||
                                 to_char(v_date, 'yyyymmdd'),
                                 '',
                                 v_insert,
                                 v_update,
                                 v_delete);
  
    v_date := v_date + 1;
    IF v_date > end_time THEN
      EXIT;
    END IF;
  END LOOP;

  sis_dw.etl_md_etl_log_detail(v_etl_number,
                               'DM_ORDER_REFUND_CNT_RATE',
                               '',
                               v_insert,
                               v_update,
                               v_delete);

EXCEPTION
  WHEN OTHERS THEN
    sis_dw.etl_md_etl_log_detail(v_etl_number,
                                 'DM_ORDER_REFUND_CNT_RATE',
                                 SQLERRM,
                                 v_insert,
                                 v_update,
                                 v_delete);
END ETL_DM_OPT_ORDER_QUA_REFUND;
/
