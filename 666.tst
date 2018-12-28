PL/SQL Developer Test script 3.0
7
begin
  -- Call the procedure
  etl_dm_opt_order_qua_refund(start_time => :start_time,
                              end_time => :end_time,
                              v_etl_number => :v_etl_number,
                              status_flag => :status_flag);
end;
4
start_time
1
2015/6/1
12
end_time
1
2015/6/3
12
v_etl_number
0
4
status_flag
0
5
0
