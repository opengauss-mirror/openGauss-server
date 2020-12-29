\c postgres
set current_schema='sql_self_tuning';
set resource_track_duration=0;
set resource_track_cost=30;
set resource_track_level=operator;

/*
 * SQL-Self Tuning scenario[5] data skew
 */

/* Test data skew */select count(*) from  t13 group by xc_node_id order by 1;
/* Test data skew */explain (analyze on ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off) 
select count(*), c1 from t13 group by 2 order by 2;

select query, query_plan, warning from pgxc_wlm_session_history where query like '%Test data skew%' order by 3,2,1;

/*
 * table in different node group can not treat as data skew
 */
select/*+hashjoin(t14 t15)leading((t15 t14))*/ count(*) table_different_node_group_01 from t14, t15 where t14.c2 = t15.c4 and t15.c1 < 10 and t14.c2 < 1000;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%table_different_node_group_01%' order by 3,2,1;

\c regression
