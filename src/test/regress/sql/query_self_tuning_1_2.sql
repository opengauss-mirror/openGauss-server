\c postgres
set current_schema='sql_self_tuning';

set resource_track_duration=0;
set resource_track_cost=30;
set resource_track_level=operator;

/*subqueryScan*/
create table self_tuning_02( a int);
insert into self_tuning_02 select count(*) from t4,t5 where
t5.c3=t4.c2 and t4.c1<10;

select warning,query from pgxc_wlm_session_history where query like '%self_tuning_02%';

/*
 * SQL-Self Tuning scenario[2] Large Table Broadcast
 */
/* Large Table in Broadcast */ select /*+ broadcast(a)*/ count(*)
from t5 a, t4 b
where a.c1=b.c2;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Large Table in Broadcast%' order by 1;

/*
 * SQL-Self Tuning scenario[3] Large Table as inner
 */
select 'not implemented' as "Result";

/*
 * SQL-Self Tuning scenario[4] Large table in Nestloop with equal-condition
 */
/* Large Table with Equal-Condition use Nestloop */
select /*+ nestloop(a b)*/ count(*)
from t13 a, t4 b
where a.c1=b.c2 and b.c2 < 20;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Large Table with Equal-Condition use Nestloop%' order by 1;

/* Large Table with Equal-Condition use Nestloop */
select /*+ nestloop(a b)*/ count(*)
from t13 a, t4 b
where a.c1=b.c2 and a.c3<b.c4 and b.c2 < 20;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Large Table with Equal-Condition use Nestloop%' order by 1;

/*
 * SQL-Self Tuning scenario[8] Check Indexscan/Seqscan
 */
set resource_track_cost=1;
/* Check Indexscan */ select count(*)
from t16 where c4=0;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Check Indexscan%' order by 1;
insert into t16 values (1,1,1025,1);
/* Check Seqscan */ select count(*)
from t16 where c3=1025;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Check Seqscan%' order by 1;

delete from ct16 where c3=3;
insert into ct16 values (0,1,3,0);
/* Check CStore Seqscan */ select count(*)
from ct16 where c3=3;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Check CStore Seqscan%' order by 1;
set enable_seqscan=off;
/* Check CStore Indexscan */ select count(*)
from ct16 where c1=1;
select query, query_plan, warning from pgxc_wlm_session_history where query like '%Check CStore Indexscan%' order by 1;

\c regression

