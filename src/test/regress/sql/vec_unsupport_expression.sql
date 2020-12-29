/*
 * This file is used to test the vector engine un-support expressions
 * We support these expressions by rewriting its plan to pull up these expression
 * And use vec_to_row to convert it to use row engine
 */
/************************************************************
Expression Type:
    T_ArrayRef
    T_AlternativeSubPlan
    T_FieldSelect
    T_FieldStore
    T_ArrayCoerceExpr
    T_ConvertRowtypeExpr
    T_ArrayExpr
    T_RowExpr
    T_XmlExpr
    T_CoerceToDomain
    T_CoerceToDomainValue
    T_CurrentOfExpr

Using Type:
    qual
    targetlist
*************************************************************/

create schema vec_unsupport_expression;
set current_schema=vec_unsupport_expression;

create table t1(c1 int, c2 int, c3 int) with (orientation=column) distribute by hash(c1);
insert into t1 select v,v,v from generate_series(1,12) as v;
create table t2(c1 int, c2 int, c3 int) with (orientation=column) distribute by hash(c1);
insert into t2 select v,v,v from generate_series(1,10) as v;

select c2,string_to_array(c2,'a') as arr from t1 where c2=arr[1] order by 1;
explain (costs off, verbose on) select c2,string_to_array(c2,'a') as arr from t1 where c2=arr[1] order by 1;

select c2,string_to_array(c2,'a') as arr from t1 where c2=arr[1]+1 order by 1;
explain (costs off, verbose on) select c2,string_to_array(c2,'a') as arr from t1 where c2=arr[1]+1 order by 1;

select c1,c2,string_to_array(c2,'a') as arr from t1 where t1.c2 in (3,4,7) and t1.c2=arr[1] order by 1,2;
explain (costs off, verbose on) select c1,c2,string_to_array(c2,'a') as arr from t1 where t1.c2 in (3,4,7) and t1.c2=arr[1] order by 1,2;

select c1,c3,string_to_array(c2,'a') as arr from t1 where t1.c2 in (3,4,7) and t1.c2=arr[1] order by 1,2;
explain (costs off, verbose on) select c1,c3,string_to_array(c2,'a') as arr from t1 where t1.c2 in (3,4,7) and t1.c2=arr[1] order by 1,2;

select c1,c3,string_to_array(c2,'a') as arr from t1 where t1.c3=arr[1] and c3 in (3,4,7) order by 1;
explain (costs off, verbose on) select c1,c3,string_to_array(c2,'a') as arr from t1 where t1.c3=arr[1] and c3 in (3,4,7) order by 1;

select t1.c1 as c1, t2.c2 as c2,string_to_array(t1.c2,'a') as arr
from t1,t2
where t1.c1 = t2.c1 and t1.c1 in (3,4,5,6) and t1.c1=arr[1]
order by 1,2;
explain (costs off, verbose on) select t1.c1 as c1, t2.c2 as c2,string_to_array(t1.c2,'a') as arr
from t1,t2
where t1.c1 = t2.c1 and t1.c1 in (3,4,5,6) and t1.c1=arr[1]
order by 1,2;

select * from
(
    select t1.c1 as c1, t2.c2 as c2,string_to_array(t1.c2,'a') as arr, count(*) as sum
    from t1,t2
    where t1.c1 = t2.c1
    group by 1,2,t1.c2
) as dt where dt.c1 in (3,4,5,6) or dt.c1=arr[1]
order by 1,2,3;
explain (costs off, verbose on) select * from
(
    select t1.c1 as c1, t2.c2 as c2,string_to_array(t1.c2,'a') as arr, count(*) as sum
    from t1,t2
    where t1.c1 = t2.c1
    group by 1,2,t1.c2
) as dt where dt.c1 in (3,4,5,6) or dt.c1=arr[1]
order by 1,2,3;

select t1.c2,string_to_array(t1.c2,'a') as arr,count(*) from t1 where t1.c1 in (1,2,3) group by t1.c2 having t1.c2=arr[1] order by 1;
explain (costs off, verbose on) select t1.c2,string_to_array(t1.c2,'a') as arr,count(*) from t1 where t1.c1 in (1,2,3) group by t1.c2 having t1.c2=arr[1] order by 1;

select t1.c2,string_to_array(t1.c2,'a') as arr,count(*) from t1 where t1.c1=arr[1] group by t1.c2 having t1.c2=arr[1] order by 1;
explain (costs off, verbose on) select t1.c2,string_to_array(t1.c2,'a') as arr,count(*) from t1 where t1.c1=arr[1] group by t1.c2 having t1.c2=arr[1] order by 1;

select t1.c3, t1.c2 from t1 where t1.c1 > 1 AND t1.c2 in (select t2.c2 from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1]) order by 1,2;
explain (costs off, verbose on) select t1.c3, t1.c2 from t1 where t1.c1 > 1 AND t1.c2 in (select t2.c2 from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1]) order by 1,2;
 
select t1.c2, sum(t1.c3) from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1] group by t1.c2 order by t1.c2 limit 2;
explain (costs off, verbose on) select t1.c2, sum(t1.c3) from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1] group by t1.c2 order by t1.c2 limit 2;

select max(t1.c2) from t1 where t1.c1 < 4 and t1.c1=(string_to_array(t1.c2,'a'))[1];
explain (costs off, verbose on) select max(t1.c2) from t1 where t1.c1 < 4 and t1.c1=(string_to_array(t1.c2,'a'))[1];

select t1.c2, t2.c3, count(*) from t1,t2 where t1.c1 = t2.c2 and t1.c1=(string_to_array(t1.c2,'a'))[1] group by t1.c2,t2.c3 order by t1.c2;
explain (costs off, verbose on) select t1.c2, t2.c3, count(*) from t1,t2 where t1.c1 = t2.c2 and t1.c1=(string_to_array(t1.c2,'a'))[1] group by t1.c2,t2.c3 order by t1.c2;

select t1.c2, t2.c3, sum(t1.c3) from t1,t2 where t1.c1 = t2.c2 and t1.c1=(string_to_array(t1.c2,'a'))[1] group by t1.c2,t2.c3 order by t1.c2 limit 2;
explain (costs off, verbose on) select t1.c2, t2.c3, sum(t1.c3) from t1,t2 where t1.c1 = t2.c2 and t1.c1=(string_to_array(t1.c2,'a'))[1] group by t1.c2,t2.c3 order by t1.c2 limit 2;

select c1 from (select * from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1] union all select * from t2) as dt order by 1;
explain (costs off, verbose on) select c1 from (select * from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1] union all select * from t2) as dt order by 1;

select c1 from (select * from t1 union all select * from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1]) as dt order by 1;
explain (costs off, verbose on) select c1 from (select * from t1 union all select * from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1]) as dt order by 1;

select * from (select * from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1] union all select * from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1]) as dt order by 1;
explain (costs off, verbose on) select * from (select * from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1] union all select * from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1]) as dt order by 1;

select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + 2)=(string_to_array(t2.c2,'a'))[1] order by 1;
explain (costs off, verbose on) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + 2)=(string_to_array(t2.c2,'a'))[1] order by 1;

select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c2 *2)=(string_to_array(t2.c2,'a'))[1] order by 1;
explain (costs off, verbose on) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c2 *2)=(string_to_array(t2.c2,'a'))[1] order by 1;

select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + t2.c2)=(string_to_array(t2.c2,'a'))[1] order by 1;
explain (costs off, verbose on) select t1.c1, t2.c1 from t1, t2 where t1.c1 = t2.c1 and (t1.c1 + t2.c2)=(string_to_array(t2.c2,'a'))[1] order by 1;

with q1(x) as (select * from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1])
select * from q1 where q1.x=(string_to_array(x,'a'))[1] order by 1,2,3;
explain (costs off, verbose on) with q1(x) as (select * from t1 where t1.c1=(string_to_array(t1.c2,'a'))[1])
select * from q1 where q1.x=(string_to_array(x,'a'))[1] order by 1,2,3;

select t1.c1 from t1,t2 where t1.c1=t2.c1 and t1.c1=(string_to_array(t1.c2,'a'))[1] and t1.c2 =1;
explain (costs off, verbose on) select t1.c1 from t1,t2 where t1.c1=t2.c1 and t1.c1=(string_to_array(t1.c2,'a'))[1] and t1.c2 =1;

select t1.c1 from t2, t1 where t1.c1=t2.c1 and t1.c1=(string_to_array(t1.c2,'a'))[1] and t1.c2 =1;
explain (costs off, verbose on) select t1.c1 from t2, t1 where t1.c1=t2.c1 and t1.c1=(string_to_array(t1.c2,'a'))[1] and t1.c2 =1;

select * from t1 where t1.c2 in (select c1 from t2 where 1=1) and c2=(string_to_array(t1.c2,'a'))[1] order by 1,2,3;
explain (costs off, verbose on) select * from t1 where t1.c2 in (select c1 from t2 where 1=1) and c2=(string_to_array(t1.c2,'a'))[1] order by 1,2,3;

select * from t1 where t1.c2 in (select c1 from t2 where 1=1 and c2=(string_to_array(t2.c2,'a'))[1]) and c2=(string_to_array(t1.c2,'a'))[1] order by 1,2,3;
explain (costs off, verbose on) select * from t1 where t1.c2 in (select c1 from t2 where 1=1 and c2=(string_to_array(t2.c2,'a'))[1]) and c2=(string_to_array(t1.c2,'a'))[1] order by 1,2,3;

set enable_hashjoin to off;
set enable_nestloop to off;

explain (costs off, verbose on)
select string_to_array(t1.c1, '-') arr2, string_to_array(t2.c1, '-') arr3
from t1, t2
where arr2[1] = arr3[1] order by 1,2;
select string_to_array(t1.c1, '-') arr2, string_to_array(t2.c1, '-') arr3
from t1, t2
where arr2[1] = arr3[1] order by 1,2;

reset enable_hashjoin;
reset enable_nestloop;

--- insert/update/delete
update t1 set c2 = c2 * 10 where t1.c1 in (select t2.c2 from t2 where t2.c1=(string_to_array(t1.c2,'a'))[1] limit 2);
explain (costs off, verbose on) update t1 set c2 = c2 * 10 where t1.c1 in (select t2.c2 from t2 where t2.c1=(string_to_array(t1.c2,'a'))[1] limit 2);

INSERT into t1 select * from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1] and t2.c2<2;
explain (costs off, verbose on) INSERT into t1 select * from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1] and t2.c2<2;

DELETE from t1 where t1.c1 in (select c2 from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1] and t2.c3=10 limit 1);
explain (costs off, verbose on) DELETE from t1 where t1.c1 in (select c2 from t2 where t2.c1=(string_to_array(t2.c2,'a'))[1] and t2.c3=10 limit 1);

create table t10(c1 int, c2 int, c3 varchar, c4 nvarchar2) with (orientation=column) distribute by hash (c1);
insert into t10 select v,v,v,v from generate_series(1,10) as v;

select * from t10 where cast(c1 as numeric)=(string_to_array(t10.c2,'a'))[1] and c2 in (5,6,7,8) order by 1;
explain (costs off, verbose on) select * from t10 where cast(c1 as numeric)=(string_to_array(t10.c2,'a'))[1] and c2 in (5,6,7,8) order by 1;

select * from t10 where c4=(string_to_array(t10.c2,'4'))[2] order by 1;
explain (costs off, verbose on) select * from t10 where c4=(string_to_array(t10.c2,'4'))[2] order by 1;

select c1 from t10 where c1=(string_to_array(t10.c2,'a'))[1] and 12 = ANY (array[2275,1245,12]) order by 1 limit 1;
explain (costs off, verbose on) select c1 from t10 where c1=(string_to_array(t10.c2,'a'))[1] and 12 = ANY (array[2275,1245,12]) order by 1 limit 1;

--- T_RowExpr
CREATE TABLE t2_col(a int, b int)with (orientation = column);
insert into t2_col values (1,2);
select a from t2_col where table_data_skewness(row(a), 'H') > 5;
explain (costs off, verbose on) select a from t2_col where table_data_skewness(row(a), 'H') > 5;

/* add_key_column_for_plan create_stream_plan*/
select a from t2_col join t1 on table_data_skewness(row(a), 'H')=t1.c1;
explain (costs off, verbose on) select a from t2_col join t1 on table_data_skewness(row(a), 'H')=t1.c1;

select table_data_skewness(row(a), 'H') from t2_col;
explain (costs off, verbose on) select table_data_skewness(row(a), 'H') from t2_col;

select * from table_skewness('t2_col', 'a') order by 1, 2, 3;
explain (costs off, verbose on) select * from table_skewness('t2_col', 'a') order by 1, 2, 3;

select table_data_skewness(row(a), 'H') from t2_col where table_data_skewness(row(a), 'H') > 5; 
explain (costs off, verbose on) select table_data_skewness(row(a), 'H') from t2_col where table_data_skewness(row(a), 'H') > 5;

explain (costs off, verbose on) select table_data_skewness(row(a), 'H') from t2_col join t1 on table_data_skewness(row(a), 'H')=t1.c1;


--- partition table
create table test3(
c1 int default 101, 
c2 char(100) default 'abcdafeawr',
c3 date default '2018-01-01',
c4 text default 'afeaerw',
c5 numeric(38,15))with(orientation=column) 
partition by range(c1)
(partition p1 values less than(100),
 partition p2 values less than(200),
 partition p3 values less than(300),
 partition p4 values less than(500));

select c2, string_to_array(c2,'o','i') as arref from test3 where c2=arref[1];
explain (costs off, verbose on) select c2, string_to_array(c2,'o','i') as arref from test3 where c2=arref[1];

-- partition table have unsupport expr, the upper stream operator need add result operator--
set enable_broadcast=off;
select string_to_array(t1.c2, '-') arr1, string_to_array(test3.c2, '-') arr2 from t1, test3 where  arr2[1]=arr1[1];
explain (costs off, verbose on) select string_to_array(t1.c2, '-') arr1, string_to_array(test3.c2, '-') arr2 from t1, test3 where  arr2[1]=arr1[1];
set enable_broadcast=on;

-- nestloop parameterized path--
set enable_mergejoin=off;
set enable_hashjoin=off;
create index index11 on t1(c1);
create index index21 on t2(c1);
select regexp_split_to_array(t1.c2, '-') arr1
from t1,t2
where t1.c1=t2.c1
and arr1[1]=t1.c2
and t1.c2 = 10;
explain (costs off, verbose on) select regexp_split_to_array(t1.c2, '-') arr1
from t1,t2
where t1.c1=t2.c1
and arr1[1]=t1.c2
and t1.c2 = 10;

select regexp_split_to_array(t2.c2, '-') arr1
from t1,t2
where t1.c1=t2.c1
and arr1[1]=t2.c2
and t2.c2 = 10;
explain (costs off, verbose on) select regexp_split_to_array(t2.c2, '-') arr1
from t1,t2
where t1.c1=t2.c1
and arr1[1]=t2.c2
and t2.c2 = 10;

select t1.c1, regexp_split_to_array(t1.c2, '-') arr1, regexp_split_to_array(t2.c2, '-') arr2
from t1,t2
where t1.c1=t2.c1
and arr1[1]=t1.c2
and arr2[1]=t2.c2
order by 1,2,3;
explain (costs off, verbose on) select t1.c1, regexp_split_to_array(t1.c2, '-') arr1, regexp_split_to_array(t2.c2, '-') arr2
from t1,t2
where t1.c1=t2.c1
and arr1[1]=t1.c2
and arr2[1]=t2.c2
order by 1,2,3;
set enable_mergejoin=on;
set enable_hashjoin=on;

-- append path rel->reltargetlist with vector engine unsupport expr --
insert into test3 values(100);
insert into test3 values(200);
insert into test3 values(300);
select c1,table_data_skewness(row(c1), 'H') as res1,c1 from t1
union all
select c1,table_data_skewness(row(c1), 'H') as res1,c1 from test3

order by 1;
explain (costs off, verbose on) select c1,table_data_skewness(row(c1), 'H') as res1,c1 from t1
union all
select c1,table_data_skewness(row(c1), 'H') as res1,c1 from test3
order by 1;

select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2 from t1
union all
select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2 from test3
order by 1,2;
explain (costs off, verbose on) select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2 from t1
union all
select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2 from test3
order by 1,2;

select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1 from t1 where table_data_skewness(row(c2), 'H')>0
union all
select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1 from test3 where table_data_skewness(row(c1), 'H')>0
order by 1,2;
explain (costs off, verbose on) select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1 from t1 where table_data_skewness(row(c2), 'H')>0
union all
select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1 from test3 where table_data_skewness(row(c1), 'H')>0
order by 1,2;

select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1=c3 from t1 where table_data_skewness(row(c2), 'H')>0
union all
select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1=c1+1 from test3 where table_data_skewness(row(c1), 'H')>0
order by 1,2;
explain (costs off, verbose on) select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1=c3 from t1 where table_data_skewness(row(c2), 'H')>0
union all
select c1,table_data_skewness(row(c1), 'H') as res1, cast(0 as decimal(7,2)) as res2,c1=c1+1 from test3 where table_data_skewness(row(c1), 'H')>0
order by 1,2;

drop schema vec_unsupport_expression cascade;

