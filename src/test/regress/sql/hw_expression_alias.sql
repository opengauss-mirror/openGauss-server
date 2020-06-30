create table t1(a int, b int, c int, d int);
insert into t1 values(1, 2 ,3 ,4),(5, 6 ,7, 8);
select a + b + current_timestamp as col1  from t1 where col1 = 1;
 
select a + b as col1, b + c + d  as col2, c + col2 as col3 from t1 order by 1, 2, 3;

select a + b as col1, a + c + col1 as col2, b + c + d  as col3, c + col3 as col4 from t1 order by 1, 2 ,3 ,4;


select a + b as col1, a + c + col1 as col2 from t1 group by col1, col2 having col1 < 10;

select case when a = 1 then a else b end as col1,  c + col1 as col2, sum(c) as col3 from t1 group by col1, col2  having col2 < 10;

create table t2(a int, b varchar, c text);
insert into t2 values(1, 'varchar123', 'varchar123');
select substr(b, 8, 3) as col1,  substr(c, 8, 3) || col1 as col2, a + col2 as col3 from t2 where a + col1 = 124 and col2 = 123123 and col3 = 123124;
insert into t1 values(123, 4, 5, 6);
insert into t1 values(1, 4, 5, 6);
select substr(b, 8, 3) as col1 from t1 join t2 on( col1 = t1.a) order by 1;
drop table t1;
drop table t2;

--column
create table t1_col(a int, b int, c int, d int) with (orientation=column);
COPY t1_col(a, b, c, d) FROM stdin;
1	2	3	4
5	6	7	8
123	4	5	6
1	4	5	6
\.

select a + b + current_timestamp as col1  from t1_col where col1 = 1;
 
select a + b as col1, b + c + d  as col2, c + col2 as col3 from t1_col order by 1, 2, 3;

select a + b as col1, a + c + col1 as col2, b + c + d  as col3, c + col3 as col4 from t1_col order by 1, 2, 3, 4;


select a + b as col1, a + c + col1 as col2 from t1_col group by col1, col2 having col1 < 10 order by 1, 2;


select case when a = 1 then a else b end as col1,  c + col1 as col2, sum(c) as col3 from t1_col group by col1, col2 having col2 < 10 order by 1, 2, 3;

create table t2_col(a int, b varchar, c text)with (orientation=column);
copy t2_col FROM stdin;
1	varchar1	varchar1
2	varchar12	varchar12
3	varchar123	varchar123
\.

select substr(b, 8, 3) as col1,  substr(c, 8, 3) || col1 as col2, a + col2 as col3 from t2_col where 
a + col1 = 126 and col2 = 123123 and col3 < 123129 order by 1,2,3;

select substr(t2_col.b, 8, 3) as col1 from t1_col, t2_col where col1 = t1_col.a order by 1;

select substr(t2_col.b, 8, 3) as col1, ROW_NUMBER() OVER(PARTITION BY col1) AS QUA_ROW_NUM_1 from t1_col, t2_col where col1 = t1_col.a order by 1, 2;

select substr(t2_col.b, 8, 3) as col1, ROW_NUMBER() OVER(PARTITION BY col1) AS QUA_ROW_NUM_1 from t1_col, t2_col where col1 = t1_col.a and QUA_ROW_NUM_1 < 10 order by 1;

select '1' col1 from t1_col where col1 + 5 = 6 order by 1; 
select a + b col1, col1 || col1 from t1_col order by 1, 2;

select a as col1, b as col1 from t1_col where col1 = 1;
select a as col1, b as col1, c + col1 from t1_col;
select a as col1, c + col10 from t1_col;
drop table t1_col;
drop table t2_col;
