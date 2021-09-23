create schema hll_senarios_mpp;
set current_schema = hll_senarios_mpp;

create table t_id(id int);
insert into t_id values(generate_series(1,5000));

--------------CONTENTS--------------------
--     hyperloglog test cases
------------------------------------------
--1. hello world
--2. website traffic static senario
--3. data warehouse use case
--4. axx senario
--5. axx extended
------------------------------------------

------------------------------------------
-- 1. hello world
------------------------------------------
--- make a dummy table
create table helloworld (
        id              integer,
        set     hll
);

--- insert an empty hll
insert into helloworld(id, set) values (1, hll_empty());

--- add a hashed integer to the hll
update helloworld set set = hll_add(set, hll_hash_integer(12345)) where id = 1;

--- or add a hashed string to the hll
update helloworld set set = hll_add(set, hll_hash_text('hello world')) where id = 1;

--- get the cardinality of the hll
select hll_cardinality(set) from helloworld where id = 1;

drop table helloworld;


------------------------------------------
-- 2. website traffic static senario
------------------------------------------

-- generate data
create table traffic(weekday int ,id int);
insert into traffic select 1, id%1000 from t_id;
insert into traffic select 2, id%2000 from t_id;
insert into traffic select 3, id%3000 from t_id;
insert into traffic select 4, id%4000 from t_id;
insert into traffic select 5, id%5000 from t_id;
insert into traffic select 6, id%6000 from t_id;
insert into traffic select 7, id%7000 from t_id;

-- table to store hll statistics
create table report(weekday int, users hll);
insert into report select weekday, hll_add_agg(hll_hash_integer(id)) from traffic group by weekday;

-- 1->1000 2->2000 3->3000 4->4000 5->5000 6->5000 7->5000
select weekday, #hll_add_agg(hll_hash_integer(id)) as unique_users from traffic group by weekday order by weekday;

-- should be around 5000
select  #hll_union_agg(users) from report;

drop table traffic;
drop table report;

------------------------------------------
-- 3. data warehouse use case
------------------------------------------
-- create table
create table facts (
	date            date,
	user_id         integer
);

-- generate date
insert into facts values ('2019-02-20', generate_series(1,100));
insert into facts values ('2019-02-21', generate_series(1,200));
insert into facts values ('2019-02-22', generate_series(1,300));
insert into facts values ('2019-02-23', generate_series(1,400));
insert into facts values ('2019-02-24', generate_series(1,500));
insert into facts values ('2019-02-25', generate_series(1,600));
insert into facts values ('2019-02-26', generate_series(1,700));
insert into facts values ('2019-02-27', generate_series(1,800));

-- create the destination table
create table daily_uniques (
    date            date UNIQUE,
    users           hll
);

-- fill it with the aggregated unique statistics
INSERT INTO daily_uniques(date, users)
    SELECT date, hll_add_agg(hll_hash_integer(user_id))
    FROM facts
    GROUP BY 1;

-- ask for the cardinality of the hll for each day
SELECT date, hll_cardinality(users) FROM daily_uniques order by date;	

-- ask for one week uniques
SELECT hll_cardinality(hll_union_agg(users)) FROM daily_uniques WHERE date >= '2019-02-20'::date AND date <= '2019-02-26'::date;

-- or a sliding window of uniques over the past 6 days
SELECT date, #hll_union_agg(users) OVER seven_days
FROM daily_uniques
WINDOW seven_days AS (ORDER BY date ASC ROWS 6 PRECEDING);

-- or the number of uniques you saw yesterday that you did not see today
SELECT date, (#hll_union_agg(users) OVER two_days) - #users AS lost_uniques
FROM daily_uniques
WINDOW two_days AS (ORDER BY date ASC ROWS 1 PRECEDING);	

drop table facts;
drop table daily_uniques;

------------------------------------------
-- 4. aqb test cases
------------------------------------------
create table test_hll(id bigint, name1 text, name2 text);
create table test_name1(id bigint, name1 hll);
create table test_name1_name2(id bigint, name1_name2 hll);

insert into test_hll select id, md5(id::text), md5(id::text) from t_id;

select hll_cardinality(hll_add_agg(hll_text)) , hll_cardinality(hll_add_agg(hll_bigint)) 
 from (
       select hll_hash_text(name1) hll_text,hll_hash_bigint(id) hll_bigint 
         from test_hll 
        union all 
       select hll_hash_text(name1||name2) hll_text,hll_hash_bigint(id) hll_bigint 
         from test_hll
      ) x;
	  
select hll_cardinality(hll_union_agg(hll_add_value))
  from (
      select hll_add_agg(hll_hash_bigint(id)) hll_add_value
        from test_hll
       ) x;

select hll_cardinality(hll_union_agg(hll_add_value))
  from (
      select hll_add_agg(hll_hash_text(name1 || name2)) hll_add_value
        from test_hll
       ) x;

select hll_cardinality(hll_union_agg(hll_add_value))
  from (
      select hll_add_agg(hll_hash_text(name1 || name2)) hll_add_value
        from test_hll
	  union all
      select hll_add_agg(hll_hash_text(name1)) hll_add_value
        from test_hll	  
       ) x;


insert into test_name1 
	select id, hll_add_agg(hll_hash_text(name1))
	from test_hll
	group by id;

select hll_cardinality(hll_union_agg(name1)) from test_name1;

insert into test_name1_name2
      select id, hll_add_agg(hll_hash_text(name1 || name2))
      from test_hll
	  group by id;

select hll_cardinality(hll_union_agg(name1_name2)) from test_name1_name2;

drop table test_hll;
drop table test_name1;
drop table test_name1_name2;

------------------------------------------
--  5. aqb extended test cases
------------------------------------------
create table t_data(a int, b int, c text , d text);
insert into t_data select mod(id,2), mod(id,3), id, id from t_id;

--create the dimentinon table
create table t_a_c_hll(a int, c hll);
create table t_a_cd_hll(a int, cd hll);
create table t_b_c_hll(b int, c hll);
create table t_b_cd_hll(b int, cd hll);

--insert the agg data
insert into t_a_c_hll select a, hll_add_agg(hll_hash_text(c)) from t_data group by a;
insert into t_a_cd_hll select a, hll_add_agg(hll_hash_text(c||d))  from t_data group by a;
insert into t_b_c_hll select b, hll_add_agg(hll_hash_text(c)) from t_data group by b;
insert into t_b_cd_hll select b, hll_add_agg(hll_hash_text(c||d))  from t_data group by b;

--group a have around 2500
--group b have around 1667
select a, #c from t_a_c_hll order by a;
select a, #cd from t_a_cd_hll order by a;
select b, #c from t_b_c_hll order by b;
select b, #cd from t_b_cd_hll order by b;

--should all be around 5000
select #hll_union_agg(c) from t_a_c_hll;
select #hll_union_agg(cd) from t_a_cd_hll;
select #hll_union_agg(c) from t_b_c_hll;
select #hll_union_agg(cd) from t_b_cd_hll;

--prepare
prepare p1(int) as select a, hll_cardinality( hll_add_agg(hll_hash_text(c)) || hll_add_agg(hll_hash_text(d)) )from t_data where a = $1 group by a order by 1;
execute p1(0);
execute p1(1);
deallocate p1;

prepare p2(int) as select b, hll_cardinality( hll_add_agg(hll_hash_text(c)) || hll_add_agg(hll_hash_text(d)) )from t_data where b = $1 group by b order by 1;
execute p2(0);
execute p2(1);
execute p2(2);
deallocate p2;

--transaction
begin;
declare c cursor for  select a, hll_cardinality( hll_add_agg(hll_hash_text(c)) || hll_add_agg(hll_hash_text(d)) )from t_data group by a order by 1;
fetch next from c;
fetch next from c;
close c;
commit;

begin;
declare c cursor for  select b, hll_cardinality( hll_add_agg(hll_hash_text(c)) || hll_add_agg(hll_hash_text(d)) )from t_data group by b order by 1;
fetch next from c;
fetch next from c;
fetch next from c;
close c;
commit;

--cleaning up
drop table t_data;
drop table t_a_c_hll;
drop table t_a_cd_hll;
drop table t_b_c_hll;
drop table t_b_cd_hll;

--final cleaning
drop table t_id;
drop schema hll_senarios_mpp cascade;
reset current_schema;
