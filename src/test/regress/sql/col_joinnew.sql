-----------------------------------------------------------------------------------------------------
-- Verify results
-----------------------------------------------------------------------------------------------------
create schema vector_distribute_joinnew;
set current_schema = vector_distribute_joinnew;

--create row table
create table row_table_01(c1 int, c2 numeric, c3 char(10)) ;
create table row_table_02(c1 numeric, c2 bigint, c3 char(10)) ;
insert into  row_table_01 select generate_series(1,1000), generate_series(1,1000), 'row'|| generate_series(1,1000);
insert into  row_table_02 select generate_series(500,1000,10), generate_series(500,1000,10), 'row'|| generate_series(1,51);
--create column table
create table joinnew_table_01(c1 int, c2 numeric, c3 char(10)) with (orientation = column) ;
create table joinnew_table_02(c1 numeric, c2 bigint, c3 char(10)) with (orientation = column) ;
insert into joinnew_table_01 select * from row_table_01;
insert into joinnew_table_02 select * from row_table_02;

analyze joinnew_table_01;
analyze joinnew_table_02;

-- both side is distribute key
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01, joinnew_table_02 where joinnew_table_01.c1 = joinnew_table_02.c2 order by joinnew_table_01.c1;

-- One side join on distribute key
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01, joinnew_table_02 where joinnew_table_01.c1 = joinnew_table_02.c1 order by joinnew_table_01.c1;

-- Both sides join on distribute key but not same
select * from joinnew_table_01, joinnew_table_02 where joinnew_table_01.c1 = joinnew_table_02.c1 and joinnew_table_01.c2 = joinnew_table_02.c2 order by joinnew_table_01.c1;

-- Both sides join on non-distribute key
select * from joinnew_table_01, joinnew_table_02 where joinnew_table_01.c3 = joinnew_table_02.c3 order by joinnew_table_01.c1;
select * from joinnew_table_01, joinnew_table_02 where substring(joinnew_table_01.c3, 2) = substring(joinnew_table_02.c3, 2) order by joinnew_table_01.c1;

-- left/right/full Join, Both sides join on the same distribute key
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 left join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c2 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 right join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c2 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 full join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c2 order by joinnew_table_01.c1;

-- left/right/full Join, one side on distribute key
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 left join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c1 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 right join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c1 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 full join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c1 order by joinnew_table_01.c1;

-- left/right/full Join, Both sides join on distribute key but not same
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 left join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c1 and joinnew_table_01.c2 = joinnew_table_02.c2 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 right join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c1 and joinnew_table_01.c2 = joinnew_table_02.c2 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 full join joinnew_table_02 on joinnew_table_01.c1 = joinnew_table_02.c1 and joinnew_table_01.c2 = joinnew_table_02.c2 order by joinnew_table_01.c1;

-- Both sides join on non-distribute key
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 left join joinnew_table_02 on joinnew_table_01.c3 = joinnew_table_02.c3 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 right join joinnew_table_02 on joinnew_table_01.c3 = joinnew_table_02.c3 order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 full join joinnew_table_02 on joinnew_table_01.c3 = joinnew_table_02.c3 order by joinnew_table_01.c1;

-- join condition is not simple val 
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 left join joinnew_table_02 on substring(joinnew_table_01.c3, 2) = substring(joinnew_table_02.c3, 2) order by joinnew_table_01.c1;
select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 right join joinnew_table_02 on substring(joinnew_table_01.c3, 2) = substring(joinnew_table_02.c3, 2) order by joinnew_table_01.c1;
--select joinnew_table_01.*, joinnew_table_02.c3 from joinnew_table_01 full join joinnew_table_02 on substring(joinnew_table_01.c3, 2) = substring(joinnew_table_02.c3, 2) order by joinnew_table_01.c1; //need debug

-- Anti Join/Semi Join
select joinnew_table_01.* from joinnew_table_01 where joinnew_table_01.c1 not in (select joinnew_table_02.c2 from joinnew_table_02) order by joinnew_table_01.c1;
select joinnew_table_02.* from joinnew_table_02 where joinnew_table_02.c1 not in (select joinnew_table_01.c2 from joinnew_table_01) order by joinnew_table_02.c1;

drop schema vector_distribute_joinnew cascade;

create schema vector_engine_test;
create table  vector_engine_test.fvt_distribute_query_tables_01(
	w_name 	char(10),
	w_street_1  	character varying(20),
	w_zip	char(9),
	w_id 	integer, partial cluster key(w_name)) with (orientation=column,max_batchrow= 30700, compression = high)  
partition by range (w_id)
(
	partition fvt_distribute_query_tables_01_p1 values less than (6),
	partition fvt_distribute_query_tables_01_p2 values less than (8),
	partition fvt_distribute_query_tables_01_p3 values less than (maxvalue)
);
create index fvt_distribute_query_indexes_01 on vector_engine_test.fvt_distribute_query_tables_01(w_id) local;
create index fvt_distribute_query_indexes_01_2 on vector_engine_test.fvt_distribute_query_tables_01(w_name) local;
create index fvt_distribute_query_indexes_01_3 on vector_engine_test.fvt_distribute_query_tables_01(w_street_1) local;


--table_02  
--alter column type, integer->varchar or text
create table  vector_engine_test.fvt_distribute_query_tables_02(
	c_id 	varchar,
	c_street_1 	varchar(20),
	c_city 	text,
	c_zip 	varchar(9),
	c_d_id 	numeric,
	c_w_id 	text, partial cluster key(c_id,c_w_id)) with (orientation=column,max_batchrow= 30700, compression = high)
;
create index fvt_distribute_query_indexes_02 on vector_engine_test.fvt_distribute_query_tables_02(c_d_id,c_id);
create index fvt_distribute_query_indexes_02_2 on vector_engine_test.fvt_distribute_query_tables_02(c_city);

--table_03  
create table  vector_engine_test.fvt_distribute_query_tables_03(
	d_w_id 	integer,
	d_name 	character varying(10),
	d_street_2 	character varying(20),
	d_city	character varying(20),
	d_id 	integer) with (orientation=column,max_batchrow= 38700, compression = yes);
--to node(datanode1);
create index fvt_distribute_query_indexes_03 on vector_engine_test.fvt_distribute_query_tables_03(d_w_id,d_id);
create index fvt_distribute_query_indexes_03_2 on vector_engine_test.fvt_distribute_query_tables_03(d_name);
create index fvt_distribute_query_indexes_03_3 on vector_engine_test.fvt_distribute_query_tables_03(d_city);

--table_04 
--alter column type,integer->varchar
create table  vector_engine_test.fvt_distribute_query_tables_04(
	w_id 	integer,
	w_name 	varchar(20),
	w_zip	integer, partial cluster key(w_id,w_name,w_zip)) with (orientation=column)
;
create index fvt_distribute_query_indexes_04 on vector_engine_test.fvt_distribute_query_tables_04(w_id);
create index fvt_distribute_query_indexes_04_02 on vector_engine_test.fvt_distribute_query_tables_04(w_zip);

insert into vector_engine_test.fvt_distribute_query_tables_01 values ('marvelly',null,'pperymin',null);
select count(*) from vector_engine_test.fvt_distribute_query_tables_01;

--table_02  
insert into vector_engine_test.fvt_distribute_query_tables_02 values (90871,null,null,'daswwwqer',null,'11398765');
insert into vector_engine_test.fvt_distribute_query_tables_02 values (null,'cantonpgxca','ttpbnmv',null,90876,'2234689');
insert into vector_engine_test.fvt_distribute_query_tables_02 values (null,null,'asdfiuopqd','lacationd',90886,'332190');
insert into vector_engine_test.fvt_distribute_query_tables_02 values (93172,'oiuyeacya',null,'hwaopvcx',90909,null);
select count(*) from vector_engine_test.fvt_distribute_query_tables_02;

--table_03  
insert into vector_engine_test.fvt_distribute_query_tables_03 values (90123,null,'tanngyanludeshuo',null,90181);
select count(*) from vector_engine_test.fvt_distribute_query_tables_03;

--table_04 
insert into vector_engine_test.fvt_distribute_query_tables_04 values(90991,'costlvmure',null);
insert into vector_engine_test.fvt_distribute_query_tables_04 values(0,'hvrjzuvnqg',null);
select count(*) from vector_engine_test.fvt_distribute_query_tables_04;

select  table_01.w_name , table_02.c_d_id<8 t2 , table_02.c_city , table_03.d_w_id , table_04.w_name 
from vector_engine_test.fvt_distribute_query_tables_01 as table_01 inner join vector_engine_test.fvt_distribute_query_tables_02 as table_02 
on coalesce(table_01.w_id,1)= table_02.c_w_id right outer join vector_engine_test.fvt_distribute_query_tables_03 as table_03
on table_01.w_id = table_03.d_w_id right outer join vector_engine_test.fvt_distribute_query_tables_04 as table_04 
on table_04.w_id=table_03.d_w_id  and table_04.w_id<table_02.c_id  order by table_01.w_name , t2 , table_02.c_city , table_03.d_w_id , table_04.w_name  fetch NEXT  ROWS ONLY ;
drop schema vector_engine_test cascade;