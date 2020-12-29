

create schema vector_distribute_joinplan;
set current_schema = vector_distribute_joinplan;
create table row_table_01(c1 int, c2 numeric, c3 char(10)) ;
create table row_table_02(c1 bigint, c2 int, c3 char(10)) ;
insert into  row_table_01 select generate_series(1,1000), generate_series(1,1000), 'row'|| generate_series(1,1000);
insert into  row_table_02 select generate_series(500,1000,10), generate_series(500,1000,10), 'row'|| generate_series(1,51);

create table joinplan_table_01(c1 int, c2 numeric, c3 char(10)) with (orientation=column) ;
create table joinplan_table_02(c1 bigint, c2 int, c3 char(10)) with (orientation=column) ;
insert into joinplan_table_01 select * from row_table_01;
insert into joinplan_table_02 select * from row_table_02;

analyze joinplan_table_01;
analyze joinplan_table_02;

-- Join on distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c2;

-- One side join on distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c1;

-- Both sides join on distribute key but not same
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c1 and joinplan_table_01.c2 = joinplan_table_02.c2;

-- Both sides join on non-distribute key
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c3 = joinplan_table_02.c3;
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where substring(joinplan_table_01.c3, 2) = substring(joinplan_table_02.c3, 2);

set enable_nestloop=off;
-- Join on distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c2;

-- One side join on distribute key
explain (verbose on, costs off) select joinplan_table_02.*, joinplan_table_01.c3 from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c2 = joinplan_table_02.c2;

-- Both sides join on distribute key but not same
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c1 and joinplan_table_01.c2 = joinplan_table_02.c2;

-- Both sides join on non-distribute key
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c3 = joinplan_table_02.c3;
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where substring(joinplan_table_01.c3, 2) = substring(joinplan_table_02.c3, 2);

set enable_nestloop=off;
set enable_hashjoin=off;
-- Join on distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c2;

-- One side join on distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c1;

-- Both sides join on distribute key but not same
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c1 = joinplan_table_02.c1 and joinplan_table_01.c2 = joinplan_table_02.c2;

-- Both sides join on non-distribute key
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where joinplan_table_01.c3 = joinplan_table_02.c3;
explain (verbose on, costs off) select * from joinplan_table_01, joinplan_table_02 where substring(joinplan_table_01.c3, 2) = substring(joinplan_table_02.c3, 2);

-- Anti Join, Semi Join, Left Join, Right Join, Full Join, Cartesian Join
set enable_nestloop=on;
set enable_hashjoin=on;

-- left/right/full Join on distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 left join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c2;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 right join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c2;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 full join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c2;

-- left/right/full Join, one side on distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 left join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c1;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 right join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c1;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 full join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c1;

-- left/right/full Join, Both sides join on distribute key but not same
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 left join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c1 and joinplan_table_01.c2 = joinplan_table_02.c2;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 right join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c1 and joinplan_table_01.c2 = joinplan_table_02.c2;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 full join joinplan_table_02 on joinplan_table_01.c1 = joinplan_table_02.c1 and joinplan_table_01.c2 = joinplan_table_02.c2;

-- Both sides join on non-distribute key
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 left join joinplan_table_02 on joinplan_table_01.c3 = joinplan_table_02.c3;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 right join joinplan_table_02 on joinplan_table_01.c3 = joinplan_table_02.c3;
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 full join joinplan_table_02 on joinplan_table_01.c3 = joinplan_table_02.c3;

-- Both sides join on non-distribute key with function
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 left join joinplan_table_02 on substring(joinplan_table_01.c3, 2) = substring(joinplan_table_02.c3, 2);
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 right join joinplan_table_02 on substring(joinplan_table_01.c3, 2) = substring(joinplan_table_02.c3, 2);
explain (verbose on, costs off) select joinplan_table_01.*, joinplan_table_02.c3 from joinplan_table_01 full join joinplan_table_02 on substring(joinplan_table_01.c3, 2) = substring(joinplan_table_02.c3, 2);

-- Anti Join/Semi Join
explain (verbose on, costs off) select joinplan_table_01.* from joinplan_table_01 where joinplan_table_01.c1 not in (select joinplan_table_02.c2 from joinplan_table_02);
explain (verbose on, costs off) select joinplan_table_02.* from joinplan_table_02 where joinplan_table_02.c1 not in (select joinplan_table_01.c1 from joinplan_table_01);
explain (verbose on, costs off) select joinplan_table_01.* from joinplan_table_01 where joinplan_table_01.c1 not in (select joinplan_table_02.c1 from joinplan_table_02);
explain (verbose on, costs off) select joinplan_table_02.* from joinplan_table_02 where joinplan_table_02.c1 not in (select joinplan_table_01.c2 from joinplan_table_01);

drop table joinplan_table_01;
drop table joinplan_table_02;
--
----
-- test semi join
CREATE TABLE row_semi_to_inner_unique1 (id INTEGER,name VARCHAR(3000)) ;
CREATE TABLE row_semi_to_inner_unique2 (id INTEGER,address VARCHAR(3000)) ;

CREATE OR REPLACE PROCEDURE pro_semi_to_inner_unique2()
AS  
BEGIN  
	FOR I IN 1..50 LOOP  
		INSERT INTO row_semi_to_inner_unique2  VALUES (1,'in_china_1_'||i);
	END LOOP; 
END;  
/
CALL pro_semi_to_inner_unique2();

INSERT INTO row_semi_to_inner_unique2  VALUES (2,'in_china_2');
INSERT INTO row_semi_to_inner_unique2  VALUES (3,'in_china_3');
INSERT INTO row_semi_to_inner_unique2  VALUES (4,'in_china_4');
INSERT INTO row_semi_to_inner_unique2  VALUES (5,'in_china_5');
INSERT INTO row_semi_to_inner_unique2  VALUES (6,'in_china_6');
INSERT INTO row_semi_to_inner_unique1  VALUES (1,'out_jeff_1_01');
INSERT INTO row_semi_to_inner_unique1  VALUES (1,'out_jeff_1_02');
INSERT INTO row_semi_to_inner_unique1  VALUES (1,'out_jeff_1_03');


SET ENABLE_MERGEJOIN = FALSE;
SET ENABLE_HASHJOIN = FALSE;

CREATE TABLE semi_to_inner_unique1 (id INTEGER,name VARCHAR(3000)) with (orientation=column) ;
CREATE TABLE semi_to_inner_unique2 (id INTEGER,address VARCHAR(3000)) with (orientation=column) ;
insert into semi_to_inner_unique1 select * from row_semi_to_inner_unique1;
insert into semi_to_inner_unique2 select * from row_semi_to_inner_unique2;

analyze semi_to_inner_unique1;
analyze semi_to_inner_unique2;

select count(*) from semi_to_inner_unique2;
select count(*) from semi_to_inner_unique1;

EXPLAIN (VERBOSE ON, COSTS OFF) SELECT count(*) from semi_to_inner_unique1 where semi_to_inner_unique1.id IN(SELECT semi_to_inner_unique2.id FROM semi_to_inner_unique2 RIGHT JOIN  semi_to_inner_unique1 USING(id));

SELECT count(*) from semi_to_inner_unique1 where semi_to_inner_unique1.id IN(SELECT semi_to_inner_unique2.id FROM semi_to_inner_unique2 RIGHT JOIN  semi_to_inner_unique1 USING(id));

DROP TABLE semi_to_inner_unique1;
DROP TABLE semi_to_inner_unique2;

--test anti join
create table row_Multi_Jointable1 (id integer not null,id1 integer,a1 varchar(5) not null,a2 varchar(32),a3 decimal(16,2));
create table row_Multi_Jointable2 (id integer not null,id1 integer,a1 varchar(5) not null,a2 varchar(32),a3 decimal(16,2));
insert into row_Multi_Jointable1 values (1,'1','a','a',1.1);
insert into row_Multi_Jointable1 values (2,'2','b','b',1.2);
insert into row_Multi_Jointable1 values (3,'3','c','c',1.3);
insert into row_Multi_Jointable1 values (4,'4','d','d',1.4);
insert into row_Multi_Jointable1 values (5,'5','e','e',1.5);
insert into row_Multi_Jointable1 values (6,'6','f','f',1.6);
insert into row_Multi_Jointable1 values (7,'6','g','g',1.6);
insert into row_Multi_Jointable1 values (8,'6','h','h',1.6);
insert into row_Multi_Jointable2 values (1,'1','a','a',1.1);
insert into row_Multi_Jointable2 values (2,'2','b','b',1.2);
insert into row_Multi_Jointable2 values (3,'3','c','c',1.3);
insert into row_Multi_Jointable2 values (4,'4','d','d',1.4);
insert into row_Multi_Jointable2 values (5,'5','e','e',1.5);
insert into row_Multi_Jointable2 values (6,'6','f','f',1.6);

create table Multi_Jointable1 (id integer not null,id1 integer,a1 varchar(5) not null,a2 varchar(32),a3 decimal(16,2)) with (orientation=column);
create table Multi_Jointable2 (id integer not null,id1 integer,a1 varchar(5) not null,a2 varchar(32),a3 decimal(16,2)) with (orientation=column);
insert into Multi_Jointable1 select * from row_Multi_Jointable1;
insert into Multi_Jointable2 select * from row_Multi_Jointable2;

analyze Multi_Jointable1;
analyze Multi_Jointable2;

EXPLAIN (VERBOSE ON, COSTS OFF)
select joinplan_table_01.id from Multi_Jointable1 joinplan_table_01 where (joinplan_table_01.id,joinplan_table_01.id1) not in (select 2,2 from Multi_Jointable2 tt1) order by 1;
select joinplan_table_01.id from Multi_Jointable1 joinplan_table_01 where (joinplan_table_01.id,joinplan_table_01.id1) not in (select 2,2 from Multi_Jointable2 tt1) order by 1;

reset search_path;
drop schema  distribute_joinplan cascade;
