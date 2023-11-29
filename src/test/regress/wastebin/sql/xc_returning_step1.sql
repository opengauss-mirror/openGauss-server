--
--FOR BLACKLIST FEATURE: INHERITS、SEQUENCE、RULE is not supported.
--

set client_min_messages=warning;
-- Lets not reuse int4_tbl & int8_tbl
-- so that this test could be run independently

create table xc_int8_tbl(q1 int8, q2 int8);
INSERT INTO xc_int8_tbl VALUES('  123   ','  456');
INSERT INTO xc_int8_tbl VALUES('123   ','4567890123456789');
INSERT INTO xc_int8_tbl VALUES('4567890123456789','123');
INSERT INTO xc_int8_tbl VALUES(+4567890123456789,'4567890123456789');
INSERT INTO xc_int8_tbl VALUES('+4567890123456789','-4567890123456789');

create table xc_int4_tbl(f1 int4);
INSERT INTO xc_int4_tbl(f1) VALUES ('   0  ');
INSERT INTO xc_int4_tbl(f1) VALUES ('123456     ');
INSERT INTO xc_int4_tbl(f1) VALUES ('    -123456');
INSERT INTO xc_int4_tbl(f1) VALUES ('2147483647');
INSERT INTO xc_int4_tbl(f1) VALUES ('-2147483647');

-- The tables have to be created on a well defined set of nodes
-- independent of the nodes available in the cluster
-- so that ctid returning tests produce predictable tests


CREATE TABLE foo (f1 serial, f2 text, f3 int default 42);


CREATE TABLE parent(a int, b int);
select create_table_nodes('child (c int) INHERITS (parent)', 'hash(a)', NULL);
select create_table_nodes('grand_child (d int) INHERITS (child)', 'hash(a)', NULL);

CREATE TABLE fp(f1 int, f2 varchar(255), f3 int);
select create_table_nodes('fp_child (fc int) INHERITS (fp)', 'hash(f1)', NULL);

CREATE TABLE bar(c1 int, c2 int);



-------------------------------------------------------------
-- insert returning the colum that was not inserted
-------------------------------------------------------------

insert into foo values(1,'One') returning f3, ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- single insert & multi-insert returning misc values
-------------------------------------------------------------

insert into bar values(8,9) 
returning c2, c1, c2, c2, c1+c2, c2-c1 as diff, 
c2-1 as minus1, ctid, least(c1,c2);

with t as 
(
  insert into bar values(1,2), (3,4),(5,6)
  returning c2, c1, c2, c2, c1+c2, c2-c1 as diff,
    c2-1 as minus1, ctid, least(c1,c2)
) select * from t order by 1,2;

truncate table bar;

-------------------------------------------------------------
-- sub-plan and init-plan in returning list
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9) returning *, ctid;

with t as
(
INSERT INTO foo SELECT f1+10, f2, f3+99 FROM foo
RETURNING *, f3-f1, f1+112 IN (SELECT q1 FROM xc_int8_tbl) AS subplan, 
EXISTS(SELECT * FROM xc_int4_tbl) AS initplan
)
select * from t order by 1,2,3;

select * from foo order by 1,2;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Make sure returning implementation works in case of inheritance
-------------------------------------------------------------

with t as
(
INSERT INTO fp VALUES (1,'test', 42), (2,'More', 11), (3,upper('more'), 7+9) returning ctid, f3, f2, f1, f3-f1
)
select * from t order by 4,3,2;

INSERT INTO fp_child VALUES(123,'child',999,-123) returning ctid, *;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- insert returning in case of an insert rule defined on table
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

create VIEW voo AS SELECT f1, f2 FROM foo;

create OR REPLACE RULE voo_i AS ON INSERT TO voo DO INSTEAD
  INSERT INTO foo VALUES(new.*, 57) RETURNING f1, f2;

INSERT INTO voo VALUES(11,'zit');
INSERT INTO voo VALUES(12,'zoo') RETURNING *, f1*2;

drop view voo;
truncate table foo;

alter sequence foo_f1_seq restart with 1;









--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
-- UPDATE Returning
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------



-------------------------------------------------------------
-- update more rows in one go and return updated values
-------------------------------------------------------------

insert into bar values(1,2), (3,4),(5,6);

with t as
(
update bar set c2=c2+1 returning c2, c1, c2, c2, c1+c2, c2-c1, c2-1, ctid
)
select * from t order by 1,2;

truncate table bar;

-------------------------------------------------------------
-- use a function in returning clause
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3 = f3+1 WHERE f1 < 2 RETURNING f3, f2, f1, f3-f1, least(f1,f3), ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- An example of a join where returning list contains columns from both tables
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3=f3*2 from xc_int4_tbl i WHERE (foo.f1 + (123456-1)) = i.f1 RETURNING foo.*, foo.f3-foo.f1,  i.f1 as "i.f1", foo.ctid, i.ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- sub-plan and init-plan in returning list
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3=f3*2 WHERE f1 < 2 RETURNING *, f3-f1, ctid, f1+112 IN (SELECT q1 FROM xc_int8_tbl) AS subplan, EXISTS(SELECT * FROM xc_int4_tbl) AS initplan;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Test * in a join case when used in returning
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

update foo set f3=f3*2 from xc_int8_tbl i  WHERE foo.f1+122 = i.q1  RETURNING *, foo.ctid;

truncate table foo;

alter sequence foo_f1_seq restart with 1;


-------------------------------------------------------------
-- Make sure returning implementation did not break update in case of inheritance
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);
ALTER table fp ADD COLUMN f4 int8 DEFAULT 99;

explain (costs off, num_verbose on)
UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

explain (costs off, num_verbose on)
UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

explain (costs off, num_verbose on)
update fp_child set fc=fc+2 returning *, f4-f1;

explain (costs off, num_verbose on)
update fp set f3 = f3 + 1 where f1<2 returning *, f3-f1;


UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

update fp_child set fc=fc+2 returning *, f4-f1;

update fp set f3 = f3 + 1 where f1<2 returning *, f3-f1;

select * from fp order by 1,2,3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Update parent with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1, 'test', 42), (2, 'More', 11), (3, upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

with t as
(
UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99 returning ctid, *
)
select * from t order by 1,2,3;

with t as
(
UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2 returning *, fp.ctid, i.ctid
)
select * from t order by 1,2,3;

with t as
(
update fp set f3=i.q1 from xc_int8_tbl i  WHERE fp.f1 = i.q1  RETURNING *, fp.f1-fp.f3
)
select * from t order by 1,2,3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- update child with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1, 'test', 42), (2, 'More', 11), (3, upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

with t as
(
UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99 returning *
)
select * from t order by 1,2,3;

with t as
(
UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2 returning *
)
select * from t order by 1,2,3;

with t as
(
update fp_child set f4 = f4 + 1 from xc_int8_tbl i  WHERE fp_child.f1 = i.q1  RETURNING *, fp_child.f1-fp_child.f3
)
select * from t order by 1,2,3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Returning in case of a rule defined on table
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

create VIEW voo AS SELECT f1, f3 FROM foo;

create OR REPLACE RULE voo_u AS ON UPDATE TO voo DO INSTEAD UPDATE foo SET f3 = new.f3 WHERE f1 = old.f1 RETURNING f3, f1;

update voo set f3 = f1 + 1;
update voo set f3 = f1 + 1 where f1 < 2 RETURNING *;

drop view voo;
truncate table foo;

alter sequence foo_f1_seq restart with 1;


-------------------------------------------------------------
-- returning in case of 3 levels of inheritance
-------------------------------------------------------------

insert into parent values(1,2),(3,4),(5,6),(7,8);

insert into child values(11,22,33),(44,55,66);

insert into grand_child values(111,222,333,444),(555,666,777,888);

update parent set b = a + 1 from xc_int8_tbl i WHERE parent.a + 455 = i.q2  RETURNING *, b-i.q2;

update child set c=c+1 from xc_int8_tbl i  WHERE child.a + (456-44) = i.q2  RETURNING *, b-a;

update grand_child set d=d+2 from xc_int8_tbl i  WHERE grand_child.a + (456-111) = i.q2  RETURNING *, b-a;

truncate table grand_child;
truncate table child;
truncate table parent;

-------------------------------------------------------------
-- Return system columns 
-------------------------------------------------------------

insert into bar values(1,2),(3,4),(5,6),(7,8),(9,0);

update bar  set c2=c2 where c1 = 1 returning c2, c1, c2-c1, ctid, cmin, xmax, cmax;
truncate table bar;









-------------------------------------------------------------
-- Returning from both the tables in case of an update using from clause
-------------------------------------------------------------
insert into bar values(1,2);

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT);
update foo set f3=f3*2 from bar i RETURNING foo.*, foo.ctid foo_ctid, i.ctid i_ctid;

truncate table bar;
truncate table foo;

alter sequence foo_f1_seq restart with 1;





---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- DELETE Returning
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------




-------------------------------------------------------------
-- delete more rows in one go and return deleted values
-------------------------------------------------------------

insert into bar values(1,2), (3,4),(5,6);

with t as
(
delete from bar returning c2, c1, c2, c2, c1+c2, c2-c1, c2-1
)
select * from t order by 1,2;

truncate table bar;

-------------------------------------------------------------
-- use a function in returning clause
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo WHERE f1 < 2 RETURNING f3, f2, f1, f3-f1, least(f1,f3);

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- An example of a join where returning list contains columns from both tables
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo USING xc_int4_tbl i WHERE (foo.f1 + (123456-1)) = i.f1 RETURNING foo.*, foo.f3-foo.f1,  i.f1 as "i.f1";

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- sub-plan and init-plan in returning list
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo WHERE f1 < 2 RETURNING *, f3-f1,  f1+112 IN (SELECT q1 FROM xc_int8_tbl) AS subplan, EXISTS(SELECT * FROM xc_int4_tbl) AS initplan;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Test * in a join case when used in returning
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

DELETE FROM foo  USING xc_int8_tbl i  WHERE foo.f1+122 = i.q1  RETURNING *;

truncate table foo;

alter sequence foo_f1_seq restart with 1;

-------------------------------------------------------------
-- Test delete returning in case of child tables and parent tables
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);
INSERT INTO fp_child VALUES(456,'child',999,-456);

UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

DELETE FROM fp_child where fc = -123 returning *, f4-f1;

DELETE FROM fp where f1 < 2 returning *, f3-f1;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Delete from parent with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

DELETE FROM fp  USING xc_int8_tbl i  WHERE fp.f1 = i.q1  RETURNING *, fp.f1-fp.f3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- Delete from child with returning in case of a join
-------------------------------------------------------------

INSERT INTO fp VALUES (1,'test',42), (2,'More', 11), (3,upper('more'), 7+9);

INSERT INTO fp_child VALUES(123,'child',999,-123);

UPDATE fp SET f4 = f4 + f3 WHERE f4 = 99;

UPDATE fp SET f3 = f3*2  FROM xc_int8_tbl i  WHERE fp.f1 = i.q2;

DELETE FROM fp_child USING xc_int8_tbl i  WHERE fp_child.f1 = i.q1  RETURNING *, fp_child.f1-fp_child.f3;

truncate table fp_child;
truncate table fp;

-------------------------------------------------------------
-- delete returning in case of a delete rule defined on table
-------------------------------------------------------------

INSERT INTO foo (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

create VIEW voo AS SELECT f1, f2 FROM foo;

create OR REPLACE RULE voo_d AS ON DELETE TO voo DO INSTEAD  DELETE FROM foo WHERE f1 = old.f1  RETURNING f1, f2;
DELETE FROM foo WHERE f1 = 1;
DELETE FROM foo WHERE f1 < 2 RETURNING *, f3-f1;

drop view voo;
truncate table foo;

alter sequence foo_f1_seq restart with 1;



-------------------------------------------------------------
-- delete returning in case of 3 levels of inheritance
-------------------------------------------------------------


insert into parent values(1,2),(3,4),(5,6),(7,8);

insert into child values(11,22,33),(44,55,66);

insert into grand_child values(111,222,333,444),(555,666,777,888);

DELETE FROM parent  USING xc_int8_tbl i  WHERE parent.a + 455 = i.q2  RETURNING *, b-a;

DELETE FROM child  USING xc_int8_tbl i  WHERE child.a + (456-44) = i.q2  RETURNING *, b-a;

DELETE FROM grand_child  USING xc_int8_tbl i  WHERE grand_child.a + (456-111) = i.q2  RETURNING *, b-a;

truncate table grand_child;
truncate table child;
truncate table parent;

-------------------------------------------------------------
-- Return system columns while deleting
-------------------------------------------------------------

insert into bar values(1,2),(3,4),(5,6),(7,8),(9,0);

with t as
(
delete from bar returning c2, c1, c2-c1
)
select * from t order by 1,2;

truncate table bar;













-------------------------------------------------------------
-- clean up
-------------------------------------------------------------


drop table bar;

drop table fp_child;
drop table fp;

drop table grand_child;
drop table child;
drop table parent;


drop table foo;


drop table xc_int8_tbl;

drop table xc_int4_tbl;

reset client_min_messages;
