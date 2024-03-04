-- inherit
CREATE DATABASE inherit_base;
\c inherit_base;

--analyze
CREATE TABLE dep AS SELECT mod(i,10000) a,
                              mod(i,10000) b
                        FROM generate_series(1,100000) s(i);

CREATE TABLE dep_son() inherits(dep);
INSERT INTO dep_son(a,b) SELECT (10000 * random())::int a,
                              (10000 * random())::int b
                            FROM generate_series(1,100000) s(i);
analyze;
EXPLAIN ANALYZE SELECT a FROM dep WHERE b < 5000 GROUP BY a;
EXPLAIN ANALYZE SELECT a FROM ONLY dep WHERE b < 5000 GROUP BY a;
drop table dep cascade;

-- iud view
create table grandfather
( id int not null,
  grandfather1_name varchar(64),
  g_age int  DEFAULT 80,
  primary key(id)
);
insert into grandfather
(id, grandfather1_name)values
(0,'A0');

create table father
( father1_name varchar(64),
  f_age int DEFAULT 60
)inherits(grandfather);
insert into father
(id, grandfather1_name, father1_name)values
(1,'A1','B1'),
(2,'A2','B2');

CREATE VIEW g_v AS select * from grandfather;
CREATE VIEW f_v AS select * from father;
select * from g_v; 
select * from f_v;

insert into f_v
(id, grandfather1_name, father1_name)values
(3,'A3','B3');
select * from g_v; 
select * from f_v;

delete from g_v where id=3;
select * from g_v; 
select * from f_v;

update g_v set grandfather1_name='A9' where id=2;
select * from g_v; 
select * from f_v;

DROP VIEW g_v;
DROP VIEW f_v;
DROP TABLE grandfather CASCADE;

-- column
CREATE TABLE grandfather1
( id int not null,
  grandfather1_name varchar(64),
  g_age int DEFAULT 80,
  primary key(id)
) WITH (ORIENTATION = COLUMN);
insert into grandfather1
(id, grandfather1_name)values
(0,'A0');
CREATE TABLE father1
( father1_name varchar(64),
  f_age int DEFAULT 60
)inherits(grandfather1) WITH (ORIENTATION = COLUMN);-- error
DROP TABLE grandfather1 CASCADE;

-- normal table inherit temporary
create temporary table tmp1(col1 int, col2 int);
create table normal(col3 int)inherits(tmp1);-- error
drop table tmp1;

-- temporary table inherit normal
create table normal(col1 int);
create temporary table tmp1(col1 int, col2 int)inherits(normal);
create temporary table tmp2(col0 int)inherits(tmp1);
create temporary table tmp3()inherits(normal, tmp1);
\d tmp1
\d normal
alter table normal add column sex Boolean;
\d tmp1
\d normal
alter table tmp2 add constraint test_pkey primary key(col0);
\d tmp2
\d normal
alter table normal add constraint test2_pkey primary key(col1);
\d tmp1
\d tmp2
\d normal
drop table tmp3;
drop table tmp2;
drop table tmp1;
drop table normal;

-- partition
CREATE TABLE plt_father1
( id int not null,
  primary key(id)
);
CREATE TABLE plt(
 id serial primary key,
 col1 varchar(8)) inherits(plt_father1)
 partition by range(id)
 (
   partition p1 values less than(10),
   partition p2 values less than(20),
   partition p3 values less than(30),
   partition p4 values less than(maxvalue)
 );-- error
DROP TABLE plt_father1 CASCADE;

CREATE TABLE plt(
 id serial primary key,
 col1 varchar(8))
 partition by range(id)
 (
   partition p1 values less than(10),
   partition p2 values less than(20),
   partition p3 values less than(30),
   partition p4 values less than(maxvalue)
 );
CREATE TABLE plt_son
( id int not null,
  primary key(id)
) inherits(plt);-- error
DROP TABLE plt CASCADE;

-- origin
CREATE TABLE grandfather1
( id int not null,
  grandfather1_name varchar(64),
  g_age int DEFAULT 80,
  primary key(id)
);
INSERT INTO grandfather1
(id, grandfather1_name) VALUES
(0,'A0');

CREATE TABLE father1
( father1_name varchar(64),
  f_age int DEFAULT 60
)inherits(grandfather1);

INSERT INTO father1
(id, grandfather1_name, father1_name) VALUES
(1,'A1','B1'),
(2,'A2','B2');
SELECT * FROM grandfather1;
SELECT * FROM father1;
DELETE FROM grandfather1 WHERE ID=2;
SELECT * FROM father1;
UPDATE grandfather1 SET grandfather1_name='A100' WHERE ID=0;
UPDATE grandfather1 SET father1_name='B100' WHERE ID=0;-- error
UPDATE grandfather1 SET grandfather1_name='A99' WHERE ID=1;
UPDATE grandfather1 SET father1_name='B99' WHERE ID=1;-- error
UPDATE father1 SET father1_name='B99' WHERE ID=1;
SELECT * FROM grandfather1;

CREATE TABLE son1
( son1_name varchar(64),
  s_age int DEFAULT 30
)inherits(father1);

INSERT INTO son1
(id, grandfather1_name, father1_name, son1_name) VALUES
(3,'A3','B3','C3'),
(4,'A4','B4','C4');
SELECT * FROM grandfather1;
SELECT * FROM son1;
DELETE FROM grandfather1 WHERE ID=4;
SELECT * FROM son1;
UPDATE grandfather1 SET grandfather1_name='A300' WHERE ID=3;
UPDATE grandfather1 SET father1_name='B300' WHERE ID=3;-- error
UPDATE grandfather1 SET son1_name='C300' WHERE ID=3;-- error
SELECT * FROM grandfather1;

ALTER TABLE grandfather1 RENAME TO grandfather0;
ALTER TABLE grandfather0 DROP COLUMN s_age CASCADE;-- error
ALTER TABLE grandfather0 rename COLUMN f_age TO father_age;-- error
ALTER TABLE grandfather0 rename COLUMN g_age TO grandfather_age;
SELECT * FROM son1;
SELECT * FROM grandfather0;
ALTER TABLE ONLY grandfather0 rename COLUMN grandfather_age TO grand_age;-- error
SELECT * FROM son1;
SELECT * FROM grandfather0;
SELECT * FROM only grandfather0;
DROP TABLE grandfather0 cascade;

CREATE TABLE father2
( id int not null,
  father2_name varchar(64),
  f2_age int DEFAULT 60
);
CREATE TABLE father3
( id int not null,
  father3_name varchar(64),
  f3_age int DEFAULT 60
);
CREATE TABLE son2
( son2_name varchar(64),
  s_age int DEFAULT 30
)inherits(father2,father3);

INSERT INTO son2
(id, father2_name, father3_name, son2_name) VALUES
(3,'A3','B3','C3'),
(4,'A4','B4','C4');

SELECT * FROM son2;
SELECT * FROM father2;
SELECT * FROM father3;
ALTER TABLE father2 DROP COLUMN id CASCADE;
SELECT * FROM son2;
SELECT * FROM father2;
SELECT * FROM father3;
DELETE FROM father3 WHERE son2_name='C4';-- error
DELETE FROM father3 WHERE id=3;
SELECT * FROM father2;

DROP TABLE IF EXISTS son2 cascade;
DROP TABLE IF EXISTS father2 cascade;
DROP TABLE IF EXISTS father3 cascade;

SET enable_indexonlyscan = off;
CREATE TABLE events (event_id int primary key);
CREATE TABLE other_events (event_id int primary key);
CREATE TABLE events_child1 () inherits (events);
INSERT INTO events_child1 (event_id) VALUES (5);
INSERT INTO events_child1 (event_id) VALUES (1);
INSERT INTO events (event_id) VALUES (2);
INSERT INTO other_events(event_id) VALUES (3);

-- order by
SELECT event_id
 FROM (SELECT event_id FROM events
       union all
       SELECT event_id FROM other_events) ss
 order by event_id;

DROP TABLE events_child1, events, other_events;
RESET enable_indexonlyscan;

-- inherit multi tables
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father_z82rgvsefn PRIMARY KEY (id)
    );
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2_z82rgvsefn PRIMARY KEY (id)
    );
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3_z82rgvsefn PRIMARY KEY (id)
    );
CREATE TABLE kid_2022() inherits(father,father2,father3);
CREATE TABLE kid_2021(like father2 including all) inherits(father,father2,father3);
\d kid_2022
\d kid_2021
DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE fa_wai CASCADE;

-- like father and including all
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father PRIMARY KEY (id)
    );
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2 PRIMARY KEY (id)
    );
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3 PRIMARY KEY (id)
    );
CREATE TABLE kid_2020() inherits(father,father2,father3);
CREATE TABLE kid_2021(like father) inherits(father);
CREATE TABLE kid_2022(like father including all) inherits(father);
CREATE TABLE kid_2023(like father) inherits(father);
\d kid_2021
\d kid_2022

INSERT INTO fa_wai VALUES(1,'a'),(2,'b');
INSERT INTO kid_wai VALUES(3,'c'),(4,'d');
INSERT INTO father VALUES(1,20,1),(2,30,2);
INSERT INTO kid_2021 VALUES(501,21,1),(502,31,2);
INSERT INTO kid_2021 VALUES(504,20,3);
SELECT count(*) FROM father;
INSERT INTO kid_2022 VALUES(505,22,1),(503,52,2);
SELECT count(*) FROM father;
INSERT INTO kid_2023 VALUES(506,23,1),(507,23,2);
SELECT * FROM father WHERE id>10;
SELECT id, md_attr FROM father* WHERE id>500;
update father SET salary =90;
SELECT * FROM kid_2021;

ALTER TABLE kid_2022 DROP CONSTRAINT "father_salary_check";-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check TO new_constraint_check;-- error
CREATE TABLESPACE example1 RELATIVE LOCATION 'tablespace22/tablespace_22';
ALTER INDEX pk_father SET TABLESPACE example1; --child index won't change
ALTER TABLE kid_2020 ALTER COLUMN md_attr TYPE int;-- error
ALTER TABLE kid_2020 RENAME COlUMN num TO new1;-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT kid_2022_pkey TO new_constraint_namepk;
ALTER TABLE father RENAME COlUMN md_attr TO new;
\d kid_2022
ALTER TABLE kid_2020 ALTER COLUMN num SET DEFAULT 1;
\d father
\d kid_2020
ALTER TABLE kid_2020 ALTER COLUMN new SET not NULL;
\d kid_2020
ALTER TABLE kid_2020 ALTER id DROP not null;
\d father
\d kid_2020
ALTER TABLE father DROP COLUMN IF EXISTS num;-- father without num, child have num
\d father
\d kid_2020
ALTER TABLE kid_2023 no inherit father;
\d father
\d kid_2023
ALTER INDEX IF EXISTS pk_father rename to new_index_name; 
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check to new_salary_check
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE father ALTER COLUMN id TYPE int;
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE kid_2022 DROP CONSTRAINT "new_constraint_namepk";

DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE kid_2023 CASCADE;
DROP TABLE fa_wai CASCADE;
DROP TABLESPACE example1;

-- not like father not including all
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father PRIMARY KEY (id)
    );
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2 PRIMARY KEY (id)
    );
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3 PRIMARY KEY (id)
    );
CREATE TABLE kid_2020() inherits(father,father2,father3);
CREATE TABLE kid_2021() inherits(father);
CREATE TABLE kid_2022() inherits(father);
CREATE TABLE kid_2023() inherits(father);
\d kid_2021
\d kid_2022

INSERT INTO fa_wai VALUES(1,'a'),(2,'b');
INSERT INTO kid_wai VALUES(3,'c'),(4,'d');
INSERT INTO father VALUES(1,20,1),(2,30,2);
INSERT INTO kid_2021 VALUES(501,21,1),(502,31,2);
INSERT INTO kid_2021 VALUES(504,20,3);
SELECT count(*) FROM father;
INSERT INTO kid_2022 VALUES(505,22,1),(503,52,2);
SELECT count(*) FROM father;
INSERT INTO kid_2023 VALUES(506,23,1),(507,23,2);
SELECT * FROM father WHERE id>10;
SELECT id, md_attr FROM father* WHERE id>500;
update father SET salary =90;
SELECT * FROM kid_2021;

ALTER TABLE kid_2022 DROP CONSTRAINT "father_salary_check";-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check TO new_constraint_check;-- error
CREATE TABLESPACE example1 RELATIVE LOCATION 'tablespace22/tablespace_22';
ALTER INDEX pk_father SET TABLESPACE example1; --child index won't change
ALTER TABLE kid_2020 ALTER COLUMN md_attr TYPE int;-- error
ALTER TABLE kid_2020 RENAME COlUMN num TO new1;-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT kid_2022_pkey TO new_constraint_namepk;-- error
ALTER TABLE father RENAME COlUMN md_attr TO new;
\d kid_2022
ALTER TABLE kid_2020 ALTER COLUMN num SET DEFAULT 1;
\d father
\d kid_2020
ALTER TABLE kid_2020 ALTER COLUMN new SET not NULL;
\d kid_2020
ALTER TABLE kid_2020 ALTER id DROP not null;
\d father
\d kid_2020
ALTER TABLE father DROP COLUMN IF EXISTS num;-- father without num, child without num
\d father
\d kid_2020
ALTER TABLE kid_2023 no inherit father;
\d father
\d kid_2023
ALTER INDEX IF EXISTS pk_father rename to new_index_name; 
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check to new_salary_check
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE father ALTER COLUMN id TYPE int;
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE kid_2022 DROP CONSTRAINT "new_constraint_namepk";-- error

DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE kid_2023 CASCADE;
DROP TABLE fa_wai CASCADE;
DROP TABLESPACE example1;

\c regression
DROP DATABASE inherit_base;

-- ustore
CREATE DATABASE inherit_ustore;
\c inherit_ustore;
CREATE TABLE grandfather1
( id int not null,
  grandfather1_name varchar(64),
  g_age int DEFAULT 80,
  primary key(id)
) WITH (storage_type=ustore);
INSERT INTO grandfather1
(id, grandfather1_name) VALUES
(0,'A0');

CREATE TABLE father1
( father1_name varchar(64),
  f_age int DEFAULT 60
)inherits(grandfather1) WITH (storage_type=ustore);

INSERT INTO father1
(id, grandfather1_name, father1_name) VALUES
(1,'A1','B1'),
(2,'A2','B2');
SELECT * FROM grandfather1;
SELECT * FROM father1;
DELETE FROM grandfather1 WHERE ID=2;
SELECT * FROM father1;
UPDATE grandfather1 SET grandfather1_name='A100' WHERE ID=0;
UPDATE grandfather1 SET father1_name='B100' WHERE ID=0;-- error
UPDATE grandfather1 SET grandfather1_name='A99' WHERE ID=1;
UPDATE grandfather1 SET father1_name='B99' WHERE ID=1;-- error
UPDATE father1 SET father1_name='B99' WHERE ID=1;
SELECT * FROM grandfather1;

CREATE TABLE son1
( son1_name varchar(64),
  s_age int DEFAULT 30
)inherits(father1) WITH (storage_type=ustore);

INSERT INTO son1
(id, grandfather1_name, father1_name, son1_name) VALUES
(3,'A3','B3','C3'),
(4,'A4','B4','C4');
SELECT * FROM grandfather1;
SELECT * FROM son1;
DELETE FROM grandfather1 WHERE ID=4;
SELECT * FROM son1;
UPDATE grandfather1 SET grandfather1_name='A300' WHERE ID=3;
UPDATE grandfather1 SET father1_name='B300' WHERE ID=3;-- error
UPDATE grandfather1 SET son1_name='C300' WHERE ID=3;-- error
SELECT * FROM grandfather1;

ALTER TABLE grandfather1 RENAME TO grandfather0;
ALTER TABLE grandfather0 DROP COLUMN s_age CASCADE;-- error
ALTER TABLE grandfather0 rename COLUMN f_age TO father_age;-- error
ALTER TABLE grandfather0 rename COLUMN g_age TO grandfather_age;
SELECT * FROM son1;
SELECT * FROM grandfather0;
ALTER TABLE ONLY grandfather0 rename COLUMN grandfather_age TO grand_age;-- error
SELECT * FROM son1;
SELECT * FROM grandfather0;
SELECT * FROM only grandfather0;
DROP TABLE grandfather0 cascade;

CREATE TABLE father2
( id int not null,
  father2_name varchar(64),
  f2_age int DEFAULT 60
) WITH (storage_type=ustore);
CREATE TABLE father3
( id int not null,
  father3_name varchar(64),
  f3_age int DEFAULT 60
) WITH (storage_type=ustore);
CREATE TABLE son2
( son2_name varchar(64),
  s_age int DEFAULT 30
)inherits(father2,father3) WITH (storage_type=ustore);

INSERT INTO son2
(id, father2_name, father3_name, son2_name) VALUES
(3,'A3','B3','C3'),
(4,'A4','B4','C4');

SELECT * FROM son2;
SELECT * FROM father2;
SELECT * FROM father3;
ALTER TABLE father2 DROP COLUMN id CASCADE;
SELECT * FROM son2;
SELECT * FROM father2;
SELECT * FROM father3;
DELETE FROM father3 WHERE son2_name='C4';-- error
DELETE FROM father3 WHERE id=3;
SELECT * FROM father2;

DROP TABLE IF EXISTS son2 cascade;
DROP TABLE IF EXISTS father2 cascade;
DROP TABLE IF EXISTS father3 cascade;

SET enable_indexonlyscan = off;
CREATE TABLE events (event_id int primary key);
CREATE TABLE other_events (event_id int primary key);
CREATE TABLE events_child1 () inherits (events);
INSERT INTO events_child1 (event_id) VALUES (5);
INSERT INTO events_child1 (event_id) VALUES (1);
INSERT INTO events (event_id) VALUES (2);
INSERT INTO other_events(event_id) VALUES (3);
-- order by
SELECT event_id
 FROM (SELECT event_id FROM events
       union all
       SELECT event_id FROM other_events) ss
 order by event_id;

DROP TABLE events_child1, events, other_events;
RESET enable_indexonlyscan;

-- inherit multi tables
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
) WITH (storage_type=ustore);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai) WITH (storage_type=ustore);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father_z82rgvsefn PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2_z82rgvsefn PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3_z82rgvsefn PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE kid_2022() inherits(father,father2,father3) WITH (storage_type=ustore);
CREATE TABLE kid_2021(like father2 including all) inherits(father,father2,father3) WITH (storage_type=ustore);-- error
\d kid_2022
\d kid_2021
DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE fa_wai CASCADE;

-- ustore like father and including all
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
) WITH (storage_type=ustore);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai) WITH (storage_type=ustore);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2 PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3 PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE kid_2020() inherits(father,father2,father3) WITH (storage_type=ustore);
CREATE TABLE kid_2021(like father) inherits(father) WITH (storage_type=ustore);
CREATE TABLE kid_2022(like father including all) inherits(father) WITH (storage_type=ustore);-- error
CREATE TABLE kid_2023(like father) inherits(father) WITH (storage_type=ustore);
\d kid_2021
\d kid_2022

INSERT INTO fa_wai VALUES(1,'a'),(2,'b');
INSERT INTO kid_wai VALUES(3,'c'),(4,'d');
INSERT INTO father VALUES(1,20,1),(2,30,2);
INSERT INTO kid_2021 VALUES(501,21,1),(502,31,2);
INSERT INTO kid_2021 VALUES(504,20,3);
SELECT count(*) FROM father;
INSERT INTO kid_2022 VALUES(505,22,1),(503,52,2);-- error
SELECT count(*) FROM father;
INSERT INTO kid_2023 VALUES(506,23,1),(507,23,2);
SELECT * FROM father WHERE id>10;
SELECT id, md_attr FROM father* WHERE id>500;
update father SET salary =90;
SELECT * FROM kid_2021;

ALTER TABLE kid_2022 DROP CONSTRAINT "father_salary_check";-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check TO new_constraint_check;-- error
CREATE TABLESPACE example1 RELATIVE LOCATION 'tablespace22/tablespace_22';
ALTER INDEX pk_father SET TABLESPACE example1; --child index won't change
ALTER TABLE kid_2020 ALTER COLUMN md_attr TYPE int;-- error
ALTER TABLE kid_2020 RENAME COlUMN num TO new1;-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT kid_2022_pkey TO new_constraint_namepk;
ALTER TABLE father RENAME COlUMN md_attr TO new;
\d kid_2022
ALTER TABLE kid_2020 ALTER COLUMN num SET DEFAULT 1;
\d father
\d kid_2020
ALTER TABLE kid_2020 ALTER COLUMN new SET not NULL;
\d kid_2020
ALTER TABLE kid_2020 ALTER id DROP not null;
\d father
\d kid_2020
ALTER TABLE father DROP COLUMN IF EXISTS num;-- father without num, child have num
\d father
\d kid_2020
ALTER TABLE kid_2023 no inherit father;
\d father
\d kid_2023
ALTER INDEX IF EXISTS pk_father rename to new_index_name; 
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check to new_salary_check
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE father ALTER COLUMN id TYPE int;
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE kid_2022 DROP CONSTRAINT "new_constraint_namepk";-- error

DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE kid_2023 CASCADE;
DROP TABLE fa_wai CASCADE;
DROP TABLESPACE example1;

-- ustore not like father not including all
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
) WITH (storage_type=ustore);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai) WITH (storage_type=ustore);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2 PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3 PRIMARY KEY (id)
    ) WITH (storage_type=ustore);
CREATE TABLE kid_2020() inherits(father,father2,father3) WITH (storage_type=ustore);
CREATE TABLE kid_2021() inherits(father) WITH (storage_type=ustore);
CREATE TABLE kid_2022() inherits(father) WITH (storage_type=ustore);
CREATE TABLE kid_2023() inherits(father) WITH (storage_type=ustore);
\d kid_2021
\d kid_2022

INSERT INTO fa_wai VALUES(1,'a'),(2,'b');
INSERT INTO kid_wai VALUES(3,'c'),(4,'d');
INSERT INTO father VALUES(1,20,1),(2,30,2);
INSERT INTO kid_2021 VALUES(501,21,1),(502,31,2);
INSERT INTO kid_2021 VALUES(504,20,3);
SELECT count(*) FROM father;
INSERT INTO kid_2022 VALUES(505,22,1),(503,52,2);
SELECT count(*) FROM father;
INSERT INTO kid_2023 VALUES(506,23,1),(507,23,2);
SELECT * FROM father WHERE id>10;
SELECT id, md_attr FROM father* WHERE id>500;
update father SET salary =90;
SELECT * FROM kid_2021;

ALTER TABLE kid_2022 DROP CONSTRAINT "father_salary_check";-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check TO new_constraint_check;-- error
CREATE TABLESPACE example1 RELATIVE LOCATION 'tablespace22/tablespace_22';
ALTER INDEX pk_father SET TABLESPACE example1; --child index won't change
ALTER TABLE kid_2020 ALTER COLUMN md_attr TYPE int;-- error
ALTER TABLE kid_2020 RENAME COlUMN num TO new1;-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT kid_2022_pkey TO new_constraint_namepk;-- error
ALTER TABLE father RENAME COlUMN md_attr TO new;
\d kid_2022
ALTER TABLE kid_2020 ALTER COLUMN num SET DEFAULT 1;
\d father
\d kid_2020
ALTER TABLE kid_2020 ALTER COLUMN new SET not NULL;
\d kid_2020
ALTER TABLE kid_2020 ALTER id DROP not null;
\d father
\d kid_2020
ALTER TABLE father DROP COLUMN IF EXISTS num;-- father without num, child without num
\d father
\d kid_2020
ALTER TABLE kid_2023 no inherit father;
\d father
\d kid_2023
ALTER INDEX IF EXISTS pk_father rename to new_index_name; 
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check to new_salary_check
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE father ALTER COLUMN id TYPE int;
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE kid_2022 DROP CONSTRAINT "new_constraint_namepk";-- error

DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE kid_2023 CASCADE;
DROP TABLE fa_wai CASCADE;
DROP TABLESPACE example1;

\c regression
DROP DATABASE inherit_ustore;

-- segment
CREATE DATABASE inherit_segment;
\c inherit_segment;
CREATE TABLE grandfather1
( id int not null,
  grandfather1_name varchar(64),
  g_age int DEFAULT 80,
  primary key(id)
) WITH (segment=on);
INSERT INTO grandfather1
(id, grandfather1_name) VALUES
(0,'A0');

CREATE TABLE father1
( father1_name varchar(64),
  f_age int DEFAULT 60
)inherits(grandfather1) WITH (segment=on);

INSERT INTO father1
(id, grandfather1_name, father1_name) VALUES
(1,'A1','B1'),
(2,'A2','B2');
SELECT * FROM grandfather1;
SELECT * FROM father1;
DELETE FROM grandfather1 WHERE ID=2;
SELECT * FROM father1;
UPDATE grandfather1 SET grandfather1_name='A100' WHERE ID=0;
UPDATE grandfather1 SET father1_name='B100' WHERE ID=0;-- error
UPDATE grandfather1 SET grandfather1_name='A99' WHERE ID=1;
UPDATE grandfather1 SET father1_name='B99' WHERE ID=1;-- error
UPDATE father1 SET father1_name='B99' WHERE ID=1;
SELECT * FROM grandfather1;

CREATE TABLE son1
( son1_name varchar(64),
  s_age int DEFAULT 30
)inherits(father1) WITH (segment=on);

INSERT INTO son1
(id, grandfather1_name, father1_name, son1_name) VALUES
(3,'A3','B3','C3'),
(4,'A4','B4','C4');
SELECT * FROM grandfather1;
SELECT * FROM son1;
DELETE FROM grandfather1 WHERE ID=4;
SELECT * FROM son1;
UPDATE grandfather1 SET grandfather1_name='A300' WHERE ID=3;
UPDATE grandfather1 SET father1_name='B300' WHERE ID=3;-- error
UPDATE grandfather1 SET son1_name='C300' WHERE ID=3;-- error
SELECT * FROM grandfather1;

ALTER TABLE grandfather1 RENAME TO grandfather0;
ALTER TABLE grandfather0 DROP COLUMN s_age CASCADE;-- error
ALTER TABLE grandfather0 rename COLUMN f_age TO father_age;-- error
ALTER TABLE grandfather0 rename COLUMN g_age TO grandfather_age;
SELECT * FROM son1;
SELECT * FROM grandfather0;
ALTER TABLE ONLY grandfather0 rename COLUMN grandfather_age TO grand_age;-- error
SELECT * FROM son1;
SELECT * FROM grandfather0;
SELECT * FROM only grandfather0;
DROP TABLE grandfather0 cascade;

CREATE TABLE father2
( id int not null,
  father2_name varchar(64),
  f2_age int DEFAULT 60
) WITH (segment=on);
CREATE TABLE father3
( id int not null,
  father3_name varchar(64),
  f3_age int DEFAULT 60
) WITH (segment=on);
CREATE TABLE son2
( son2_name varchar(64),
  s_age int DEFAULT 30
)inherits(father2,father3) WITH (segment=on);

INSERT INTO son2
(id, father2_name, father3_name, son2_name) VALUES
(3,'A3','B3','C3'),
(4,'A4','B4','C4');

SELECT * FROM son2;
SELECT * FROM father2;
SELECT * FROM father3;
ALTER TABLE father2 DROP COLUMN id CASCADE;
SELECT * FROM son2;
SELECT * FROM father2;
SELECT * FROM father3;
DELETE FROM father3 WHERE son2_name='C4';-- error
DELETE FROM father3 WHERE id=3;
SELECT * FROM father2;

DROP TABLE IF EXISTS son2 cascade;
DROP TABLE IF EXISTS father2 cascade;
DROP TABLE IF EXISTS father3 cascade;

SET enable_indexonlyscan = off;
CREATE TABLE events (event_id int primary key) WITH (segment=on);
CREATE TABLE other_events (event_id int primary key) WITH (segment=on);
CREATE TABLE events_child1 () inherits (events) WITH (segment=on);
INSERT INTO events_child1 (event_id) VALUES (5);
INSERT INTO events_child1 (event_id) VALUES (1);
INSERT INTO events (event_id) VALUES (2);
INSERT INTO other_events(event_id) VALUES (3);
-- order by
SELECT event_id
 FROM (SELECT event_id FROM events
       union all
       SELECT event_id FROM other_events) ss
 order by event_id;

DROP TABLE events_child1, events, other_events;
RESET enable_indexonlyscan;
-- inherit multi tables
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
) WITH (segment=on);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai) WITH (segment=on);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father_z82rgvsefn PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2_z82rgvsefn PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3_z82rgvsefn PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE kid_2022() inherits(father,father2,father3) WITH (segment=on);
CREATE TABLE kid_2021(like father2 including all) inherits(father,father2,father3) WITH (segment=on);-- error
\d kid_2022
\d kid_2021
DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE fa_wai CASCADE;

-- segment like father and including all
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
) WITH (segment=on);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai) WITH (segment=on);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2 PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3 PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE kid_2020() inherits(father,father2,father3) WITH (segment=on);
CREATE TABLE kid_2021(like father) inherits(father) WITH (segment=on);
CREATE TABLE kid_2022(like father including all) inherits(father) WITH (segment=on);-- error
CREATE TABLE kid_2023(like father) inherits(father) WITH (segment=on);
\d kid_2021
\d kid_2022

INSERT INTO fa_wai VALUES(1,'a'),(2,'b');
INSERT INTO kid_wai VALUES(3,'c'),(4,'d');
INSERT INTO father VALUES(1,20,1),(2,30,2);
INSERT INTO kid_2021 VALUES(501,21,1),(502,31,2);
INSERT INTO kid_2021 VALUES(504,20,3);
SELECT count(*) FROM father;
INSERT INTO kid_2022 VALUES(505,22,1),(503,52,2);-- error
SELECT count(*) FROM father;
INSERT INTO kid_2023 VALUES(506,23,1),(507,23,2);
SELECT * FROM father WHERE id>10;
SELECT id, md_attr FROM father* WHERE id>500;
update father SET salary =90;
SELECT * FROM kid_2021;

ALTER TABLE kid_2022 DROP CONSTRAINT "father_salary_check";-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check TO new_constraint_check;-- error
CREATE TABLESPACE example1 RELATIVE LOCATION 'tablespace22/tablespace_22';
ALTER INDEX pk_father SET TABLESPACE example1; --child index won't change
ALTER TABLE kid_2020 ALTER COLUMN md_attr TYPE int;-- error
ALTER TABLE kid_2020 RENAME COlUMN num TO new1;-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT kid_2022_pkey TO new_constraint_namepk;
ALTER TABLE father RENAME COlUMN md_attr TO new;
\d kid_2022
ALTER TABLE kid_2020 ALTER COLUMN num SET DEFAULT 1;
\d father
\d kid_2020
ALTER TABLE kid_2020 ALTER COLUMN new SET not NULL;
\d kid_2020
ALTER TABLE kid_2020 ALTER id DROP not null;
\d father
\d kid_2020
ALTER TABLE father DROP COLUMN IF EXISTS num;-- father without num, child have num
\d father
\d kid_2020
ALTER TABLE kid_2023 no inherit father;
\d father
\d kid_2023
ALTER INDEX IF EXISTS pk_father rename to new_index_name; 
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check to new_salary_check
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE father ALTER COLUMN id TYPE int;
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE kid_2022 DROP CONSTRAINT "new_constraint_namepk";-- error

DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE kid_2023 CASCADE;
DROP TABLE fa_wai CASCADE;
DROP TABLESPACE example1;

-- segment not like father not including all
CREATE TABLE fa_wai(
   ID  INT    PRIMARY KEY  NOT NULL,
   NAME TEXT    NOT NULL
) WITH (segment=on);
CREATE TABLE kid_wai(like fa_wai) inherits(fa_wai) WITH (segment=on);
CREATE TABLE father(
        id int NOT NULL,
        md_attr CHARACTER VARYING(32) UNIQUE,
        wai_id int references fa_wai(ID),
        num int DEFAULT 2,
        SALARY         REAL    CHECK(SALARY > 0),
        CONSTRAINT pk_father PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE father2(
        id int UNIQUE,
	md_attr CHARACTER VARYING(32) not null,
        CONSTRAINT pk_father2 PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE father3(
        id int CHECK(id > 3),
        md_attr CHARACTER VARYING(32) UNIQUE,
        CONSTRAINT pk_father3 PRIMARY KEY (id)
    ) WITH (segment=on);
CREATE TABLE kid_2020() inherits(father,father2,father3) WITH (segment=on);
CREATE TABLE kid_2021() inherits(father) WITH (segment=on);
CREATE TABLE kid_2022() inherits(father) WITH (segment=on);
CREATE TABLE kid_2023() inherits(father) WITH (segment=on);
\d kid_2021
\d kid_2022

INSERT INTO fa_wai VALUES(1,'a'),(2,'b');
INSERT INTO kid_wai VALUES(3,'c'),(4,'d');
INSERT INTO father VALUES(1,20,1),(2,30,2);
INSERT INTO kid_2021 VALUES(501,21,1),(502,31,2);
INSERT INTO kid_2021 VALUES(504,20,3);
SELECT count(*) FROM father;
INSERT INTO kid_2022 VALUES(505,22,1),(503,52,2);
SELECT count(*) FROM father;
INSERT INTO kid_2023 VALUES(506,23,1),(507,23,2);
SELECT * FROM father WHERE id>10;
SELECT id, md_attr FROM father* WHERE id>500;
update father SET salary =90;
SELECT * FROM kid_2021;

ALTER TABLE kid_2022 DROP CONSTRAINT "father_salary_check";-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check TO new_constraint_check;-- error
CREATE TABLESPACE example1 RELATIVE LOCATION 'tablespace22/tablespace_22';
ALTER INDEX pk_father SET TABLESPACE example1; --child index won't change
ALTER TABLE kid_2020 ALTER COLUMN md_attr TYPE int;-- error
ALTER TABLE kid_2020 RENAME COlUMN num TO new1;-- error
ALTER TABLE kid_2022 RENAME CONSTRAINT kid_2022_pkey TO new_constraint_namepk;-- error
ALTER TABLE father RENAME COlUMN md_attr TO new;
\d kid_2022
ALTER TABLE kid_2020 ALTER COLUMN num SET DEFAULT 1;
\d father
\d kid_2020
ALTER TABLE kid_2020 ALTER COLUMN new SET not NULL;
\d kid_2020
ALTER TABLE kid_2020 ALTER id DROP not null;
\d father
\d kid_2020
ALTER TABLE father DROP COLUMN IF EXISTS num;-- father without num, child without num
\d father
\d kid_2020
ALTER TABLE kid_2023 no inherit father;
\d father
\d kid_2023
ALTER INDEX IF EXISTS pk_father rename to new_index_name; 
ALTER TABLE kid_2022 RENAME CONSTRAINT father_salary_check to new_salary_check
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE father ALTER COLUMN id TYPE int;
\d father
\d kid_2021
\d kid_2022
\d kid_2023
ALTER TABLE kid_2022 DROP CONSTRAINT "new_constraint_namepk";-- error

DROP TABLE father CASCADE;
DROP TABLE father2 CASCADE;
DROP TABLE father3 CASCADE;
DROP TABLE kid_2023 CASCADE;
DROP TABLE fa_wai CASCADE;
DROP TABLESPACE example1;

\c regression
DROP DATABASE inherit_segment;

-- multi_update
CREATE DATABASE inherit_multi_update DBCOMPATIBILITY = 'B';
\c inherit_multi_update;

-- five relation
DROP TABLE IF EXISTS t_t_mutil_t1;
DROP TABLE IF EXISTS t_t_mutil_t2;
DROP TABLE IF EXISTS t_t_mutil_t3;
DROP TABLE IF EXISTS t_t_mutil_t4;
DROP TABLE IF EXISTS t_t_mutil_t5;
CREATE TABLE t_t_mutil_t1(col1 int,col2 int);
CREATE TABLE t_t_mutil_t2(col1 int,col2 int);
CREATE TABLE t_t_mutil_t3 () INHERITS(t_t_mutil_t1);
CREATE TABLE t_t_mutil_t4 () INHERITS(t_t_mutil_t3);
CREATE TABLE t_t_mutil_t5(col1 int,col2 int); 
INSERT INTO t_t_mutil_t1 VALUES(1,1),(1,1);
INSERT INTO t_t_mutil_t2 VALUES(1,1),(1,2);
INSERT INTO t_t_mutil_t3 VALUES(1,1),(1,3);
INSERT INTO t_t_mutil_t4 VALUES(1,1),(1,4);
INSERT INTO t_t_mutil_t5 VALUES(1,1),(1,5);

-- single inherits multi update
update t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t5 c SET b.col2=5,a.col2=4,c.col2=6 WHERE a.col1=b.col1 and b.col1=c.col1;-- error
update t_t_mutil_t3 a,t_t_mutil_t2 b,t_t_mutil_t5 c SET b.col2=3,a.col2=2,c.col2=5 WHERE a.col1=b.col1 and b.col1=c.col1;-- error
update t_t_mutil_t4 a,t_t_mutil_t2 b,t_t_mutil_t5 c SET b.col2=2,a.col2=1,c.col2=4 WHERE a.col1=b.col1 and b.col1=c.col1;
SELECT * FROM t_t_mutil_t2;
SELECT * FROM t_t_mutil_t4;
SELECT * FROM t_t_mutil_t5;
update t_t_mutil_t4 a,t_t_mutil_t2 b,t_t_mutil_t3 c SET b.col2=2,a.col2=1,c.col2=3 WHERE a.col1=b.col1 and b.col1=c.col1;-- error
update t_t_mutil_t3 a,t_t_mutil_t2 b,t_t_mutil_t4 c SET b.col2=1,a.col2=11,c.col2=2 WHERE a.col1=b.col1 and b.col1=c.col1;-- error

update t_t_mutil_t1 a,t_t_mutil_t2 b SET a.col2=7,b.col2=8 WHERE a.col1=b.col1;-- error
update t_t_mutil_t3 a,t_t_mutil_t2 b SET a.col2=6,b.col2=5 WHERE a.col1=b.col1;-- error
update t_t_mutil_t4 a,t_t_mutil_t2 b SET a.col2=5,b.col2=4 WHERE a.col1=b.col1;
SELECT * FROM t_t_mutil_t2;
SELECT * FROM t_t_mutil_t4;
update t_t_mutil_t4 a,t_t_mutil_t3 b SET a.col2=3,b.col2=2 WHERE a.col1=b.col1;-- error
update t_t_mutil_t3 a,t_t_mutil_t4 b SET a.col2=2,b.col2=1 WHERE a.col1=b.col1;-- error;

\c regression
DROP DATABASE inherit_multi_update;
CREATE DATABASE inherit_multi_delete DBCOMPATIBILITY = 'B';
\c inherit_multi_delete;

create table t_father_opengauss_inherit_alter_inherit_case0001_1 (id int,
f1_name varchar(20),
f1_age int
);
create table t_son_opengauss_inherit_alter_inherit_case0001_1 (id int,f1_name varchar(20),f1_age int,s_name varchar(20),s_age int);

alter table t_son_opengauss_inherit_alter_inherit_case0001_1 inherit t_father_opengauss_inherit_alter_inherit_case0001_1;
drop table if exists t_father_opengauss_inherit_alter_inherit_case0001_1 cascade;
drop table if exists t_son_opengauss_inherit_alter_inherit_case0001_1 cascade;

-- five relation
DROP TABLE IF EXISTS t_t_mutil_t1;
DROP TABLE IF EXISTS t_t_mutil_t2;
DROP TABLE IF EXISTS t_t_mutil_t3;
DROP TABLE IF EXISTS t_t_mutil_t4;
DROP TABLE IF EXISTS t_t_mutil_t5;
CREATE TABLE t_t_mutil_t1(col1 int,col2 int);
CREATE TABLE t_t_mutil_t2(col1 int,col2 int);
CREATE TABLE t_t_mutil_t3 () INHERITS(t_t_mutil_t1);
CREATE TABLE t_t_mutil_t4 () INHERITS(t_t_mutil_t3);
CREATE TABLE t_t_mutil_t5(col1 int,col2 int); 
INSERT INTO t_t_mutil_t1 VALUES(1,1),(1,1);
INSERT INTO t_t_mutil_t2 VALUES(1,1),(1,2);
INSERT INTO t_t_mutil_t3 VALUES(1,1),(1,3);
INSERT INTO t_t_mutil_t4 VALUES(1,1),(1,4);
INSERT INTO t_t_mutil_t5 VALUES(1,1),(1,5);

delete FROM t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
delete FROM t_t_mutil_t3 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
begin;
delete FROM t_t_mutil_t4 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;
SELECT * FROM t_t_mutil_t2;
SELECT * FROM t_t_mutil_t4;
SELECT * FROM t_t_mutil_t5;
rollback;

-- delete xx FROM xxx;
delete t_t_mutil_t1 a FROM t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
delete t_t_mutil_t3 a FROM t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
begin;
delete t_t_mutil_t4 a FROM t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;
rollback;

delete a FROM t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
delete a FROM t_t_mutil_t3 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
begin;
delete a FROM t_t_mutil_t4 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;
rollback;

delete t_t_mutil_t1 a,t_t_mutil_t2 b FROM t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
delete t_t_mutil_t3 a,t_t_mutil_t2 b FROM t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
begin;
delete t_t_mutil_t4 a,t_t_mutil_t2 b FROM t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;
rollback;

delete a,b FROM t_t_mutil_t1 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
delete a,b FROM t_t_mutil_t3 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;-- error
begin;
delete a,b FROM t_t_mutil_t4 a,t_t_mutil_t2 b,t_t_mutil_t5 c WHERE a.col2=b.col2 and b.col2=c.col2;
rollback;

delete a FROM t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2;-- error
delete a FROM t_t_mutil_t3 a left join t_t_mutil_t2 b on a.col2=b.col2;-- error
begin;
delete a FROM t_t_mutil_t4 a left join t_t_mutil_t2 b on a.col2=b.col2;
rollback;

delete a FROM t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error
delete a FROM t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 order by a.col2; -- error
delete a FROM t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 returning *; -- error
delete t_t_mutil_t1 a FROM t_t_mutil_t1 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error

delete a FROM t_t_mutil_t3 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error
delete a FROM t_t_mutil_t3 a left join t_t_mutil_t2 b on a.col2=b.col2 order by a.col2; -- error
delete a FROM t_t_mutil_t3 a left join t_t_mutil_t2 b on a.col2=b.col2 returning *; -- error
delete t_t_mutil_t3 a FROM t_t_mutil_t3 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error

delete a FROM t_t_mutil_t4 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error
delete a FROM t_t_mutil_t4 a left join t_t_mutil_t2 b on a.col2=b.col2 order by a.col2; -- error
delete a FROM t_t_mutil_t4 a left join t_t_mutil_t2 b on a.col2=b.col2 returning *; -- error
delete t_t_mutil_t4 a FROM t_t_mutil_t4 a left join t_t_mutil_t2 b on a.col2=b.col2 limit 1; -- error

-- condition is false
delete FROM t_t_mutil_t1 a,t_t_mutil_t2 b WHERE a.col1 = 1 and a.col1=2;-- error
delete FROM t_t_mutil_t3 a,t_t_mutil_t2 b WHERE a.col1 = 1 and a.col1=2;-- error
delete FROM t_t_mutil_t4 a,t_t_mutil_t2 b WHERE a.col1 = 1 and a.col1=2;

\c regression
DROP DATABASE inherit_multi_delete;
