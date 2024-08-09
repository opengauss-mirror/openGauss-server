CREATE schema schema_vastbase_subpartition_hash;
set search_path to schema_vastbase_subpartition_hash;
-- init
set datestyle = 'ISO, MDY';
set behavior_compat_options = '';

create table t_subpart_normal_table_hash(id int);
create table t_subpart_part_table_hash(id int)
partition by hash(id)
(
    partition p1
);



----------------------------
-- Hash subpartition syntax
----------------------------
create table t_subpart_range_hash_1 (id integer, age integer, name varchar(30), sale integer)
partition by range(age)
subpartition by hash(age)
(
partition p1 values less than (10),
partition p2 values less than (100)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values less than (200)
);

create table t_subpart_list_hash_1 (id integer, age integer, name varchar(30), sale integer)
partition by list(age)
subpartition by hash(age)
(
partition p1 values (1, 2, 3, 4, 5),
partition p2 values (10, 20, 30, 40, 50)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values (111, 222, 333)
);

create table t_subpart_hash_hash_1 (id integer, age integer, name varchar(30), sale integer)
partition by hash(age)
subpartition by hash(age)
(
partition p1,
partition p2
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3
);

create table t_subpart_range_hash_2 (id integer, age numeric, name varchar(30), bd date)
partition by range(age)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values less than (10),
partition p2 values less than (100)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values less than (MAXVALUE)
    (
    subpartition sp3,
    subpartition sp4
    )
);

create table t_subpart_list_hash_2 (id integer, age numeric, name varchar(30), bd date)
partition by list(age)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values (1, 2, 3, 4, 5),
partition p2 values (10, 20, 30, 40, 50)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values (100, 200)
    (
    subpartition sp3,
    subpartition sp4
    )
);

create table t_subpart_hash_hash_2 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1,
partition p2
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3
    (
    subpartition sp3,
    subpartition sp4
    )
);

create table t_subpart_range_hash_3 (id integer, age numeric, name text, bd timestamp)
partition by range(id, age)
subpartition by hash(id)
    subpartitions 2
(
partition p1 values less than (10, 10.6789),
partition p2 values less than (100, 12345.6789)
    subpartitions 3,
partition p3 values less than (MAXVALUE, MAXVALUE)
    (
    subpartition sp1,
    subpartition sp2
    )
);

create table t_subpart_list_hash_3 (id integer, age numeric, name text, bd timestamp)
partition by list(age)
subpartition by hash(id)
    subpartitions 2
(
partition p1 values (10, 10.6789),
partition p2 values (100, 12345.6789)
    subpartitions 3,
partition p3 values (DEFAULT)
    (
    subpartition sp1,
    subpartition sp2
    )
);

create table t_subpart_hash_hash_3 (id integer, age numeric, name text, bd timestamp)
partition by hash(age)
subpartition by hash(id)
    subpartitions 2
(
partition p1,
partition p2
    subpartitions 3,
partition p3
    (
    subpartition sp1,
    subpartition sp2
    )
);

create table t_subpart_hash_hash_4 (id integer, age numeric, name text, bd timestamp)
partition by hash(age)
subpartition by hash(id)
    subpartitions 2
partitions 3;

select p1.tablename, p1.relname, p1.parttype, p1.partstrategy, p1.subpartstrategy,
p1.parentid, p1.boundaries, p1.relfilenode, p1.reltoastrelid
from schema_subpartition.v_subpartition p1
where p1.tablename like 't_subpart_range_hash_%'
    or p1.tablename like 't_subpart_list_hash_%'
    or p1.tablename like 't_subpart_hash_hash_%';

select p1.tablename, p1.subparttemplate
from schema_subpartition.v_subpartition p1
where p1.subparttemplate is not null
    and (p1.tablename like 't_subpart_range_hash_%'
         or p1.tablename like 't_subpart_list_hash_%'
         or p1.tablename like 't_subpart_hash_hash_%');

select get_subpart_template('t_subpart_range_hash_1'::regclass, 0) is null;
select pg_get_tabledef('t_subpart_range_hash_1');
select get_subpart_template('t_subpart_range_hash_2'::regclass, 0);
select get_subpart_template('t_subpart_list_hash_3'::regclass, 2);
select get_subpart_template('t_subpart_hash_hash_4'::regclass, 4);
select pg_get_tabledef('t_subpart_range_hash_2');
select pg_get_tabledef('t_subpart_list_hash_3');
select pg_get_tabledef('t_subpart_hash_hash_4');

create table t_subpart_range_hash_float4 (col1 float4)
partition by range(col1)
subpartition by hash(col1)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values less than (-34.84),
partition p2 values less than (0),
partition p3 values less than (1004.3)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p4 values less than (1.2345678901234e+20)
);

create table t_subpart_list_hash_float4 (col1 float4)
partition by list(col1)
subpartition by hash(col1)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values (-3.1, -3.14, -3.141, -3.1415, -3.14159, -3.141592, -3.1415926),
partition p2 values (0, 10, 100, 1000, 10000)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values (1.2345678901234e-20, 1.2345678901234e-10, 1.2345678901234e+10, 1.2345678901234e+20)
);

create table t_subpart_hash_hash_float4 (col1 float4)
partition by hash(col1)
subpartition by hash(col1)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1,
partition p2
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3
);

create table t_subpart_range_hash_float8 (col1 float8)
partition by range(col1)
subpartition by hash(col1)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values less than (-34.84),
partition p2 values less than (0),
partition p3 values less than (1004.3)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p4 values less than (1.2345678901234e+200)
);

create table t_subpart_list_hash_float8 (col1 float8)
partition by list(col1)
subpartition by hash(col1)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values (-3.1, -3.14, -3.141, -3.1415, -3.14159, -3.141592, -3.1415926),
partition p2 values (0, 10, 100, 1000, 10000)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values (1.2345678901234e-200, 1.2345678901234e-100, 1.2345678901234e+100, 1.2345678901234e+200)
);

create table t_subpart_hash_hash_float8 (col1 float8)
partition by hash(col1)
subpartition by hash(col1)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1,
partition p2
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3
);

select p1.tablename, p1.relname, p1.parttype, p1.partstrategy, p1.subpartstrategy,
p1.parentid, p1.boundaries, p1.relfilenode, p1.reltoastrelid
from schema_subpartition.v_subpartition p1
where p1.tablename like 't_subpart_range_hash_float%'
    or p1.tablename like 't_subpart_list_hash_float%'
    or p1.tablename like 't_subpart_hash_hash_float%';

select p1.tablename, p1.subparttemplate
from schema_subpartition.v_subpartition p1
where p1.subparttemplate is not null
    and (p1.tablename like 't_subpart_range_hash_float%'
         or p1.tablename like 't_subpart_list_hash_float%'
         or p1.tablename like 't_subpart_hash_hash_float%');

create table t_subpart_range_hash_7 (id integer primary key, age numeric, name varchar(30), bd date)
partition by range(id)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values less than (100),
partition p2 values less than (500)
    (
    subpartition sp1,
    subpartition sp2
    )
);

create table t_subpart_list_hash_7 (id integer primary key, age numeric, name varchar(30), bd date)
partition by list(id)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1 values (100),
partition p2 values (500)
    (
    subpartition sp1,
    subpartition sp2
    )
);

create table t_subpart_hash_hash_7 (id integer primary key, age numeric, name varchar(30), bd date)
partition by hash(id)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp2
    )
(
partition p1,
partition p2
    (
    subpartition sp1,
    subpartition sp2
    )
);


create table t_subpart_range_hash_8 (id integer, age numeric, name char(30), bd date,
    CONSTRAINT i_t_subpart_range_hash_8 PRIMARY KEY (id, age, name))
partition by range(age, name)
subpartition by hash(id)
(
partition p1 values less than (20, 'AAA')
);

create table t_subpart_list_hash_8 (id integer, age numeric, name char(30), bd date,
    CONSTRAINT i_t_subpart_list_hash_8 PRIMARY KEY (id, age, name))
partition by list(age)
subpartition by hash(id)
(
partition p1 values (20)
);

create table t_subpart_hash_hash_8 (id integer, age integer, name char(30), bd date,
    CONSTRAINT i_t_subpart_hash_hash_8 PRIMARY KEY (id, age, name))
partition by hash(age)
subpartition by hash(id)
(
partition p1
);

create table t_subpart_range_hash_9 (id integer, age numeric, name char(30), bd date,
    CONSTRAINT i_t_subpart_range_hash_9 PRIMARY KEY (age, name))
partition by range(age, name)
subpartition by hash(id)
(
partition p1 values less than (100, 'AAA')
);

create table t_subpart_list_hash_9 (id integer, age numeric, name char(30), bd date,
    CONSTRAINT i_t_subpart_list_hash_9 PRIMARY KEY (id, name))
partition by list(age)
subpartition by hash(id)
(
partition p1 values (100)
);

create table t_subpart_hash_hash_9 (id integer, age numeric, name char(30), bd date,
    CONSTRAINT i_t_subpart_hash_hash_9 PRIMARY KEY (bd, name))
partition by hash(age)
subpartition by hash(id)
(
partition p1
);

create unique index i_t_subpart_range_hash_8_1 on t_subpart_range_hash_8 (id, bd);
create unique index i_t_subpart_list_hash_8_1 on t_subpart_list_hash_8 (id, bd);
create unique index i_t_subpart_hash_hash_8_1 on t_subpart_hash_hash_8 (id, bd);


create table t_subpart_range_hash_10 (id integer, age numeric, name char(30), bd date)
partition by range(age, name)
subpartition by hash(id)
(
partition p1 values less than (10, 'AAA')
    (
    subpartition sp1
    ),
partition p2 values less than (100, 'MAXVALUE')
    (
    subpartition sp2,
    subpartition sp3
    )
);

create table t_subpart_list_hash_10 (id integer, age numeric, name char(30), bd date)
partition by list(age)
subpartition by hash(id)
(
partition p1 values (10)
    (
    subpartition sp1
    ),
partition p2 values (100)
    (
    subpartition sp2,
    subpartition sp3
    )
);

create table t_subpart_hash_hash_10 (id integer, age integer, name char(30), bd date)
partition by hash(age)
subpartition by hash(id)
(
partition p1
    (
    subpartition sp1
    ),
partition p2
    (
    subpartition sp2,
    subpartition sp3
    )
);

create unique index i_t_subpart_range_hash_10_1 on t_subpart_range_hash_10 (id) local; -- error
create unique index i_t_subpart_range_hash_10_1 on t_subpart_range_hash_10 (name, age) local; -- error
create unique index i_t_subpart_range_hash_10_1 on t_subpart_range_hash_10 (age, name, id) local;
create index i_t_subpart_range_hash_10_2 on t_subpart_range_hash_10 (name, age) local;

create unique index i_t_subpart_list_hash_10_1 on t_subpart_list_hash_10 (age) local; -- error
create unique index i_t_subpart_list_hash_10_1 on t_subpart_list_hash_10 (name, bd) local; -- error
create unique index i_t_subpart_list_hash_10_1 on t_subpart_list_hash_10 (age, id) local;
create index i_t_subpart_list_hash_10_2 on t_subpart_list_hash_10 (name, age) local;

create unique index i_t_subpart_hash_hash_10_1 on t_subpart_hash_hash_10 (bd) local; -- error
create unique index i_t_subpart_hash_hash_10_1 on t_subpart_hash_hash_10 (name, bd) local; -- error
create unique index i_t_subpart_hash_hash_10_1 on t_subpart_hash_hash_10 (age, id, bd) local;
create index i_t_subpart_hash_hash_10_2 on t_subpart_hash_hash_10 (age, bd) local;

create index i_t_subpart_range_hash_10_3 on t_subpart_range_hash_10 (bd) local
(
partition p1_idx
    (
    subpartition subp1_index_local
    ),
partition p2_idx
    (
    subpartition subp2_index_local
    )
); -- error
create index i_t_subpart_range_hash_10_3 on t_subpart_range_hash_10 (bd) local
(
partition p1_idx
    (
    subpartition subp1_bd_idx_local
    ),
partition p2_idx
    (
    subpartition subp2_bd_idx_local,
    subpartition subp3_bd_idx_local
    )
);

create index i_t_subpart_list_hash_10_3 on t_subpart_list_hash_10 (bd) local
(
partition p1_idx
    (
    subpartition subp1_index_local
    ),
partition p2_idx
    (
    subpartition subp2_index_local
    )
); -- error
create index i_t_subpart_list_hash_10_3 on t_subpart_list_hash_10 (bd) local
(
partition p1_idx
    (
    subpartition subp1_bd_idx_local
    ),
partition p2_idx
    (
    subpartition subp2_bd_idx_local,
    subpartition subp3_bd_idx_local
    )
);

create index i_t_subpart_hash_hash_10_3 on t_subpart_hash_hash_10 (bd) local
(
partition p1_idx
    (
    subpartition subp1_index_local
    ),
partition p2_idx
    (
    subpartition subp2_index_local
    )
); -- error
create index i_t_subpart_hash_hash_10_3 on t_subpart_hash_hash_10 (bd) local
(
partition p1_idx
    (
    subpartition subp1_bd_idx_local
    ),
partition p2_idx
    (
    subpartition subp2_bd_idx_local,
    subpartition subp3_bd_idx_local
    )
);

create unique index i_t_subpart_range_hash_10_4 on t_subpart_range_hash_10 (name, age) global; -- error
create unique index i_t_subpart_range_hash_10_4 on t_subpart_range_hash_10 (age, bd) global;
drop index i_t_subpart_range_hash_10_2;
create unique index i_t_subpart_range_hash_10_5 on t_subpart_range_hash_10 (name, age) global;

create unique index i_t_subpart_list_hash_10_4 on t_subpart_list_hash_10 (name, age) global; -- error
create unique index i_t_subpart_list_hash_10_4 on t_subpart_list_hash_10 (name, bd) global;
drop index i_t_subpart_list_hash_10_2;
create unique index i_t_subpart_list_hash_10_5 on t_subpart_list_hash_10 (name, age) global;

create unique index i_t_subpart_hash_hash_10_4 on t_subpart_hash_hash_10 (bd, age) global; -- error
create unique index i_t_subpart_hash_hash_10_4 on t_subpart_hash_hash_10 (name, id) global;
drop index i_t_subpart_hash_hash_10_2;
create unique index i_t_subpart_hash_hash_10_5 on t_subpart_hash_hash_10 (bd, age) global;


select p1.tablename, p1.relname, p1.reltoastidxid, p1.indextblid
from schema_subpartition.v_subpartition p1
where p1.tablename in ('t_subpart_range_hash_7', 't_subpart_range_hash_8', 't_subpart_range_hash_10');

select p1.tablename, p1.relname, p1.reltoastidxid, p1.indextblid
from schema_subpartition.v_subpartition p1
where p1.tablename in ('t_subpart_list_hash_7', 't_subpart_list_hash_8', 't_subpart_list_hash_10');

select p1.tablename, p1.relname, p1.reltoastidxid, p1.indextblid
from schema_subpartition.v_subpartition p1
where p1.tablename in ('t_subpart_hash_hash_7', 't_subpart_hash_hash_8', 't_subpart_hash_hash_10');


select * from pg_indexes where tablename like 't_subpart_range_hash_%' order by tablename, indexname;
select * from pg_indexes where tablename like 't_subpart_list_hash_%' order by tablename, indexname;
select * from pg_indexes where tablename like 't_subpart_hash_hash_%' order by tablename, indexname;


-- \d
\d t_subpart_range_hash_8
\d t_subpart_list_hash_8
\d t_subpart_hash_hash_8
\d t_subpart_range_hash_10
\d t_subpart_list_hash_10
\d t_subpart_hash_hash_10



create table t_subpart_range_hash_11 (id integer, age numeric, name varchar(30), bd date)
partition by range(age)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    )
(
partition p1 values less than (10),
partition p2 values less than (100) tablespace ts_subpart_hash_2,
partition p3 values less than (1000)
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    ),
partition p4 values less than (MAXVALUE) tablespace ts_subpart_hash_2
    (
    subpartition sp3 tablespace ts_subpart_hash_1,
    subpartition sp4
    )
);

create table t_subpart_list_hash_11 (id integer, age numeric, name varchar(30), bd date)
partition by list(age)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    )
(
partition p1 values (10),
partition p2 values (20) tablespace ts_subpart_hash_2,
partition p3 values (30)
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    ),
partition p4 values (DEFAULT) tablespace ts_subpart_hash_2
    (
    subpartition sp3 tablespace ts_subpart_hash_1,
    subpartition sp4
    )
);

create table t_subpart_hash_hash_11 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    )
(
partition p1,
partition p2 tablespace ts_subpart_hash_2,
partition p3
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    ),
partition p4 tablespace ts_subpart_hash_2
    (
    subpartition sp3 tablespace ts_subpart_hash_1,
    subpartition sp4
    )
);

create table t_subpart_hash_hash_11_2 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
    subpartitions 3 store in (ts_subpart_hash_1, ts_subpart_hash_2)
partitions 5 store in (ts_subpart_hash_2, ts_subpart_hash_1);

alter table t_subpart_hash_hash_11_2 add partition p6;
alter table t_subpart_hash_hash_11_2 modify partition p6 add subpartition p6_sp3;

select p1.tablename, p1.relname, p1.parttype, p2.spcname tablespace_name
from schema_subpartition.v_subpartition p1 left join pg_tablespace p2 on p1.reltablespace = p2.oid
where p1.tablename in ('t_subpart_range_hash_11', 't_subpart_list_hash_11', 't_subpart_hash_hash_11', 't_subpart_hash_hash_11_2')
order by p1.parentid, p1.oid;

select p1.tablename, p1.subparttemplate
from schema_subpartition.v_subpartition p1
where p1.subparttemplate is not null
    and p1.tablename in ('t_subpart_range_hash_11', 't_subpart_list_hash_11', 't_subpart_hash_hash_11', 't_subpart_hash_hash_11_2');


SET SESSION AUTHORIZATION user_subpart_hash PASSWORD 'Test@123';
create table t_subpart_hash_hash_12 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1 tablespace ts_subpart_hash_test_user,
    subpartition sp2 tablespace ts_subpart_hash_1
    )
(
partition p1
);

create table t_subpart_hash_hash_12 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
(
partition p1 tablespace ts_subpart_hash_test_user,
partition p2 tablespace ts_subpart_hash_1
);

create table t_subpart_hash_hash_12 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
(
partition p1
    (
    subpartition sp1 tablespace ts_subpart_hash_test_user,
    subpartition sp2 tablespace ts_subpart_hash_1
    )
);

create table t_subpart_hash_hash_12 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
    subpartitions 2 store in (ts_subpart_hash_test_user, ts_subpart_hash_1)
(
partition p1
);

create table t_subpart_hash_hash_12 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
partitions 2 store in (ts_subpart_hash_test_user, ts_subpart_hash_1);

create table t_subpart_hash_hash_12 (id integer, age numeric, name varchar(30), bd date)
partition by hash(age)
subpartition by hash(id)
(
partition p1
    subpartitions 2 store in (ts_subpart_hash_test_user, ts_subpart_hash_1)
);

RESET SESSION AUTHORIZATION;



----------------------------
-- syntax error
----------------------------
create table t_subpart_error (id integer, name varchar(30))
partition by range(id)
(
partition p1 values less than (10)
    (
    subpartition sp1
    )
);

create table t_subpart_error (id integer, name varchar(30))
partition by list(id)
(
partition p1 values (10)
    (
    subpartition sp1
    )
);

create table t_subpart_error (id integer, name varchar(30))
partition by hash(id)
(
partition p1
    (
    subpartition sp1
    )
);


create table t_subpart_error (id integer, name varchar(30))
partition by range(name)
subpartition by hash(id)
(
partition p1 values less than ('a')
    (
    subpartition sp1
    ),
partition p2 values less than ('A')
    (
    subpartition sp1
    )
);

create table t_subpart_error (id integer, name varchar(30))
partition by list(name)
subpartition by hash(id)
(
partition p1 values ('a')
    (
    subpartition sp1
    ),
partition p2 values ('A')
    (
    subpartition sp1
    )
);

create table t_subpart_error (id integer, name int8)
partition by hash(name)
subpartition by hash(id)
(
partition p1
    (
    subpartition sp1
    ),
partition p2
    (
    subpartition sp1
    )
);


create table t_subpart_error (id integer, name varchar(30))
partition by range(name)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp1
    )
(
partition p1 values less than ('a')
);

create table t_subpart_error (id integer, name varchar(30))
partition by list(name)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp1
    )
(
partition p1 values ('a')
);

create table t_subpart_error (id integer, name int2)
partition by hash(name)
subpartition by hash(id)
    subpartition template
    (
    subpartition sp1,
    subpartition sp1
    )
(
partition p1
);



create table t_subpart_error (id integer, name varchar(30))
partition by range(name)
subpartition by hash(id)
(
partition p1 values less than ('10')
    (
    subpartition sp1
    ),
partition p2 values less than ('100')
    (
    subpartition p1
    )
);

create table t_subpart_error (id integer, name varchar(30))
partition by list(id)
subpartition by hash(id)
(
partition p1 values ('10')
    (
    subpartition sp1
    ),
partition p2 values ('100')
    (
    subpartition p1
    )
);

create table t_subpart_error (id integer, name int4)
partition by hash(name)
subpartition by hash(id)
(
partition p1
    (
    subpartition sp1
    ),
partition p2
    (
    subpartition p1
    )
);
create table t_subpart_error (id integer, name int4)
partition by hash(name)
subpartition by hash(id)
(
partition p1
    (
    subpartition sp1
    ),
partition sp1
    (
    subpartition sp2
    )
);



create table t_subpart_error (id integer, name text)
partition by range(name)
subpartition by hash(id)
(
partition p1 values less than ('10')
    (
    subpartition p2_subpartdefault1
    ),
partition p2 values less than ('100')
);
drop table t_subpart_error;
create table t_subpart_error (id integer, name text)
partition by list(name)
subpartition by hash(id)
(
partition p1 values ('10')
    (
    subpartition p2_subpartdefault1
    ),
partition p2 values ('100')
);
drop table t_subpart_error;
create table t_subpart_error (id integer, name integer)
partition by hash(name)
subpartition by hash(id)
(
partition p1
    (
    subpartition p2_subpartdefault1
    ),
partition p2
);
drop table t_subpart_error;
create table t_subpart_error (id integer, name varchar(30))
partition by hash(id)
subpartition by hash(id)
(
partition p1,
partition p2
    (
    subpartition p1_subpartdefault1
    )
);
drop table t_subpart_error;


create table t_subpart_error (id integer, name varchar(30), age int, bd varchar(30), addr varchar(30))
partition by hash(id)
subpartition by hash(id, name, age, bd, addr)
(
partition p1
);


create table t_subpart_error (id integer, name varchar(30), m money)
partition by hash(id)
subpartition by hash(m)
(
partition p1
);
create table t_subpart_error (id integer, name varchar(30), m money) -- now is ok
partition by hash(id)
subpartition by hash(name)
(
partition p1
);
drop table t_subpart_error;
create table t_subpart_error (id integer, name varchar(30), bd date) -- now is ok
partition by hash(id)
subpartition by hash(bd)
(
partition p1
);
drop table t_subpart_error;


create table t_subpart_error (id integer, name varchar(30), age int)
partition by hash(id)
subpartition by hash(age)
(
partition p1
    (
    subpartition sp1 values less than (1)
    )
);
create table t_subpart_error (id integer, name varchar(30), age int)
partition by hash(id)
subpartition by hash(age)
(
partition p1
    (
    subpartition sp1 end (1)
    )
);

create table t_subpart_error (id integer, name varchar(30))
partition by hash(id)
subpartition by hash(name)
(
partition p1
    (
    subpartition sp1 values ('a', 'b')
    )
);

create table t_subpart_error (id integer, name varchar(30))
partition by hash(id)
subpartition by hash(name)
    subpartition template
    (
    subpartition sp1 values less than (1)
    )
(
partition p1
);
create table t_subpart_error (id integer, name varchar(30))
partition by hash(id)
subpartition by hash(name)
    subpartition template
    (
    subpartition sp1 end (1)
    )
(
partition p1
);
create table t_subpart_error (id integer, name varchar(30))
partition by hash(id)
subpartition by hash(name)
    subpartition template
    (
    subpartition sp1 values ('a')
    )
(
partition p1
);


create table t_subpart_error (id integer, name integer)
partition by hash(id)
subpartition by hash(name)
    subpartition template
    (
    subpartition sp1
        (
        subpartition ssp1 values (DEFAULT)
        )
    )
(
partition p1
);


create table t_subpart_error (id integer, name integer)
partition by hash(id)
subpartition by hash(name)
    subpartition template
    (
    subpartition sp1 values (DEFAULT)
    )
(
partition p1
);
create table t_subpart_error (id integer, name integer)
partition by hash(id)
subpartition by hash(name)
    subpartition template
    (
    subpartition sp1 values (DEFAULT),
    subpartition sp2
    )
(
partition p1
);
create table t_subpart_error (id integer, name integer)
partition by hash(id)
subpartition by hash(name)
(
partition p1
    (
    subpartition sp1 values (DEFAULT)
    )
);
create table t_subpart_error (id integer, name integer)
partition by hash(id)
subpartition by hash(name)
(
partition p1
    (
    subpartition sp1 values (DEFAULT),
    subpartition sp2
    )
);



alter table t_subpart_hash_hash_2 drop column age;
alter table t_subpart_hash_hash_2 drop column id;
alter table t_subpart_hash_hash_2 modify (age numeric(6,1));
alter table t_subpart_hash_hash_2 modify (id text);


alter table t_subpart_range_hash_1 add partition p4 values less than (300);
alter table t_subpart_range_hash_1 add partition p5 start (300) end (400)
(
    subpartition sp3,
    subpartition sp4,
    subpartition sp5
);
alter table t_subpart_range_hash_1 add partition p6 values less than (500)
(
    subpartition sp6,
    subpartition sys_subp4294967295
);

alter table t_subpart_list_hash_1 add partition p4 values (300);
alter table t_subpart_list_hash_1 add partition p5 values (400)
(
    subpartition sp3,
    subpartition sp4,
    subpartition sp5
);
alter table t_subpart_list_hash_1 add partition p6 values (500)
(
    subpartition sp6,
    subpartition sys_subp4294967295
);

alter table t_subpart_hash_hash_1 add partition p4;
alter table t_subpart_hash_hash_1 add partition p5
(
    subpartition sp3,
    subpartition sp4,
    subpartition sp5
);
alter table t_subpart_hash_hash_1 add partition p6
(
    subpartition sp6,
    subpartition sys_subp4294967295
);


alter table t_subpart_range_hash_7 add partition p3 end (1000);
alter table t_subpart_range_hash_7 add partition p4 values less than (2000)
(
    subpartition sp3,
    subpartition p5_sp2
);
alter table t_subpart_range_hash_10 add partition p3 values less than (MAXVALUE, MAXVALUE)
(
    subpartition sp4,
    subpartition sp5
);

alter table t_subpart_list_hash_7 add partition p3 values (1000);
alter table t_subpart_list_hash_7 add partition p4 values (2000)
(
    subpartition sp3,
    subpartition p5_sp2
);
alter table t_subpart_list_hash_10 add partition p3 values (DEFAULT)
(
    subpartition sp4,
    subpartition sp5
);
alter table t_subpart_hash_hash_7 add partition p3;
alter table t_subpart_hash_hash_7 add partition p4
(
    subpartition sp3,
    subpartition p5_sp2
);
alter table t_subpart_hash_hash_10 add partition p3
(
    subpartition sp4,
    subpartition sp5
);



alter table t_subpart_normal_table_hash add partition p1;
alter table t_subpart_normal_table_hash add partition p2
(
    subpartition sp1,
    subpartition sp2
);

alter table t_subpart_part_table_hash add partition p2
(
    subpartition sp1,
    subpartition sp2
);


alter table t_subpart_range_hash_1 add partition p_error values (500);
alter table t_subpart_range_hash_1 add partition p_error;
alter table t_subpart_range_hash_1 add partition p7 end (500)
(
    subpartition sp_error values less than (100)
);
alter table t_subpart_range_hash_1 add partition p7 end (500)
(
    subpartition sp_error start (100)
);
alter table t_subpart_range_hash_1 add partition p7 end (500)
(
    subpartition sp_error values (0)
);

alter table t_subpart_list_hash_1 add partition p_error values less than (500);
alter table t_subpart_list_hash_1 add partition p_error;
alter table t_subpart_list_hash_1 add partition p7 values (700)
(
    subpartition sp_error end (100)
);
alter table t_subpart_list_hash_1 add partition p7 values (700)
(
    subpartition sp_error values (0)
);

alter table t_subpart_hash_hash_1 add partition p_error values less than (500);
alter table t_subpart_hash_hash_1 add partition p_error end (500);
alter table t_subpart_hash_hash_1 add partition p_error values (0);
alter table t_subpart_hash_hash_1 add partition p7
(
    subpartition sp_error values less than (100)
);
alter table t_subpart_hash_hash_1 add partition p7
(
    subpartition sp_error end (100)
);
alter table t_subpart_hash_hash_1 add partition p7
(
    subpartition sp_error values (1)
);
alter table t_subpart_hash_hash_1 add partition p7
(
    subpartition sp_error
);
alter table t_subpart_hash_hash_1 add partitions 1;
alter table t_subpart_hash_hash_1 add partition p7
(
    subpartitions 2
);



alter table t_subpart_hash_hash_1 add partition p_error
(
    subpartition sp3,
    subpartition sp3
);
alter table t_subpart_hash_hash_1 add partition p_error
(
    subpartition sp3,
    subpartition p_error
);
alter table t_subpart_hash_hash_1 add partition p_error
(
    subpartition sp1,
    subpartition sp22
);
alter table t_subpart_hash_hash_1 add partition p_error
(
    subpartition p1
);
alter table t_subpart_hash_hash_7 add partition p3;
alter table t_subpart_hash_hash_7 add partition sp1;


alter table t_subpart_range_hash_7 add partition p5 values less than (MAXVALUE);
alter table t_subpart_list_hash_7 add partition p5 values (DEFAULT);
alter table t_subpart_hash_hash_7 add partition p5;



alter table t_subpart_range_hash_10 add partition p_error values less than (9999, 9999);
alter table t_subpart_list_hash_10 add partition p_error values (9999);
alter table t_subpart_hash_hash_10 add partition p_error;




alter table t_subpart_range_hash_1 modify partition p2 add subpartition p2_sp20;
alter table t_subpart_range_hash_10 modify partition p1 add subpartition p1_sp22;

alter table t_subpart_list_hash_1 modify partition p2 add subpartition p2_sp20;
alter table t_subpart_list_hash_10 modify partition p1 add subpartition p1_sp22;

alter table t_subpart_hash_hash_1 modify partition p2 add subpartition p2_sp20;
alter table t_subpart_hash_hash_10 modify partition p1 add subpartition p1_sp22;


alter table t_subpart_normal_table_hash modify partition p1 add subpartition sp1;
alter table t_subpart_normal_table_hash modify partition p1 add subpartition sp1
(
    subpartition sp3,
    subpartition sp4
);

alter table t_subpart_part_table_hash modify partition p1 add subpartition sp1;
alter table t_subpart_part_table_hash modify partition p1 add subpartition sp1
(
    subpartition sp3,
    subpartition sp4
);


alter table t_subpart_hash_hash_1 modify partition p2 add subpartition sp_error values less than (10);
alter table t_subpart_hash_hash_1 modify partition p2 add subpartition sp_error end (10);
alter table t_subpart_hash_hash_1 modify partition p2 add subpartition sp_error values (1000);


alter table t_subpart_range_hash_1 modify partition p_error add subpartition sp_error;
alter table t_subpart_range_hash_1 modify partition for (999) add subpartition sp_error;
alter table t_subpart_list_hash_1 modify partition p_error add subpartition sp_error;
alter table t_subpart_list_hash_1 modify partition for (999) add subpartition sp_error;
alter table t_subpart_hash_hash_1 modify partition p_error add subpartition sp21;
alter table t_subpart_hash_hash_1 modify partition for (999) add subpartition sp_error;


alter table t_subpart_range_hash_1 modify partition p2 add subpartition sp1;
alter table t_subpart_range_hash_1 modify partition p2 add subpartition sp3;
alter table t_subpart_range_hash_1 modify partition p2 add subpartition p1;

alter table t_subpart_list_hash_1 modify partition p2 add subpartition sp1;
alter table t_subpart_list_hash_1 modify partition p2 add subpartition sp3;
alter table t_subpart_list_hash_1 modify partition p2 add subpartition p1;

alter table t_subpart_hash_hash_1 modify partition p2 add subpartition sp1;
alter table t_subpart_hash_hash_1 modify partition p2 add subpartition sp3;
alter table t_subpart_hash_hash_1 modify partition p2 add subpartition p1;

alter table t_subpart_hash_hash_1 modify partition p2 add subpartitions 1;


select p1.tablename, p1.relname, p1.parttype, p1.partstrategy,
p1.parentid, p1.boundaries, p1.relfilenode, p1.reltoastrelid
from schema_subpartition.v_subpartition p1
where p1.tablename in ('t_subpart_range_hash_1', 't_subpart_range_hash_7', 't_subpart_range_hash_10',
    't_subpart_list_hash_1', 't_subpart_list_hash_7', 't_subpart_list_hash_10',
    't_subpart_hash_hash_1', 't_subpart_hash_hash_7', 't_subpart_hash_hash_10');




create table t_subpart_hash_hash_13 (id integer, age int)
partition by hash(id)
subpartition by hash(age)
    subpartition template
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    )
(
partition p1,
partition p2 tablespace ts_subpart_hash_2,
partition p3
    (
    subpartition sp1 tablespace ts_subpart_hash_1,
    subpartition sp2
    ),
partition p4 tablespace ts_subpart_hash_2
    (
    subpartition sp3 tablespace ts_subpart_hash_1,
    subpartition sp4
    )
);

alter table t_subpart_hash_hash_13 add partition p5;
alter table t_subpart_hash_hash_13 add partition p6 tablespace ts_subpart_hash_2;
alter table t_subpart_hash_hash_13 add partition p7 tablespace ts_subpart_hash_2
(
    subpartition sp5,
    subpartition sp6 tablespace ts_subpart_hash_1
);
alter table t_subpart_hash_hash_13 add partition p8 
(
    subpartition sp7,
    subpartition sp8 tablespace ts_subpart_hash_1
);

alter table t_subpart_hash_hash_13 modify partition p1 add subpartition p1_sp20;
alter table t_subpart_hash_hash_13 modify partition p1 add subpartition p1_sp21 tablespace ts_subpart_hash_1;
alter table t_subpart_hash_hash_13 modify partition p2 add subpartition p2_sp22;
alter table t_subpart_hash_hash_13 modify partition p2 add subpartition p2_sp23 tablespace ts_subpart_hash_1;


SET SESSION AUTHORIZATION user_subpart_hash PASSWORD 'Test@123';
create table t_subpart_hash_hash_14 (id integer, age integer)
partition by hash (id)
subpartition by hash (age)
(
partition p1
    (
    subpartition sp1
    )
);

alter table t_subpart_hash_hash_14 add partition p2 tablespace ts_subpart_hash_1;
alter table t_subpart_hash_hash_14 add partition p2
(
    subpartition sp2,
    subpartition sp3 tablespace ts_subpart_hash_1
);
alter table t_subpart_hash_hash_14 modify partition p1 add subpartition p1_sp2 tablespace ts_subpart_hash_1;

drop table t_subpart_hash_hash_14;
RESET SESSION AUTHORIZATION;

select p1.tablename, p1.relname, p1.parttype, p2.spcname tablespace_name
from schema_subpartition.v_subpartition p1 left join pg_tablespace p2 on p1.reltablespace = p2.oid
where p1.tablename = 't_subpart_hash_hash_13'
order by p1.parentid, p1.oid;



alter table t_subpart_range_hash_1 drop partition p6;
alter table t_subpart_range_hash_1 drop partition for (350); -- drop p5
alter table t_subpart_range_hash_7 drop partition p3;
alter table t_subpart_range_hash_10 drop partition for (1, 'A'); -- drop p1

alter table t_subpart_list_hash_1 drop partition p6;
alter table t_subpart_list_hash_1 drop partition for (400); -- drop p5
alter table t_subpart_list_hash_7 drop partition p3;
alter table t_subpart_list_hash_10 drop partition for (10); -- drop p1

alter table t_subpart_hash_hash_1 drop partition p6;
alter table t_subpart_hash_hash_1 drop partition for (4);    -- drop p5
alter table t_subpart_hash_hash_7 drop partition p3;
alter table t_subpart_hash_hash_10 drop partition for (10); -- drop p1
alter table t_subpart_hash_hash_13 drop partition p4;



alter table t_subpart_range_hash_1 drop partition p_error;
alter table t_subpart_range_hash_7 drop partition for (9999);

alter table t_subpart_list_hash_1 drop partition p_error;
alter table t_subpart_list_hash_7 drop partition for (9999);

alter table t_subpart_hash_hash_1 drop partition p_error;
alter table t_subpart_hash_hash_7 drop partition for (9999);



alter table t_subpart_list_hash_10 drop partition p2;



alter table t_subpart_range_hash_10 drop partition p3; -- ok
alter table t_subpart_range_hash_10 drop partition p2; -- error

alter table t_subpart_list_hash_10 drop partition p3; -- ok
alter table t_subpart_list_hash_10 drop partition p2; -- error

alter table t_subpart_hash_hash_10 drop partition p3;  -- error
alter table t_subpart_hash_hash_10 drop partition p2;



alter table t_subpart_range_hash_1 drop subpartition sp1;
alter table t_subpart_range_hash_7 drop subpartition for (100, 101); -- drop sp2

alter table t_subpart_list_hash_1 drop subpartition sp1;
alter table t_subpart_list_hash_7 drop subpartition for (500, 101); -- drop sp2

alter table t_subpart_hash_hash_1 drop subpartition sp1;
alter table t_subpart_hash_hash_7 drop subpartition for (1, 9); -- drop sp2
alter table t_subpart_hash_hash_13 drop subpartition sp2;
alter table t_subpart_hash_hash_13 drop subpartition for (4, 100); -- drop p5_sp1

alter table t_subpart_range_hash_1 drop subpartition sp_error;
alter table t_subpart_range_hash_7 drop subpartition for (100, 1);

alter table t_subpart_list_hash_1 drop subpartition sp_error;
alter table t_subpart_list_hash_7 drop subpartition for (500, 1);

alter table t_subpart_hash_hash_1 drop subpartition sp_error;
alter table t_subpart_hash_hash_7 drop subpartition for (501, 1);


alter table t_subpart_range_hash_7 drop subpartition sp1;
alter table t_subpart_list_hash_7 drop subpartition sp1;
alter table t_subpart_hash_hash_7 drop subpartition sp1;



select p1.tablename, p1.relname, p1.parttype, p1.partstrategy,
p1.parentid, p1.boundaries, p1.relfilenode, p1.reltoastrelid
from schema_subpartition.v_subpartition p1
where p1.tablename in ('t_subpart_range_hash_1', 't_subpart_range_hash_7', 't_subpart_range_hash_10',
    't_subpart_list_hash_1', 't_subpart_list_hash_7', 't_subpart_list_hash_10',
    't_subpart_hash_hash_1', 't_subpart_hash_hash_7', 't_subpart_hash_hash_10');


select p1.tablename, p1.relname, p1.parttype, p2.spcname tablespace_name
from schema_subpartition.v_subpartition p1 left join pg_tablespace p2 on p1.reltablespace = p2.oid
where p1.tablename = 't_subpart_hash_hash_13'
order by p1.parentid, p1.oid;




select * from t_subpart_range_hash_1 partition (p2);
select * from t_subpart_range_hash_1 partition for (10);
select * from t_subpart_range_hash_1 subpartition (sp2);
select * from t_subpart_range_hash_1 subpartition for (50, 51);

select * from t_subpart_list_hash_1 partition (p2);
select * from t_subpart_list_hash_1 partition for (10);
select * from t_subpart_list_hash_1 subpartition (sp2);
select * from t_subpart_list_hash_1 subpartition for (50, 51);

select * from t_subpart_hash_hash_1 partition (p2);
select * from t_subpart_hash_hash_1 partition for (1);
select * from t_subpart_hash_hash_1 subpartition (sp2);
select * from t_subpart_hash_hash_1 subpartition for (51, 51);



update t_subpart_range_hash_1 partition (p2) set id = id + 10;
update t_subpart_range_hash_1 partition for (10) set id = id + 10;
update t_subpart_range_hash_1 subpartition (sp2) set id = id + 10;
update t_subpart_range_hash_1 subpartition for (50, 51) set id = id + 10;

update t_subpart_list_hash_1 partition (p2) set id = id + 10;
update t_subpart_list_hash_1 partition for (10) set id = id + 10;
update t_subpart_list_hash_1 subpartition (sp2) set id = id + 10;
update t_subpart_list_hash_1 subpartition for (50, 51) set id = id + 10;

update t_subpart_hash_hash_1 partition (p2) set id = id + 10;
update t_subpart_hash_hash_1 partition for (1) set id = id + 10;
update t_subpart_hash_hash_1 subpartition (sp2) set id = id + 10;
update t_subpart_hash_hash_1 subpartition for (51, 51) set id = id + 10;




delete from t_subpart_range_hash_1 partition (p2);
delete from t_subpart_range_hash_1 partition for (10);
delete from t_subpart_range_hash_1 subpartition (sp2);
delete from t_subpart_range_hash_1 subpartition for (50, 51);

delete from t_subpart_list_hash_1 partition (p2);
delete from t_subpart_list_hash_1 partition for (10);
delete from t_subpart_list_hash_1 subpartition (sp2);
delete from t_subpart_list_hash_1 subpartition for (50, 51);

delete from t_subpart_hash_hash_1 partition (p2);
delete from t_subpart_hash_hash_1 partition for (1);
delete from t_subpart_hash_hash_1 subpartition (sp2);
delete from t_subpart_hash_hash_1 subpartition for (51, 51);



-- range-hash
create table t_subpart_range_hash_20 (id integer, name text)
partition by range(name)
subpartition by hash(name)
(
partition p1 values less than ('e'),
partition p2 values less than ('k')
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values less than (MAXVALUE)
    (
    subpartition sp3,
    subpartition sp4
    )
);
insert into t_subpart_range_hash_20 values (1,'a');
insert into t_subpart_range_hash_20 values (2,'e');
insert into t_subpart_range_hash_20 values (3,'g');
insert into t_subpart_range_hash_20 values (4,'m');
insert into t_subpart_range_hash_20 values (5,'r');
insert into t_subpart_range_hash_20 values (6,NULL);

explain(costs off) select * from t_subpart_range_hash_20;

explain(costs off) select * from t_subpart_range_hash_20 where name is null;
select * from t_subpart_range_hash_20 where name is null;
explain(costs off) select * from t_subpart_range_hash_20 where name is not null;
select * from t_subpart_range_hash_20 where name is not null;

explain(costs off) select * from t_subpart_range_hash_20 where name = 'e';
select * from t_subpart_range_hash_20 where name = 'e';
explain(costs off) select * from t_subpart_range_hash_20 where name > 'e';
select * from t_subpart_range_hash_20 where name > 'e';
explain(costs off) select * from t_subpart_range_hash_20 where name >= 'e';
select * from t_subpart_range_hash_20 where name >= 'e';
explain(costs off) select * from t_subpart_range_hash_20 where name < 'e';
select * from t_subpart_range_hash_20 where name < 'e';
explain(costs off) select * from t_subpart_range_hash_20 where name <= 'e';
select * from t_subpart_range_hash_20 where name <= 'e';
explain(costs off) select * from t_subpart_range_hash_20 where name <> 'e';
select * from t_subpart_range_hash_20 where name <> 'e';

explain(costs off) select * from t_subpart_range_hash_20 where name = 'e' and name is null;
select * from t_subpart_range_hash_20 where name = 'e' and name is null;
explain(costs off) select * from t_subpart_range_hash_20 where name = 'e' or name is null;
select * from t_subpart_range_hash_20 where name = 'e' or name is null;

explain(costs off) select * from t_subpart_range_hash_20 where name in ('r', NULL);
select * from t_subpart_range_hash_20 where name in ('r', NULL);
explain(costs off) select * from t_subpart_range_hash_20 where name = any(array['e', 'g']) or name in ('r', NULL);
select * from t_subpart_range_hash_20 where name = any(array['e', 'g']) or name in ('r', NULL);


-- list-hash
create table t_subpart_list_hash_20 (id integer, age integer, name text)
partition by list(age)
subpartition by hash(name)
(
partition p1 values (1, 2, 3),
partition p2 values (10, 20, 50, 60)
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3 values (DEFAULT)
    (
    subpartition sp3,
    subpartition sp4
    )
);
insert into t_subpart_list_hash_20 values (1, 1, NULL);
insert into t_subpart_list_hash_20 values (2, 20, 'b');
insert into t_subpart_list_hash_20 values (3, 50, 'f');
insert into t_subpart_list_hash_20 values (4, 100, NULL);
insert into t_subpart_list_hash_20 values (5, NULL, 'g');
insert into t_subpart_list_hash_20 values (6, NULL, NULL);

explain(costs off) select * from t_subpart_list_hash_20;

explain(costs off) select * from t_subpart_list_hash_20 where age is null;
select * from t_subpart_list_hash_20 where age is null;
explain(costs off) select * from t_subpart_list_hash_20 where age is not null;
select * from t_subpart_list_hash_20 where age is not null;
explain(costs off) select * from t_subpart_list_hash_20 where name is null;
select * from t_subpart_list_hash_20 where name is null;
explain(costs off) select * from t_subpart_list_hash_20 where name is not null;
select * from t_subpart_list_hash_20 where name is not null;

explain(costs off) select * from t_subpart_list_hash_20 where age is null and name is null;
select * from t_subpart_list_hash_20 where age is null and name is null;
explain(costs off) select * from t_subpart_list_hash_20 where age is null or name is null;
select * from t_subpart_list_hash_20 where age is null or name is null;

explain(costs off) select * from t_subpart_list_hash_20 where age = 20;
select * from t_subpart_list_hash_20 where age = 20;
explain(costs off) select * from t_subpart_list_hash_20 where name = 'b';
select * from t_subpart_list_hash_20 where name = 'b';
explain(costs off) select * from t_subpart_list_hash_20 where age = 20 and name = 'b';
select * from t_subpart_list_hash_20 where age = 20 and name = 'b';
explain(costs off) select * from t_subpart_list_hash_20 where age = 20 or name = 'b';
select * from t_subpart_list_hash_20 where age = 20 or name = 'b';

explain(costs off) select * from t_subpart_list_hash_20 where age is null and name = 'b';
select * from t_subpart_list_hash_20 where age is null and name = 'b';
explain(costs off) select * from t_subpart_list_hash_20 where age is null or name = 'b';
select * from t_subpart_list_hash_20 where age is null or name = 'b';
explain(costs off) select * from t_subpart_list_hash_20 where age = 20 and name is null;
select * from t_subpart_list_hash_20 where age = 20 and name is null;
explain(costs off) select * from t_subpart_list_hash_20 where age = 20 or name is null;
select * from t_subpart_list_hash_20 where age = 20 or name is null;

explain(costs off) select * from t_subpart_list_hash_20 where name = any(array['g', NULL]);
select * from t_subpart_list_hash_20 where name = any(array['g', NULL]);
explain(costs off) select * from t_subpart_list_hash_20 where age in (20, 200) and name = any(array['g', NULL]);
select * from t_subpart_list_hash_20 where age in (20, 200) and name = any(array['g', NULL]);


-- hash-hash
create table t_subpart_hash_hash_20 (id integer, name text, bd time)
partition by hash(name)
subpartition by hash(bd)
(
partition p1,
partition p2
    (
    subpartition sp1,
    subpartition sp2
    ),
partition p3
    (
    subpartition sp3,
    subpartition sp4
    )
);
insert into t_subpart_hash_hash_20 values (1, 'a', '1:2:3');
insert into t_subpart_hash_hash_20 values (2, 'g', NULL);
insert into t_subpart_hash_hash_20 values (3, 'h', '11:2:3');
insert into t_subpart_hash_hash_20 values (4, 'o', NULL);
insert into t_subpart_hash_hash_20 values (5, 't', '21:0:0');
insert into t_subpart_hash_hash_20 values (6, NULL, NULL);

explain(costs off) select * from t_subpart_hash_hash_20;

explain(costs off) select * from t_subpart_hash_hash_20 where name is null;
select * from t_subpart_hash_hash_20 where name is null;
explain(costs off) select * from t_subpart_hash_hash_20 where name is not null;
select * from t_subpart_hash_hash_20 where name is not null;
explain(costs off) select * from t_subpart_hash_hash_20 where bd is null;
select * from t_subpart_hash_hash_20 where bd is null;
explain(costs off) select * from t_subpart_hash_hash_20 where bd is not null;
select * from t_subpart_hash_hash_20 where bd is not null;
explain(costs off) select * from t_subpart_hash_hash_20 where name is null and bd is null;
select * from t_subpart_hash_hash_20 where name is null and bd is null;

explain(costs off) select * from t_subpart_hash_hash_20 where name = 'g';
select * from t_subpart_hash_hash_20 where name = 'g';
explain(costs off) select * from t_subpart_hash_hash_20 where bd = '11:2:3';
select * from t_subpart_hash_hash_20 where bd = '11:2:3';
explain(costs off) select * from t_subpart_hash_hash_20 where name = 'g' and bd = '11:2:3';
select * from t_subpart_hash_hash_20 where name = 'g' and bd = '11:2:3';
explain(costs off) select * from t_subpart_hash_hash_20 where name = 'g' or bd = '11:2:3';
select * from t_subpart_hash_hash_20 where name = 'g' or bd = '11:2:3';

explain(costs off) select * from t_subpart_hash_hash_20 where name is null and bd = '11:2:3';
select * from t_subpart_hash_hash_20 where name is null and bd = '11:2:3';
explain(costs off) select * from t_subpart_hash_hash_20 where name is null or bd = '11:2:3';
select * from t_subpart_hash_hash_20 where name is null or bd = '11:2:3';
explain(costs off) select * from t_subpart_hash_hash_20 where name = 'g' and bd is null;
select * from t_subpart_hash_hash_20 where name = 'g' and bd is null;
explain(costs off) select * from t_subpart_hash_hash_20 where name = 'g' or bd is null;
select * from t_subpart_hash_hash_20 where name = 'g' or bd is null;

explain(costs off) select * from t_subpart_hash_hash_20 where bd = any(array['11:2:3'::time, '21:0:0'::time]);
select * from t_subpart_hash_hash_20 where bd = any(array['11:2:3'::time, '21:0:0'::time]);
explain(costs off) select * from t_subpart_hash_hash_20 where name in ('g','o') and bd = any(array['11:2:3'::time, '21:0:0'::time]);
select * from t_subpart_hash_hash_20 where name in ('g','o') and bd = any(array['11:2:3'::time, '21:0:0'::time]);



----------------------------
-- truncate partition & subpartition
----------------------------
-- PARTITION [FOR]
alter table t_subpart_range_hash_1 truncate partition p1;
alter table t_subpart_range_hash_1 truncate partition for (10);
alter table t_subpart_range_hash_10 truncate partition p2;
alter table t_subpart_range_hash_10 truncate partition for (10, 'MAXVALUE');

alter table t_subpart_list_hash_1 truncate partition p1;
alter table t_subpart_list_hash_1 truncate partition for (10);
alter table t_subpart_list_hash_10 truncate partition p2;
alter table t_subpart_list_hash_10 truncate partition for (100);

alter table t_subpart_hash_hash_1 truncate partition p1;
alter table t_subpart_hash_hash_1 truncate partition for (0);
alter table t_subpart_hash_hash_7 truncate partition p1;
alter table t_subpart_hash_hash_7 truncate partition for (100);
alter table t_subpart_hash_hash_10 truncate partition p2;
alter table t_subpart_hash_hash_10 truncate partition for (1);

-- SUBPARTITION [FOR]
alter table t_subpart_range_hash_1 truncate subpartition p1_subpartdefault1;
alter table t_subpart_range_hash_1 truncate subpartition for (100, 51);
alter table t_subpart_range_hash_10 truncate subpartition sp2;
alter table t_subpart_range_hash_10 truncate subpartition for (10, 'MAXVALUE', 9);

alter table t_subpart_list_hash_1 truncate subpartition p1_subpartdefault1;
alter table t_subpart_list_hash_1 truncate subpartition for (10, 51);
alter table t_subpart_list_hash_10 truncate subpartition sp2;
alter table t_subpart_list_hash_10 truncate subpartition for (100, 9);

alter table t_subpart_hash_hash_1 truncate subpartition p1_subpartdefault1;
alter table t_subpart_hash_hash_1 truncate subpartition for (11, 51);
alter table t_subpart_hash_hash_7 truncate subpartition sp1;
alter table t_subpart_hash_hash_7 truncate subpartition for (101, 10);
alter table t_subpart_hash_hash_10 truncate subpartition sp2;
alter table t_subpart_hash_hash_10 truncate subpartition for (1, 7);

alter table t_subpart_range_hash_1 truncate partition p_error;
alter table t_subpart_range_hash_1 truncate partition for (300);
alter table t_subpart_range_hash_1 truncate subpartition sp_error;
alter table t_subpart_range_hash_1 truncate subpartition for (10, 4); -- ok

alter table t_subpart_list_hash_1 truncate partition p_error;
alter table t_subpart_list_hash_1 truncate partition for (999);
alter table t_subpart_list_hash_1 truncate subpartition sp_error;
alter table t_subpart_list_hash_1 truncate subpartition for (10, 4); -- ok

alter table t_subpart_hash_hash_1 truncate partition p_error;
alter table t_subpart_hash_hash_1 truncate partition for (4); -- ok
alter table t_subpart_hash_hash_1 truncate subpartition sp_error;
alter table t_subpart_hash_hash_1 truncate subpartition for (11, 4); -- ok




alter table t_subpart_range_hash_1 set subpartition template
(
    subpartition sp1,
    subpartition sp2
);

alter table t_subpart_list_hash_1 set subpartition template
(
    subpartition sp1,
    subpartition sp2
);

alter table t_subpart_hash_hash_1 set subpartition template
(
    subpartition sp1,
    subpartition sp2
);



----------------------------
-- TODO SPLIT [SUB]PARTITION [FOR]
----------------------------
-- TODO SPLIT RANGE PARTITION [FOR]
alter table t_subpart_range_hash_2 split partition p1 at (5) into (partition p1_1, partition p1_2);
alter table t_subpart_range_hash_2 split partition for (50) at (50) into (partition p2_1, partition p2_2);

alter table t_subpart_range_hash_2 split partition p3 into (partition p3_1 end (200), partition p3_2 end (300), partition p3_3 end (400), partition p3_4 end (500), partition p3_5 end (MAXVALUE));
alter table t_subpart_range_hash_2 split partition for (50) into (partition p2_2_1 values less than (60), partition p2_2_2 values less than (100));


alter table t_subpart_range_hash_2 split partition p1_1 values (5) into (partition p1_1_1, partition p1_1_2);


-- TODO SPLIT LIST PARTITION [FOR]
alter table t_subpart_list_hash_2 split partition p1 values (5) into (partition p1_1, partition p1_2);
alter table t_subpart_list_hash_2 split partition for (100) values (200) into (partition p1_1, partition p1_2);
alter table t_subpart_list_hash_2 split partition p2 into (partition p3_1 values (10), partition p3_2 values (20), partition p3_3 values (30), partition p3_4 values (40), partition p3_5 values (50));

-- error, LIST partition not support  AT ... INTO
alter table t_subpart_list_hash_2 split partition p1 at (3) into (partition p1_1, partition p1_2);
alter table t_subpart_list_hash_2 split partition for (3) at (3) into (partition p1_1 values (10), partition p1_2 values (20.6789));


-- HASH partition not support SPLIT
alter table t_subpart_hash_hash_2 split partition p1 at (5) into (partition p1_1, partition p1_2);
alter table t_subpart_hash_hash_2 split partition for (0) at (5) into (partition p1_1, partition p1_2);
alter table t_subpart_hash_hash_2 split partition p2 values (5) into (partition p1_1, partition p1_2);
alter table t_subpart_hash_hash_2 split partition p3 into (partition p3_1 values less than (100), partition p3_2 values less than (200));

-- HASH subpartition not support   SPLIT
alter table t_subpart_hash_hash_3 split subpartition sp1 at ('a') into (subpartition sp1_1, subpartition sp1_2);
alter table t_subpart_hash_hash_3 split subpartition for (100, '1') at ('a') into (subpartition sp2_1, subpartition sp2_2 );
alter table t_subpart_hash_hash_3 split subpartition sp1 values ('a') into (subpartition sp1_1, subpartition sp1_2);
alter table t_subpart_hash_hash_3 split subpartition for (100, '1') values ('1', '2') into (subpartition sp2_1, subpartition sp2_2 );
alter table t_subpart_hash_hash_3 split subpartition sp3 into (subpartition sp3_1 values ('A'), subpartition sp3_2 values ('B'), subpartition sp3_3 values ('C'), subpartition sp3_4 values ('D', 'E'));
alter table t_subpart_hash_hash_3 split subpartition for (300, '1') into (subpartition sp5_1 values ('1', '2', '3', '4', '5'), subpartition sp5_2 values ('A', 'B', 'C', 'D', 'E'), subpartition sp5_3 values (DEFAULT));





-- TODO MERGE RANGE PARTITIONS [FOR]
alter table t_subpart_range_hash_1 merge partitions p1,p2 into partition p12;
alter table t_subpart_range_hash_1 merge partitions for (1), for (10), for (100), for (200) into partition p1234;
alter table t_subpart_range_hash_1 merge partitions p1 to p4 into partition p1234;

-- TODO MERGE LIST PARTITION [FOR]
alter table t_subpart_list_hash_1 merge partitions p1,p2 into partition p12;
alter table t_subpart_list_hash_1 merge partitions for (1), for (10), for (70), for (222) into partition p1234;

alter table t_subpart_list_hash_1 merge partitions p1 to p4 into partition p1234; -- error


alter table t_subpart_hash_hash_1 merge partitions p1,p1 into partition p12;
alter table t_subpart_hash_hash_1 merge partitions for (0), for (1) into partition p12;
alter table t_subpart_hash_hash_1 merge partitions p1 to p3 into partition p123;

alter table t_subpart_hash_hash_1 merge subpartitions sp1,sp1 into subpartition sp12;
alter table t_subpart_hash_hash_1 merge subpartitions for (1, 0), for (1, 1) into subpartition p12;
alter table t_subpart_hash_hash_1 merge subpartitions sp1 to sp2 into subpartition sp12;





----------------------------
-- TODO EXCHANGE PARTITION [FOR]
----------------------------
create table t_subpart_range_hash_8_exchange (like t_subpart_range_hash_8);
alter table t_subpart_range_hash_8_exchange add primary key (id, age, name);
create table t_subpart_list_hash_8_exchange (like t_subpart_list_hash_8);
alter table t_subpart_list_hash_8_exchange add primary key (id, age, name);
create table t_subpart_hash_hash_8_exchange (like t_subpart_hash_hash_8);
alter table t_subpart_hash_hash_8_exchange add primary key (id, age, name);

alter table t_subpart_range_hash_8 EXCHANGE PARTITION (p1) with table t_subpart_range_hash_8_exchange WITHOUT VALIDATION;
alter table t_subpart_range_hash_8 EXCHANGE PARTITION (p1) with table t_subpart_range_hash_8_exchange;
alter table t_subpart_range_hash_8 EXCHANGE PARTITION p1 with table t_subpart_range_hash_8_exchange WITH VALIDATION;
alter table t_subpart_range_hash_8 EXCHANGE PARTITION for (20, 'A') with table t_subpart_range_hash_8_exchange VERBOSE;
alter table t_subpart_range_hash_8 EXCHANGE PARTITION for (19, 'BBB') with table t_subpart_range_hash_8_exchange WITH VALIDATION VERBOSE;

alter table t_subpart_list_hash_8 EXCHANGE PARTITION (p1) with table t_subpart_list_hash_8_exchange WITHOUT VALIDATION;
alter table t_subpart_list_hash_8 EXCHANGE PARTITION (p1) with table t_subpart_list_hash_8_exchange;
alter table t_subpart_list_hash_8 EXCHANGE PARTITION p1 with table t_subpart_list_hash_8_exchange WITH VALIDATION;
alter table t_subpart_list_hash_8 EXCHANGE PARTITION for (20) with table t_subpart_list_hash_8_exchange VERBOSE;
alter table t_subpart_list_hash_8 EXCHANGE PARTITION for (20)with table t_subpart_list_hash_8_exchange WITH VALIDATION VERBOSE;

alter table t_subpart_hash_hash_8 EXCHANGE PARTITION (p1) with table t_subpart_hash_hash_8_exchange WITHOUT VALIDATION;
alter table t_subpart_hash_hash_8 EXCHANGE PARTITION (p1) with table t_subpart_hash_hash_8_exchange;
alter table t_subpart_hash_hash_8 EXCHANGE PARTITION p1 with table t_subpart_hash_hash_8_exchange WITH VALIDATION;
alter table t_subpart_hash_hash_8 EXCHANGE PARTITION for (10) with table t_subpart_hash_hash_8_exchange VERBOSE;
alter table t_subpart_hash_hash_8 EXCHANGE PARTITION for (10) with table t_subpart_hash_hash_8_exchange WITH VALIDATION VERBOSE;



----------------------------
-- EXCHANGE SUBPARTITION [FOR]
----------------------------
alter table t_subpart_range_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_range_hash_8_exchange WITHOUT VALIDATION;
alter table t_subpart_range_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_range_hash_8_exchange;
alter table t_subpart_range_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_range_hash_8_exchange WITH VALIDATION;
alter table t_subpart_range_hash_8 EXCHANGE SUBPARTITION for (20, 'A', '10') with table t_subpart_range_hash_8_exchange VERBOSE;
alter table t_subpart_range_hash_8 EXCHANGE SUBPARTITION for (19, 'BBB', '10') with table t_subpart_range_hash_8_exchange WITH VALIDATION VERBOSE;

alter table t_subpart_list_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_list_hash_8_exchange WITHOUT VALIDATION;
alter table t_subpart_list_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_list_hash_8_exchange;
alter table t_subpart_list_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_list_hash_8_exchange WITH VALIDATION;
alter table t_subpart_list_hash_8 EXCHANGE SUBPARTITION for (20, '20') with table t_subpart_list_hash_8_exchange VERBOSE;
alter table t_subpart_list_hash_8 EXCHANGE SUBPARTITION for (20, '20') with table t_subpart_list_hash_8_exchange WITH VALIDATION VERBOSE;

alter table t_subpart_hash_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_hash_hash_8_exchange WITHOUT VALIDATION;
alter table t_subpart_hash_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_hash_hash_8_exchange;
alter table t_subpart_hash_hash_8 EXCHANGE SUBPARTITION p1_subpartdefault1 with table t_subpart_hash_hash_8_exchange WITH VALIDATION;
alter table t_subpart_hash_hash_8 EXCHANGE SUBPARTITION for (10, '20') with table t_subpart_hash_hash_8_exchange VERBOSE;
alter table t_subpart_hash_hash_8 EXCHANGE SUBPARTITION for (10, '20') with table t_subpart_hash_hash_8_exchange WITH VALIDATION VERBOSE;


drop table t_subpart_range_hash_8_exchange;
drop table t_subpart_list_hash_8_exchange;
drop table t_subpart_hash_hash_8_exchange;



-- TODO List partition MODIFY ADD/DROP VALUES (...)
-- alter table xxx modify partition p1 add values ();
-- alter table xxx modify partition p1 drop values ();
-- alter table xxx modify subpartition p1 add values ();
-- alter table xxx modify subpartition p1 addropd values ();



----------------------------
-- TODO MOVE [SUB]PARTITION [FOR]
----------------------------
alter table t_subpart_range_hash_10 move partition p1 tablespace ts_subpart_hash_1;
alter table t_subpart_range_hash_10 move partition for (10, 'MAXVALUE') tablespace ts_subpart_hash_1;
alter table t_subpart_range_hash_10 move subpartition sp2 tablespace ts_subpart_hash_2;
alter table t_subpart_range_hash_10 move subpartition for (10, 'MAXVALUE', '1') tablespace ts_subpart_hash_2;

alter table t_subpart_list_hash_10 move partition p1 tablespace ts_subpart_hash_1;
alter table t_subpart_list_hash_10 move partition for (100) tablespace ts_subpart_hash_1;
alter table t_subpart_list_hash_10 move subpartition sp2 tablespace ts_subpart_hash_2;
alter table t_subpart_list_hash_10 move subpartition for (100, '1') tablespace ts_subpart_hash_2;

alter table t_subpart_hash_hash_10 move partition p1 tablespace ts_subpart_hash_1;
alter table t_subpart_hash_hash_10 move partition for (1) tablespace ts_subpart_hash_1;
alter table t_subpart_hash_hash_10 move subpartition sp2 tablespace ts_subpart_hash_2;
alter table t_subpart_hash_hash_10 move subpartition for (1, '1') tablespace ts_subpart_hash_2;



----------------------------
-- TODO ROW MOVEMENT
----------------------------
alter table t_subpart_range_hash_10 enable row movement;
alter table t_subpart_range_hash_10 disable row movement;

alter table t_subpart_list_hash_10 enable row movement;
alter table t_subpart_list_hash_10 disable row movement;

alter table t_subpart_hash_hash_10 enable row movement;
alter table t_subpart_hash_hash_10 disable row movement;



----------------------------
-- ALTER INDEX ... UNUSABLE
-- ALTER INDEX ... REBUILD
-- ALTER INDEX ... MODIFY [SUB]PARTITION name UNUSABLE
-- ALTER INDEX ... REBUILD [SUB]PARTITION name
----------------------------
alter index i_t_subpart_hash_hash_10_3 UNUSABLE;

alter index i_t_subpart_hash_hash_10_3 REBUILD partition subp1_bd_idx_local;
alter index i_t_subpart_hash_hash_10_3 REBUILD subpartition subp3_bd_idx_local;

alter index i_t_subpart_hash_hash_10_4 UNUSABLE;
alter index i_t_subpart_hash_hash_10_4 REBUILD partition subp1_index_local; -- error
alter index i_t_subpart_hash_hash_10_4 REBUILD subpartition subp3_index_local; -- error

select relname, parttype, indisusable from pg_partition where parentid='i_t_subpart_hash_hash_10_3'::regclass order by relname;
select relname, relkind, parttype, indisusable from pg_class left join pg_index on pg_class.oid=indexrelid where pg_class.oid in ('i_t_subpart_hash_hash_10_3'::regclass, 'i_t_subpart_hash_hash_10_4'::regclass) order by relname;

explain (costs off)
select * from t_subpart_hash_hash_10 where bd = '2999-01-01';

-- alter index i_t_subpart_hash_hash_10_3 REBUILD;
-- alter index i_t_subpart_hash_hash_10_4 REBUILD;

explain (costs off)
select * from t_subpart_hash_hash_10 where bd = '2999-01-01';

alter index i_t_subpart_hash_hash_10_3 modify partition subp1_bd_idx_local unusable;
alter index i_t_subpart_hash_hash_10_3 modify subpartition subp3_bd_idx_local unusable;

explain (costs off)
select * from t_subpart_hash_hash_10 where bd = '2999-01-01';

select relname, parttype, indisusable from pg_partition where parentid='i_t_subpart_hash_hash_10_3'::regclass order by relname;
select relname, relkind, parttype, indisusable from pg_class left join pg_index on pg_class.oid=indexrelid where pg_class.oid in ('i_t_subpart_hash_hash_10_3'::regclass, 'i_t_subpart_hash_hash_10_4'::regclass) order by relname;



----------------------------
-- ALTER TABLE ... MODIFY [SUB]PARTITION [FOR] ... [REBUILD] UNUSABLE LOCAL INDEXES
----------------------------
alter table t_subpart_hash_hash_10 modify partition p1 unusable local indexes;
alter table t_subpart_hash_hash_10 modify partition for (3) unusable local indexes;
select relname, parttype, indisusable from pg_partition where parentid='i_t_subpart_hash_hash_10_3'::regclass order by relname;

explain (costs off)
select * from t_subpart_hash_hash_10 where bd = '2999-01-01';

alter table t_subpart_hash_hash_10 modify partition p1 REBUILD unusable local indexes;
alter table t_subpart_hash_hash_10 modify partition for (3) REBUILD unusable local indexes;
select relname, parttype, indisusable from pg_partition where parentid='i_t_subpart_hash_hash_10_3'::regclass order by relname;

explain (costs off)
select * from t_subpart_hash_hash_10 where bd = '2999-01-01';

alter table t_subpart_hash_hash_10 modify subpartition sp1 unusable local indexes;
alter table t_subpart_hash_hash_10 modify subpartition for (3, NULL) unusable local indexes;
select relname, parttype, indisusable from pg_partition where parentid='i_t_subpart_hash_hash_10_3'::regclass order by relname;

explain (costs off)
select * from t_subpart_hash_hash_10 where bd = '2999-01-01';

alter table t_subpart_hash_hash_10 modify subpartition sp1 REBUILD unusable local indexes;
alter table t_subpart_hash_hash_10 modify subpartition for (3, NULL) REBUILD unusable local indexes;
select relname, parttype, indisusable from pg_partition where parentid='i_t_subpart_hash_hash_10_3'::regclass order by relname;

explain (costs off)
select * from t_subpart_hash_hash_10 where bd = '2999-01-01';



----------------------------
-- TODO RENAME
----------------------------
alter table t_subpart_hash_hash_10 rename partition p2 to p0;
alter table t_subpart_hash_hash_10 rename partition p0 to p2;

alter table t_subpart_hash_hash_10 rename subpartition sp2 to sp0;
alter table t_subpart_hash_hash_10 rename subpartition sp0 to sp2;




select table_name,partitioning_type,subpartitioning_type,partition_count,
def_subpartition_count,partitioning_key_count,subpartitioning_key_count
from all_part_tables where lower(table_name) in ('t_subpart_range_hash_11', 't_subpart_list_hash_11', 't_subpart_hash_hash_11') order by table_name;

select (table_owner is not null) as has_owner,table_name,partition_name,subpartition_name from all_tab_subpartitions where lower(table_name) in ('t_subpart_range_hash_11', 't_subpart_list_hash_11', 't_subpart_hash_hash_11') order by table_name,partition_name,subpartition_name;

select (table_owner is not null) as has_owner,table_name,partition_name,subpartition_count from all_tab_partitions where lower(table_name) in ('t_subpart_range_hash_11', 't_subpart_list_hash_11', 't_subpart_hash_hash_11') order by table_name,partition_name;




CREATE TABLE t_subpart_cstore_hh (id integer, name varchar(30), db date)
with ( orientation = column )
partition by hash(id)
subpartition by hash(db)
(
partition p1
);



----------------------------
-- ERROR
----------------------------

create table t_subpart_error (id integer, name varchar(30))
partition by VALUES(id)
subpartition by hash(id);

create table t_subpart_error (id integer, name varchar(30))
partition by hash(id)
subpartition by VALUES(name)
(
partition p1
);

create table t_subpart_interval (id integer, name varchar(30), db date)
partition by range(db)
INTERVAL ('1 day')
subpartition by hash(id)
(
partition p1 values less than ('2000-01-01')
);

create table t_subpart_error (id integer, name varchar(30), db date)
partition by hash(id)
subpartition by range(db)
INTERVAL ('1 day')
(
partition p1
);




select oid,relname from pg_class
where (relkind = 'r' and parttype != 'n' and oid not in (select distinct parentid from pg_partition where parttype='r'))
    or (relkind = 'i' and parttype != 'n' and oid not in (select distinct parentid from pg_partition where parttype='x'));

select p1.relname, p1.parttype, p1.parentid, p1.boundaries
from pg_partition p1
where (p1.parttype = 'r' and p1.parentid not in (select oid from pg_class where relkind = 'r' and parttype != 'n')) 
    or (p1.parttype = 'r' and not exists (select oid from pg_partition where parttype='p' and parentid=p1.parentid)) 
    or (p1.parttype = 'p' and not exists (select oid from pg_partition where parttype='r' and parentid=p1.parentid)) 
    or (p1.parttype = 'p' and exists (select oid from pg_class where parttype='s' and oid=p1.parentid) and not exists (select oid from pg_partition where parttype='s' and parentid=p1.oid)) 
    or (p1.parttype = 's' and not exists (select oid from pg_partition where parttype='p' and oid=p1.parentid)) 
    or (p1.parttype = 'x' and p1.parentid not in (select oid from pg_class where relkind = 'i' and parttype != 'n')) 
    or (p1.indextblid != 0 and p1.indextblid not in (select oid from pg_partition where parttype != 'r')); 

drop index i_t_subpart_hash_hash_10_3, i_t_subpart_hash_hash_10_4;


-- drop table t_subpart_normal_table_hash, t_subpart_part_table_hash;
-- drop schema schema_vastbase_subpartition_hash cascade;
-- drop tablespace ts_subpart_hash_1;
-- drop tablespace ts_subpart_hash_2;
-- drop tablespace ts_subpart_hash_test_user;
-- drop user user_subpart_hash;
