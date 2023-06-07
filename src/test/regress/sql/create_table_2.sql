-- b compatibility case
drop database if exists b;
create database b dbcompatibility 'b';

\c b
-- test primary key in M mode
-- test [index_type]
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11, f12));
\d+ test_primary
drop table test_primary;
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using hash(f11, f12));
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using hash(f11, f12)) with (orientation = column);

-- test [CONSTRAINT [constraint_name]]
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key(f11));
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key(f11));
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint primary key(f11));
\d+ test_primary
drop table test_primary;

-- test [ASC|DESC]
create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11 desc, f12 asc));
\d+ test_primary
drop table test_primary;

-- test expression, error
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key ((abs(f11))));
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key ((f11 * 2 + 1)));

-- test foreign key in M mode
-- test [CONSTRAINT [constraint_name]] and [index_name]
create table test_primary(f11 int, f12 varchar(20), f13 bool, primary key (f11));
create table test_foreign(f21 int, f22 timestamp, constraint con_t_foreign foreign key f_t_foreign (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, constraint con_t_foreign foreign key (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, constraint foreign key f_t_foreign (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, foreign key f_t_foreign (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;

create table test_foreign(f21 int, f22 timestamp, foreign key (f21) references test_primary(f11));
\d+ test_foreign
drop table test_foreign;
drop table test_primary;

-- test unique key in M mode
-- test [index_type]
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using btree(f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using hash(f31, f32));
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using hash(f31, f32)) with (orientation = column);

-- test [CONSTRAINT [constraint_name]] and [index_name]
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique u_t_unique(f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), unique u_t_unique(f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique (f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint unique (f31, f32));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), unique (f31, f32));
\d+ test_unique
drop table test_unique;

-- test [ASC|DESC]
create table test_unique(f31 int, f32 varchar(20), unique (f31 desc, f32 asc));
\d+ test_unique
drop table test_unique;

-- test expression
create table test_unique(f31 int, f32 varchar(20), unique ((abs(f31)) desc, (lower(f32)) asc));
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), unique ((f31 * 2 + 1) desc, (lower(f32)) asc));
\d+ test_unique
drop table test_unique;

-- test unreserved_keyword index and key
-- error
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique key using btree(f31));
create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique index using btree(f31));

-- partition table
-- test primary key in M mode
-- test [index_type]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using btree(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using hash(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using hash(f1, f2, f3)
)
with (orientation = column)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

-- test [CONSTRAINT [constraint_name]]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint primary key(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

-- test [ASC|DESC]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using btree(f1 desc, f2 asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_primary
drop table test_p_primary;

-- test expression, error
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key using btree((abs(f1)) desc, (f2 * 2 + 1) asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

-- test foreign key in M mode
-- test [CONSTRAINT [constraint_name]] and [index_name]
CREATE TABLE test_p_primary
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_pri primary key(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_foreign foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_foreign foreign key(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    foreign key f_t_foreign(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;

CREATE TABLE test_p_foreign
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    foreign key(f1) references test_p_primary(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_foreign
drop table test_p_foreign;
drop table test_p_primary;

-- test unique key in M mode
-- test [index_type]
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using btree(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

-- error
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using hash(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique using hash(f1, f2, f3)
)
with (orientation = column)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

-- test [CONSTRAINT [constraint_name]] and [index_name]
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique u_t_unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique u_t_unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique(f1, f2, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

-- test [ASC|DESC]
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique(f1 desc, f2 asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;

-- test expression
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    unique((abs(f1)) desc, (f2 * 2 + 1) asc, f3)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);
\d+ test_p_unique
drop table test_p_unique;


-- test unreserved_keyword index and key
-- error
CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique key using btree(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

CREATE TABLE test_p_unique
(
    f1  INTEGER,
    f2  INTEGER,
    f3  INTEGER,
    constraint con_t_unique unique index using btree(f1)
)
PARTITION BY RANGE(f1)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P3 VALUES LESS THAN(2451544),
        PARTITION P4 VALUES LESS THAN(MAXVALUE)
);

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique using btree(f31, f32) comment 'unique index' using btree);
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique (f31, f32) comment 'unique index' using btree);
\d+ test_unique
drop table test_unique;

create table test_unique(f31 int, f32 varchar(20), constraint con_t_unique unique (f31, f32) comment 'unique index' using btree using btree);
\d+ test_unique
drop table test_unique;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11 desc, f12 asc) comment 'primary key' using btree);
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key (f11 desc, f12 asc) comment 'primary key' using btree);
\d+ test_primary
drop table test_primary;

create table test_primary(f11 int, f12 varchar(20), f13 bool, constraint con_t_pri primary key using btree(f11 desc, f12 asc) comment 'primary key' using btree using btree);
\d+ test_primary
drop table test_primary;
