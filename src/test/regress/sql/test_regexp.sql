-- create new schema --
drop schema if exists test_regexp;
create schema test_regexp;
set search_path=test_regexp;


create table t1
(
    f_int1         integer default 0 not null,
    f_int2         integer,
    f_int3         integer,
    f_bigint1      bigint,
    f_bigint2      bigint,
    f_bigint3      bigint,
    f_bool1        bool,
    f_bool2        bool,
    f_num1         number(38, 0),
    f_num2         number(38, 0),
    f_dec1         DECIMAL(38, 0),
    f_dec2         DECIMAL(38, 0),
    f_num10        number(38, 10),
    f_dec10        decimal(38, 10),
    f_float        float,
    f_double       double precision,
    f_real         real,
    f_char1        char(128),
    f_char2        char(128),
    f_varchar1     varchar(512),
    f_varchar2     varchar2(512),
    f_date1        date,
    f_date2        date,
    f_time         date,
    f_timestamp    timestamp,
    f_tp_tz        timestamp,
    f_tp_ltz       timestamp,
    f_binary       bytea,
    f_varbinary    bytea,
    f_blob         blob,
    f_clob         clob
);

create index idx_1 on t1(f_int1);

delete from t1;
insert into t1(f_int1, f_varchar1) values (1,'1234560');
insert into t1(f_int1, f_varchar1) values (2,'1234560');
insert into t1(f_int1, f_varchar1) values (3,'1b3b560');
insert into t1(f_int1, f_varchar1) values (4,'abc');
insert into t1(f_int1, f_varchar1) values (5,'abcde');
insert into t1(f_int1, f_varchar1) values (6,'ADREasx');
insert into t1(f_int1, f_varchar1) values (7,'123 45');
insert into t1(f_int1, f_varchar1) values (8,'adc de');
insert into t1(f_int1, f_varchar1) values (9,'adc,.de');
insert into t1(f_int1, f_varchar1) values (10,'1B');
insert into t1(f_int1, f_varchar1) values (11,'abcbvbnb');
insert into t1(f_int1, f_varchar1) values (12,'11114560');
insert into t1(f_int1, f_varchar1) values (13,'11124560');
insert into t1(f_int1, f_varchar1) values(14, 'abc'||chr(10)||'DEF'||chr(10)||'hij');
insert into t1(f_int1, f_varchar1) values(15, '1abc2abc3abc4abc5abc6abc7abc8abc9abc9abcAabcBabc');
insert into t1(f_int1, f_varchar1) values(16, '1abc2abc3abc4abc5abc6abc7abc8abc9abcAabcAabcBabc');
insert into t1(f_int1, f_varchar1) values(17, 'oltp100');
insert into t1(f_int1, f_varchar1) values(18, 'oltp 100');
insert into t1(f_int1, f_char1, f_varchar2) values(19,'Fluffy','Fluffy');
insert into t1(f_int1, f_char1, f_varchar2) values(20,'Buffy','Buffy');
insert into t1(f_int1, f_char1, f_varchar2) values(21,'fluffy','fluffy');
insert into t1(f_int1, f_char1, f_varchar2) values(22,'buffy','buffy');
insert into t1(f_int1, f_char1, f_varchar2) values(23,'桂林山水abc高山流水','桂林山水abc高山流水');
insert into t1(f_int1, f_char1, f_varchar2) values(24,'aa abc zzzz','aa abc zzzz');
insert into t1(f_int1, f_char1, f_varchar2) values(25,'我的的的的 abcabcabcabcabcabcabcabcabcabcabcabc','我的的的的 abcabcabcabcabcabcabcabcabcabcabcabc');
insert into t1(f_int1, f_char1, f_varchar2) values(26,'abcbvbnb
efgh
ijjkkkkkkk','abcbvbnb
efgh
ijjkkkkkkk123');
insert into t1(f_int1, f_char1, f_varchar2) values(27,'abc efg','hgj khln');
insert into t1(f_int1, f_char1, f_varchar2) values(28,'abc\efg','hgj(khln');
insert into t1(f_int1, f_char1, f_varchar2) values(29,'*+?|^${}.','*+?|^${}.');



select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'1....60');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'1[0-9]{4}60');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'1[[:digit:]]{4}60');
select f_int1,f_varchar1 from t1 where not regexp_like(f_varchar1,'^[[:digit:]]+$');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'^[^[:digit:]]+$');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'^1[2B]');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'[[:space:]]');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'^([a-z]+|[0-9]+)$');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'[[:punct:]]');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'^DEF$');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'^1[2b]','ic');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'^1[2b]','ci');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'a');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'(1abc)(2abc)(3abc)(4abc)(5abc)(6abc)(7abc)(8abc)(9abc)\9(Aabc)(Babc)');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'(1abc)(2abc)(3abc)(4abc)(5abc)(6abc)(7abc)(8abc)(9abc)(Aabc)\a(Babc)');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'oltp 100');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'oltp100');
select f_int1,f_varchar1 from t1 where regexp_like(f_varchar1,'ffy*') order by 1;

-- clean
drop index idx_1;
drop table t1;

drop schema if exists test_regexp cascade;

