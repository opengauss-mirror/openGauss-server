--
-- insert with DEFAULT in the target_list
--
create table inserttest (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT, DEFAULT);
insert into inserttest (col2, col3) values (3, DEFAULT);
insert into inserttest (col1, col2, col3) values (DEFAULT, 5, DEFAULT);
insert into inserttest values (DEFAULT, 5, 'test');
insert into inserttest values (DEFAULT, 7);

select * from inserttest;

--
-- insert with similar expression / target_list values (all fail)
--
insert into inserttest (col1, col2, col3) values (DEFAULT, DEFAULT);
insert into inserttest (col1, col2, col3) values (1, 2);
insert into inserttest (col1) values (1, 2);
insert into inserttest (col1) values (DEFAULT, DEFAULT);

select * from inserttest;

--
-- VALUES test
--
insert into inserttest values(10, 20, '40'), (-1, 2, DEFAULT),
    ((select 2), (select i from (values(3)) as foo (i)), 'values are fun!');

select * from inserttest order by 1,2,3;

--
-- TOASTed value test
--
insert into inserttest values(30, 50, repeat('x', 10000));

select col1, col2, char_length(col3) from inserttest order by 1,2,3;

drop table inserttest;

--
---- I tried to use generate_series() function to load a bunch of test data in a
---- table. It fails with the following error:
---- set-valued function called in context that cannot accept a set
--
-- distribute by hash
create table test_hash(a int, b char(10)); 
insert into test_hash values (generate_series(1,1000), 'foo');
drop table test_hash;

-- distribute by modulo
create table test_modulo(a int, b char(10)); 
insert into test_modulo values (generate_series(1,1000), 'foo');
drop table test_modulo;

-- distribute by replication
create table test_replication(a int, b char(10));
insert into test_replication values (generate_series(1,1000), 'foo');
drop table test_replication;

-- distribute by roundrobin
create table test_roundrobin(a int, b char(10));
insert into test_roundrobin values (generate_series(1,1000), 'foo');
drop table test_roundrobin;

--insert into pg_auth_history
insert into pg_auth_history values(10, '2015-110-10 08:00:00.57603+08', 'sha256232f8630ce6af1095f6db3ed4c05a48747038936d42176e1103594d43c7d1adc4aca54361a23e51c6cd9371ccc95776450219376e45bcca01e27a7f06bf8088a8b1a9e280cdcc315c8134879818442bc3e92064a70e27b2ea83fcf6990a607d0');

-- insert upsert
drop table if exists t_grammer;
create table t_grammer(c1 INT PRIMARY KEY, c2 int, C3 INT[3]);
insert into t_grammer values(12,2) on duplicate key update c2=2,c3[1]=3;
select * from t_grammer;