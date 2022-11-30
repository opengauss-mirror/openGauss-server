set client_min_messages = error;
set search_path=swtest;
SET CLIENT_ENCODING='UTF8';

--accepted cases
explain (costs off)
select * from test_area CONNECT BY LEVEL <= LENGTH('SOME TEXT');
explain (costs off)
select *, LEVEL from test_area CONNECT BY LEVEL <= LENGTH('SOME TEXT');

explain (costs off)
select * from test_area CONNECT BY ROWNUM <= LENGTH('SOME TEXT');
explain (costs off)
select *, ROWNUM from test_area CONNECT BY ROWNUM <= LENGTH('SOME TEXT');

explain (costs off)
select * from test_area CONNECT BY LEVEL < LENGTH('SOME TEXT');
explain (costs off)
select *, LEVEL from test_area CONNECT BY LEVEL < LENGTH('SOME TEXT');

explain (costs off)
select * from test_area CONNECT BY ROWNUM < LENGTH('SOME TEXT');
explain (costs off)
select *, ROWNUM from test_area CONNECT BY ROWNUM < LENGTH('SOME TEXT');

--rejected cases
explain (costs off)
select *, LEVEL from test_area CONNECT BY LEVEL > LENGTH('SOME TEXT');
explain (costs off)
select *, LEVEL from test_area CONNECT BY LEVEL >= LENGTH('SOME TEXT');
explain (costs off)
select * from test_area CONNECT BY APPLE > LENGTH('SOME TEXT');
explain (costs off)
select * from test_area CONNECT BY APPLE < LENGTH('SOME TEXT');
explain (costs off)
select * from test_area CONNECT BY APPLE <= LENGTH('SOME TEXT');

-- materialized view with column name 'level' 'connect_by_isleaf' 'connect_by_iscycle'
create table startwith_t(id int, level int, connect_by_isleaf int, connect_by_iscycle int);
create materialized view startwith_mv as 
    WITH RECURSIVE t(id, level, connect_by_isleaf, connect_by_iscycle) as (select * from startwith_t)
    select t.id, t.connect_by_isleaf as level, t.level as connect_by_isleaf from t;
select pg_get_viewdef('startwith_mv', true);

drop materialized view startwith_mv;
drop table startwith_t;
