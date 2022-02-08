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
