select oid, * from pg_proc where proname in ('first', 'first_transition', 'last', 'last_transition') order by oid;

create table first_last_test(a int, b int, c int, d int);
select first(a ORDER BY a), last(a ORDER BY a) from first_last_test;

insert into first_last_test values
(1,    1,    1,    NULL),
(1,    2,    3,    NULL),
(1,    4,    2,    NULL),
(2,    1,    5,    NULL),
(2,    3,    NULL, NULL),
(3,    1,    4,    NULL),
(3,    NULL, 2,    NULL),
(4,    4,    5,    NULL),
(4,    3,    1,    NULL),
(4,    NULL, NULL, NULL),
(5,    4,    3,    NULL),
(5,    5,    4,    NULL);

select first(a ORDER BY a), last(a ORDER BY a) from first_last_test;
select a, first(b ORDER BY c), last(b ORDER BY c) from first_last_test GROUP BY a ORDER BY a;
select a, first(b ORDER BY c DESC), last(b ORDER BY c DESC) from first_last_test GROUP BY a ORDER BY a;
select a, first(b ORDER BY c), last(b ORDER BY c) from first_last_test GROUP BY a having first(b ORDER BY c) <= 3 ORDER BY a;
select a, first(b ORDER BY c NULLS FIRST), last(b ORDER BY c NULLS FIRST) from first_last_test GROUP BY a ORDER BY a;
select a, first(b ORDER BY c NULLS LAST), last(b ORDER BY c NULLS LAST) from first_last_test GROUP BY a ORDER BY a;
select a, first(b ORDER BY c DESC NULLS LAST), last(b ORDER BY c DESC NULLS LAST) from first_last_test GROUP BY a ORDER BY a;
select a, first(b ORDER BY d), last(b ORDER BY d) from first_last_test GROUP BY a ORDER BY a;
select a, first(d ORDER BY b), last(d ORDER BY c) from first_last_test GROUP BY a ORDER BY a;

insert into first_last_test values
(1,    1,    1,    1);
select a, first(d ORDER BY b), last(d ORDER BY c) from first_last_test GROUP BY a ORDER BY a;

drop table first_last_test;

