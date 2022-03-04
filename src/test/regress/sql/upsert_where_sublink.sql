----
-- setup
----
CREATE SCHEMA schema_upsert_where_sublink;
SET search_path = schema_upsert_where_sublink;

create table tab_target(
c1 int unique not null,
c2 bigint default 0,
c3 numeric default 0,
c4 varchar(100) default 'abcdefjfieE##$#KFAEOJop13SEFJeo',
primary key(c2,c3));

INSERT INTO tab_target (c1, c2, c3) VALUES(
    generate_series(1,10),
    generate_series(1,10),
    generate_series(1,10));

CREATE TABLE tab_source(c1 int, c2 int, c3 int, c4 int);
INSERT INTO tab_source VALUES(generate_series(1,10), generate_series(10,1, -1), generate_series(1,10), generate_series(1,10));

---------------------------------------
-- not corelated sublink
---------------------------------------
begin;
-- in/not in sublink
-- multi confliction -> primary key first
insert into tab_target values(1,1,1) on duplicate key update c4 = 'conflict1' where excluded.c1 in (select 1);

insert into tab_target values(1,1,1) on duplicate key update c4 = 'ERROR' where excluded.c1 not in (select 1);

insert into tab_target values(1,2,2) on duplicate key update c4 = 'conflict2' where excluded.c3 in (select c1 from tab_source);

insert into tab_target values(1,2,2) on duplicate key update c4 = 'ERROR' where excluded.c3 not in (select c1 from tab_source);

insert into tab_target values(0,3,3) on duplicate key update c4 = 'conflict3' where excluded.c1 not in (select 1);

insert into tab_target values(0,3,3) on duplicate key update c4 = 'ERROR' where excluded.c1 in (select 1);

-- (not) exists sublink
insert into tab_target values(4,1,2) on duplicate key update c4 = 'conflict4' where exists (select c1 from tab_source where c4 = 4);

insert into tab_target values(4,1,2) on duplicate key update c4 = 'ERROR' where not exists (select c1 from tab_source where c4 = 4);

insert into tab_target values(0,5,5) on duplicate key update c4 = 'conflict5' where not exists (select c2 from tab_source where c4 = 4 and c1 = 1);

insert into tab_target values(0,5,5) on duplicate key update c4 = 'ERROR' where exists (select c2 from tab_source where c4 = 4 and c1 = 1);

-- any/some
insert into tab_target values(6,0,0) on duplicate key update c4 = 'conflict6' where excluded.c3 = any (select 0);

insert into tab_target values(6,0,0) on duplicate key update c4 = 'ERROR' where excluded.c3 != any (select 0);

insert into tab_target values(7,0,0) on duplicate key update c4 = 'conflict7' where excluded.c3 > some (select -1);

insert into tab_target values(7,0,0) on duplicate key update c4 = 'ERROR' where excluded.c3 < some (select -1);

-- opr sublink
insert into tab_target values(8,8,8) on duplicate key update c4 = 'conflict8' where not (excluded.c3 > (select c2 from tab_source where c3 < 8 limit 1));

insert into tab_target values(8,8,8) on duplicate key update c4 = 'ERROR' where (excluded.c3 > (select c2 from tab_source where c3 < 8 limit 1));

-- nested sublink
insert into tab_target values(9,9,9) on duplicate key update c4 = 'conflict9' where excluded.c1 = (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = 9
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

insert into tab_target values(9,9,9) on duplicate key update c4 = 'ERROR' where excluded.c1 != (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = 9
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

-- sublink with CTE
insert into tab_target values(10,10,10) on duplicate key update c4 = 'conflict10' where c1 = (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = 10
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

insert into tab_target values(10,10,10) on duplicate key update c4 = 'ERROR' where c1 != (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = 10
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

select * from tab_target order by 1,2,3,4;

rollback;

-- check plan
begin;
-- in/not in sublink
-- multi confliction -> primary key first
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,1,1) on duplicate key update c4 = 'conflict1' where excluded.c1 in (select 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,1,1) on duplicate key update c4 = 'ERROR' where excluded.c1 not in (select 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,2,2) on duplicate key update c4 = 'conflict2' where excluded.c3 in (select c1 from tab_source);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,2,2) on duplicate key update c4 = 'ERROR' where excluded.c3 not in (select c1 from tab_source);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,3,3) on duplicate key update c4 = 'conflict3' where excluded.c1 not in (select 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,3,3) on duplicate key update c4 = 'ERROR' where excluded.c1 in (select 1);

-- (not) exists sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(4,1,2) on duplicate key update c4 = 'conflict4' where exists (select c1 from tab_source where c4 = 4);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(4,1,2) on duplicate key update c4 = 'ERROR' where not exists (select c1 from tab_source where c4 = 4);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,5,5) on duplicate key update c4 = 'conflict5' where not exists (select c2 from tab_source where c4 = 4 and c1 = 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,5,5) on duplicate key update c4 = 'ERROR' where exists (select c2 from tab_source where c4 = 4 and c1 = 1);

-- any/some
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,0,0) on duplicate key update c4 = 'conflict6' where excluded.c3 = any (select 0);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,0,0) on duplicate key update c4 = 'ERROR' where excluded.c3 != any (select 0);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(7,0,0) on duplicate key update c4 = 'conflict7' where excluded.c3 > some (select -1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(7,0,0) on duplicate key update c4 = 'ERROR' where excluded.c3 < some (select -1);

-- opr sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(8,8,8) on duplicate key update c4 = 'conflict8' where not (excluded.c3 > (select c2 from tab_source where c3 < 8 limit 1));

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(8,8,8) on duplicate key update c4 = 'ERROR' where (excluded.c3 > (select c2 from tab_source where c3 < 8 limit 1));

-- nested sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(9,9,9) on duplicate key update c4 = 'conflict9' where excluded.c1 = (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = 9
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(9,9,9) on duplicate key update c4 = 'ERROR' where excluded.c1 != (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = 9
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

-- sublink with CTE
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(10,10,10) on duplicate key update c4 = 'conflict10' where c1 = (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = 10
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(10,10,10) on duplicate key update c4 = 'ERROR' where c1 != (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = 10
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

select * from tab_target order by 1,2,3,4;

rollback;

---------------------------------------
-- corelated sublink - reference target
---------------------------------------
begin;
-- in/not in sublink
-- multi confliction -> primary key first
insert into tab_target values(1,1,1) on duplicate key update c4 = 'conflict1' where excluded.c1 in (select 1 where tab_target.c1 = 1);

insert into tab_target values(1,1,1) on duplicate key update c4 = 'ERROR' where excluded.c1 not in (select 1 where tab_target.c1 = 1);

insert into tab_target values(1,2,2) on duplicate key update c4 = 'conflict2' where excluded.c3 in (select c1 from tab_source where tab_target.c1 = c1);

insert into tab_target values(1,2,2) on duplicate key update c4 = 'ERROR' where excluded.c3 not in (select c1 from tab_source where tab_target.c1 = c1);

insert into tab_target values(0,3,3) on duplicate key update c4 = 'conflict3' where excluded.c1 not in (select 1 where tab_target.c1 = 1);

insert into tab_target values(0,3,3) on duplicate key update c4 = 'ERROR' where excluded.c1 in (select 1 where tab_target.c1 = 1);

-- (not) exists sublink
insert into tab_target values(4,1,2) on duplicate key update c4 = 'conflict4' where exists (select c1 from tab_source where c4 = 4 and c3 = tab_target.c3);

insert into tab_target values(4,1,2) on duplicate key update c4 = 'ERROR' where not exists (select c1 from tab_source where c4 = 4 and c3 = tab_target.c3);

insert into tab_target values(0,5,5) on duplicate key update c4 = 'conflict5' where not exists (select c2 from tab_source where c4 = 4 and c3 = tab_target.c3);

insert into tab_target values(0,5,5) on duplicate key update c4 = 'ERROR' where exists (select c2 from tab_source where c4 = 4 and c3 = tab_target.c3);

-- any/some
insert into tab_target values(6,0,6) on duplicate key update c4 = 'conflict6' where excluded.c3 = any (select c3 from tab_source where tab_target.c1 = c1);

insert into tab_target values(6,0,6) on duplicate key update c4 = 'ERROR' where excluded.c3 != any (select c3 from tab_source where tab_target.c1 = c1);

insert into tab_target values(7,0,7) on duplicate key update c4 = 'conflict7' where excluded.c3 > some (select -1 from tab_source where tab_target.c1 = c1);

insert into tab_target values(7,0,7) on duplicate key update c4 = 'ERROR' where excluded.c3 < some (select -1 from tab_source where tab_target.c1 = c1);

-- opr sublink
insert into tab_target values(8,8,8) on duplicate key update c4 = 'conflict8' where not (excluded.c3 > (select c2 from tab_source where c3 < tab_target.c1 limit 1));

insert into tab_target values(8,8,8) on duplicate key update c4 = 'ERROR' where (excluded.c3 > (select c2 from tab_source where c3 < tab_target.c1 limit 1));

-- nested sublink
insert into tab_target values(9,9,9) on duplicate key update c4 = 'conflict9' where excluded.c1 = (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = tab_target.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

insert into tab_target values(9,9,9) on duplicate key update c4 = 'ERROR' where tab_target.c1 != (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = excluded.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

-- sublink with CTE
insert into tab_target values(10,10,10) on duplicate key update c4 = 'conflict10' where c1 = (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = tab_target.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

insert into tab_target values(10,10,10) on duplicate key update c4 = 'ERROR' where c1 != (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = tab_target.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

select * from tab_target order by 1,2,3,4;

rollback;

-- check plan
begin;
-- in/not in sublink
-- multi confliction -> primary key first
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,1,1) on duplicate key update c4 = 'conflict1' where excluded.c1 in (select 1 where tab_target.c1 = 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,1,1) on duplicate key update c4 = 'ERROR' where excluded.c1 not in (select 1 where tab_target.c1 = 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,2,2) on duplicate key update c4 = 'conflict2' where excluded.c3 in (select c1 from tab_source where tab_target.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,2,2) on duplicate key update c4 = 'ERROR' where excluded.c3 not in (select c1 from tab_source where tab_target.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,3,3) on duplicate key update c4 = 'conflict3' where excluded.c1 not in (select 1 where tab_target.c1 = 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,3,3) on duplicate key update c4 = 'ERROR' where excluded.c1 in (select 1 where tab_target.c1 = 1);

-- (not) exists sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(4,1,2) on duplicate key update c4 = 'conflict4' where exists (select c1 from tab_source where c4 = 4 and c3 = tab_target.c3);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(4,1,2) on duplicate key update c4 = 'ERROR' where not exists (select c1 from tab_source where c4 = 4 and c3 = tab_target.c3);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,5,5) on duplicate key update c4 = 'conflict5' where not exists (select c2 from tab_source where c4 = 4 and c3 = tab_target.c3);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,5,5) on duplicate key update c4 = 'ERROR' where exists (select c2 from tab_source where c4 = 4 and c3 = tab_target.c3);

-- any/some
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,0,6) on duplicate key update c4 = 'conflict6' where excluded.c3 = any (select c3 from tab_source where tab_target.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,0,6) on duplicate key update c4 = 'ERROR' where excluded.c3 != any (select c3 from tab_source where tab_target.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(7,0,7) on duplicate key update c4 = 'conflict7' where excluded.c3 > some (select -1 from tab_source where tab_target.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(7,0,7) on duplicate key update c4 = 'ERROR' where excluded.c3 < some (select -1 from tab_source where tab_target.c1 = c1);

-- opr sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(8,8,8) on duplicate key update c4 = 'conflict8' where not (excluded.c3 > (select c2 from tab_source where c3 < tab_target.c1 limit 1));

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(8,8,8) on duplicate key update c4 = 'ERROR' where (excluded.c3 > (select c2 from tab_source where c3 < tab_target.c1 limit 1));

-- nested sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(9,9,9) on duplicate key update c4 = 'conflict9' where excluded.c1 = (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = tab_target.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(9,9,9) on duplicate key update c4 = 'ERROR' where tab_target.c1 != (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = excluded.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

-- sublink with CTE
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(10,10,10) on duplicate key update c4 = 'conflict10' where c1 = (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = tab_target.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(10,10,10) on duplicate key update c4 = 'ERROR' where c1 != (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = tab_target.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

select * from tab_target order by 1,2,3,4;

rollback;

-----------------------------------------
-- corelated sublink - reference conflict
-----------------------------------------
begin;
-- in/not in sublink
-- multi confliction -> primary key first
insert into tab_target values(1,1,1) on duplicate key update c4 = 'conflict1' where excluded.c1 in (select 1 where excluded.c1 = 1);

insert into tab_target values(1,1,1) on duplicate key update c4 = 'ERROR' where excluded.c1 not in (select 1 where excluded.c1 = 1);

insert into tab_target values(1,2,2) on duplicate key update c4 = 'conflict2' where excluded.c3 in (select c1 + 1 from tab_source where excluded.c1 = c1);

insert into tab_target values(1,2,2) on duplicate key update c4 = 'ERROR' where excluded.c3 not in (select c1 + 1 from tab_source where excluded.c1 = c1);

insert into tab_target values(0,3,3) on duplicate key update c4 = 'conflict3' where excluded.c1 not in (select 1 where excluded.c1 = 1);

insert into tab_target values(0,3,3) on duplicate key update c4 = 'ERROR' where excluded.c1 in (select 1 where excluded.c1 = 1);

-- (not) exists sublink
insert into tab_target values(4,1,4) on duplicate key update c4 = 'conflict4' where exists (select c1 from tab_source where c4 = 4 and c3 = excluded.c3);

insert into tab_target values(4,1,4) on duplicate key update c4 = 'ERROR' where not exists (select c1 from tab_source where c4 = 4 and c3 = excluded.c3);

insert into tab_target values(0,5,5) on duplicate key update c4 = 'conflict5' where not exists (select c2 from tab_source where c4 = 4 and c3 = excluded.c3);

insert into tab_target values(0,5,5) on duplicate key update c4 = 'ERROR' where exists (select c2 from tab_source where c4 = 4 and c3 = excluded.c3);

-- any/some
insert into tab_target values(6,0,6) on duplicate key update c4 = 'conflict6' where excluded.c3 = any (select c3 from tab_source where excluded.c1 = c1);

insert into tab_target values(6,0,6) on duplicate key update c4 = 'ERROR' where excluded.c3 != any (select c3 from tab_source where excluded.c1 = c1);

insert into tab_target values(7,0,7) on duplicate key update c4 = 'conflict7' where excluded.c3 > some (select -1 from tab_source where excluded.c1 = c1);

insert into tab_target values(7,0,7) on duplicate key update c4 = 'ERROR' where excluded.c3 < some (select -1 from tab_source where excluded.c1 = c1);

-- opr sublink
insert into tab_target values(8,8,8) on duplicate key update c4 = 'conflict8' where not (excluded.c3 > (select c2 from tab_source where c3 < excluded.c1 limit 1));

insert into tab_target values(8,8,8) on duplicate key update c4 = 'ERROR' where (excluded.c3 > (select c2 from tab_source where c3 < excluded.c1 limit 1));

-- nested sublink
insert into tab_target values(9,9,9) on duplicate key update c4 = 'conflict9' where excluded.c1 = (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = excluded.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

insert into tab_target values(9,9,9) on duplicate key update c4 = 'ERROR' where excluded.c1 != (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = excluded.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

-- sublink with CTE
insert into tab_target values(10,10,10) on duplicate key update c4 = 'conflict10' where c1 = (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = excluded.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

insert into tab_target values(10,10,10) on duplicate key update c4 = 'ERROR' where c1 != (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = excluded.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

select * from tab_target order by 1,2,3,4;

rollback;

-- check plan
begin;
-- in/not in sublink
-- multi confliction -> primary key first
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,1,1) on duplicate key update c4 = 'conflict1' where excluded.c1 in (select 1 where excluded.c1 = 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,1,1) on duplicate key update c4 = 'ERROR' where excluded.c1 not in (select 1 where excluded.c1 = 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,2,2) on duplicate key update c4 = 'conflict2' where excluded.c3 in (select c1 + 1 from tab_source where excluded.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,2,2) on duplicate key update c4 = 'ERROR' where excluded.c3 not in (select c1 + 1 from tab_source where excluded.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,3,3) on duplicate key update c4 = 'conflict3' where excluded.c1 not in (select 1 where excluded.c1 = 1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,3,3) on duplicate key update c4 = 'ERROR' where excluded.c1 in (select 1 where excluded.c1 = 1);

-- (not) exists sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(4,1,4) on duplicate key update c4 = 'conflict4' where exists (select c1 from tab_source where c4 = 4 and c3 = excluded.c3);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(4,1,4) on duplicate key update c4 = 'ERROR' where not exists (select c1 from tab_source where c4 = 4 and c3 = excluded.c3);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,5,5) on duplicate key update c4 = 'conflict5' where not exists (select c2 from tab_source where c4 = 4 and c3 = excluded.c3);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(0,5,5) on duplicate key update c4 = 'ERROR' where exists (select c2 from tab_source where c4 = 4 and c3 = excluded.c3);

-- any/some
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,0,6) on duplicate key update c4 = 'conflict6' where excluded.c3 = any (select c3 from tab_source where excluded.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,0,6) on duplicate key update c4 = 'ERROR' where excluded.c3 != any (select c3 from tab_source where excluded.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(7,0,7) on duplicate key update c4 = 'conflict7' where excluded.c3 > some (select -1 from tab_source where excluded.c1 = c1);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(7,0,7) on duplicate key update c4 = 'ERROR' where excluded.c3 < some (select -1 from tab_source where excluded.c1 = c1);

-- opr sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(8,8,8) on duplicate key update c4 = 'conflict8' where not (excluded.c3 > (select c2 from tab_source where c3 < excluded.c1 limit 1));

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(8,8,8) on duplicate key update c4 = 'ERROR' where (excluded.c3 > (select c2 from tab_source where c3 < excluded.c1 limit 1));

-- nested sublink
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(9,9,9) on duplicate key update c4 = 'conflict9' where excluded.c1 = (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = excluded.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(9,9,9) on duplicate key update c4 = 'ERROR' where excluded.c1 != (
    select c1 from tab_source where c1 = (
        select c1 from tab_source where c1 = (
            select c1 from tab_source where c1 = (
                select c1 from tab_source where c1 = (
                    select c1 from tab_source where c1 = (
                        select c1 from tab_source where c1 = (
                            select c1 from (
                                select c1 from (
                                    select c1 from (
                                        select c1 from tab_source where c1 = excluded.c1
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
);

-- sublink with CTE
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(10,10,10) on duplicate key update c4 = 'conflict10' where c1 = (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = excluded.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(10,10,10) on duplicate key update c4 = 'ERROR' where c1 != (
    with cte1 as (
        with cte2 as (
            with cte3 as (
                with cte4 as (
                    with cte5 as (
                        select c1 from tab_source where c3 = excluded.c1
                    ) select c1 from cte5
                ) select c1 from cte4
            ) select c1 from cte3
        ) select c1 from cte2
    ) select c1 from cte1 limit 1
);

select * from tab_target order by 1,2,3,4;

rollback;

--------
-- misc
--------
begin;
-- agg + group by
insert into tab_target values(1,2,3) on duplicate key update c4 = 'conflict1' where excluded.c2 = (select count(1) from tab_source where c2 < 3);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(1,2,3) on duplicate key update c4 = 'ERROR' where excluded.c2 != (select count(1) from tab_source where c2 < 3);

-- limit + offset
insert into tab_target values(2,3,5) on duplicate key update c4 = 'conflict2' where excluded.c2 = (select c2 from tab_source order by c1 asc limit 1 offset 7);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(2,3,5) on duplicate key update c4 = 'ERROR' where excluded.c2 != (select c2 from tab_source order by c1 asc limit 1 offset 7)
                                                                                    and c3 = (select c1 from tab_source order by c1 desc limit 1 offset 7);

-- window funcs
insert into tab_target values(3,5,7) on duplicate key update c4 = 'conflict3' where c2 = (select sum_rows from (
    SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following) as sum_rows
    FROM generate_series(1, 3) i order by 2 limit 1
));

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(3,5,7) on duplicate key update c4 = 'ERROR' where c2 > (select sum_rows from (
    SELECT i, sum(i) over (order by i rows between 1 preceding and 1 following) as sum_rows
    FROM generate_series(1, 3) i order by 2 limit 1
));

-- setopt
insert into tab_target values(4,8,9) on duplicate key update c4 = 'conflict4' where c1 = (
    select count(*) / 2.5 from (
        (select c1, c2 from tab_source)
        union
        (select c2, c1 from tab_source)
    )
);

explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(4,8,9) on duplicate key update c4 = 'ERROR' where c1 = (
    select count(*) / 4 from (
        (select c1, c2 from tab_source)
        union all
        (select c2, c1 from tab_source)
        minus
        (select c1, c4 from tab_source)
        intersect
        (select c2, c3 from tab_source)
    )
);

-- with param
prepare p1 as
insert into tab_target values($1,$2,$3) on duplicate key update c4 = $4 where c1 in (
    with cte as (
        select c1 from tab_source where c2 in (
            select c2 from tab_source where c1 <= $5
        )
    ) select c1 from cte where c1 >= $6
);

-- gplan not supported yet
set plan_cache_mode = force_generic_plan;
explain (analyze on, verbose off, timing off, costs off)
execute p1(5, 6, 7, 'conflict5', 5, 5);

set plan_cache_mode = force_custom_plan;
explain (analyze on, verbose off, timing off, costs off)
execute p1(5, 6, 7, 'ERROR', 4, 5);

-- test with hint
-- blockname
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,8,7) on duplicate key update c4 = 'conflict6' where c1 = (
    select /*+ blockname(tt) */ c4 from tab_source where c1 = 6
);

-- no_expand
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(6,8,7) on duplicate key update c4 = 'ERROR' where c1 != (
    select /*+ no_expand */ c4 from tab_source where c1 = 6
);

-- leading/join
explain (analyze on, verbose off, timing off, costs off)
insert into tab_target values(7,4,7) on duplicate key update c4 = 'conflict7' where c1 = (
    select /*+ leading((t2 t1)) mergejoin(t1 t2) */ t1.c4 from tab_source t1, tab_source t2 where t1.c2 = t2.c2 and t1.c3 = 7
);

-- rowmarks
insert into tab_target values(8,4,3) on duplicate key update c4 = 'conflict8' where c1 in (select c4 from tab_source where c4 = 8 for update);

insert into tab_target values(9,6,3) on duplicate key update c4 = 'conflict9' where c1 = (select c3 from tab_source where c1 = 9 for share);

select * from tab_target where c1 < 10 order by 1,2,3,4;
rollback;


DROP SCHEMA schema_upsert_where_sublink CASCADE;