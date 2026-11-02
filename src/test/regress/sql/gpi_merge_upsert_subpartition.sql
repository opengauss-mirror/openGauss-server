--
-- Verify fake relation lookup for subpartitioned tables.
-- MERGE PARTITIONS is not supported for subpartitioned tables, so this case
-- focuses on the recursive parent/subpartition lookup used by DML paths.
--

\o /dev/null
drop table if exists issue_8420_subpartition_t cascade;

create table issue_8420_subpartition_t
(
    a date not null,
    b int not null,
    c varchar2(30),
    primary key (a, b)
)
partition by range (a) subpartition by range (b)
(
    partition p1 values less than ('2021-02-01')
    (
        subpartition p1_s1 values less than (10),
        subpartition p1_s2 values less than (maxvalue)
    ),
    partition p2 values less than ('2021-03-01')
    (
        subpartition p2_s1 values less than (10),
        subpartition p2_s2 values less than (maxvalue)
    ),
    partition p3 values less than ('2021-04-01')
    (
        subpartition p3_s1 values less than (10),
        subpartition p3_s2 values less than (maxvalue)
    )
)
enable row movement;

insert into issue_8420_subpartition_t values ('2021-01-10', 1, 'initial');
insert into issue_8420_subpartition_t values ('2021-01-10', 1, 'initial')
    on duplicate key update c = 'upsert';

copy issue_8420_subpartition_t (a, b, c) from stdin;
2021-02-10	2	copy_row
\.

update issue_8420_subpartition_t
set a = '2021-03-10'
where c = 'copy_row';

delete from issue_8420_subpartition_t
where c = 'upsert';
\o

\pset format unaligned
\pset tuples_only on

select count(*) as total_rows,
       sum(case when c = 'upsert' then 1 else 0 end) as upsert_rows,
       sum(case when c = 'copy_row' then 1 else 0 end) as copy_rows
from issue_8420_subpartition_t;

select to_char(a, 'YYYY-MM-DD HH24:MI:SS') as a, b, c
from issue_8420_subpartition_t
order by a, b;

\o /dev/null
drop table issue_8420_subpartition_t cascade;
