--
-- Verify that concurrent partition MERGE and UPSERT do not leak fakerel or
-- partcache references when an invisible partition OID is replaced.
--

drop table if exists issue_8420_interval_part_t;
drop table if exists issue_8420_tmp_t;

create table issue_8420_tmp_t
(
    a date primary key,
    b int,
    c varchar2(30)
)
partition by range (a) interval ('1 month')
(
    partition p1 values less than ('2021-01-31 23:59:59'),
    partition p2 values less than ('2021-02-28 23:59:59'),
    partition p3 values less than ('2021-03-31 23:59:59'),
    partition p4 values less than ('2021-04-30 23:59:59')
)
enable row movement;

create table issue_8420_interval_part_t (like issue_8420_tmp_t including all);

create index issue_8420_interval_idx on issue_8420_interval_part_t(a, c);

insert into issue_8420_interval_part_t values
    ('2021-03-07 17:00:00', 1, 'rr'), ('2021-02-08 17:00:00', 2, 'dd');
insert into issue_8420_interval_part_t values
    ('2021-04-07 17:00:00', 1, 'rr'), ('2021-04-08 17:00:00', 2, 'dd');
insert into issue_8420_interval_part_t values
    ('2021-08-07 17:00:00', 1, 'dd'), ('2021-09-08 17:00:00', 2, 'dd');
insert into issue_8420_interval_part_t values
    ('2021-05-06 17:00:00', 1, 'dd'), ('2021-10-18 17:00:00', 2, 'dd');
insert into issue_8420_interval_part_t values
    ('2021-05-17 17:00:00', 1, '1'), ('2021-10-08 17:00:00', 2, '1');
insert into issue_8420_interval_part_t values
    ('2021-10-17 17:00:00', 1, '1'), ('2021-11-08 17:00:00', 2, '1');
insert into issue_8420_interval_part_t values
    ('2021-08-17 17:00:00', 1, '1'), ('2021-08-08 17:00:00', 2, '1');
insert into issue_8420_interval_part_t values
    ('2021-09-17 17:00:00', 1, '1'), ('2021-10-02 17:00:00', 2, '1');
insert into issue_8420_interval_part_t values
    ('2021-11-17 17:00:00', 1, '1'), ('2023-01-08 17:00:00', 2, '1');
insert into issue_8420_interval_part_t values
    ('2021-12-28 17:00:00', 2, '1'), ('2021-11-18 17:00:00', 1, '1');

-- Suppress nondeterministically ordered command tags from parallel workers.
-- Server warnings are written to stderr and remain visible to pg_regress.
\o /dev/null
\parallel on 4
alter table issue_8420_interval_part_t
    merge partitions sys_p2, sys_p4, sys_p5 into partition sys_p5 update global index;
insert into issue_8420_interval_part_t partition (sys_p5) values
    ('2021-11-17 17:00:00', 1, '1'), ('2021-11-18 17:00:00', 1, '1')
    on duplicate key update c = 'specified_partition';
insert into issue_8420_interval_part_t values
    ('2023-01-08 17:00:00', 2, '1')
    on duplicate key update c = 'zz';
insert into issue_8420_interval_part_t values
    ('2021-12-28 17:00:00', 2, '1')
    on duplicate key update c = 'zz';
\parallel off
\o

select count(*) as total_rows,
       sum(case when c = 'zz' then 1 else 0 end) as updated_rows,
       sum(case when c = 'specified_partition' then 1 else 0 end) as specified_rows
from issue_8420_interval_part_t;

select to_char(a, 'YYYY-MM-DD HH24:MI:SS') as a, b, c
from issue_8420_interval_part_t
where a in ('2021-11-17 17:00:00', '2021-11-18 17:00:00',
            '2021-12-28 17:00:00', '2023-01-08 17:00:00')
order by a;

-- Exercise the other write paths whose callers now receive the opened OID.
copy issue_8420_interval_part_t (a, b, c) from stdin;
2024-01-15 17:00:00	3	copy_row
\.

update issue_8420_interval_part_t
set a = '2024-02-15 17:00:00'
where a = '2024-01-15 17:00:00';

select to_char(a, 'YYYY-MM-DD HH24:MI:SS') as a, b, c
from issue_8420_interval_part_t
where c = 'copy_row';

drop table issue_8420_interval_part_t;
drop table issue_8420_tmp_t;
