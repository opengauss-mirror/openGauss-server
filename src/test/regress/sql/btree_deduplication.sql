drop table if exists t1;
create table t1(
    c1 int,
    c2 varchar(32),
    c3 timestamp without time zone
);
insert into t1 (c1, c2, c3) select n, 't1' || n, '2020-06-29 21:00:00' from generate_series(1, 25000) n;

create index idx_dedup_1 on t1 using btree(c3) with (deduplication=on);
create index idx_dedup_2 on t1 using btree(c3) with (deduplication=off);
\di+

alter index idx_dedup_1 set (deduplication=off);
alter index idx_dedup_2 set (deduplication=on);
\di+

insert into t1 (c1, c2, c3) select n, 't1' || n, '2020-06-29 21:00:00' from generate_series(1, 25000) n;
\di+

reindex index idx_dedup_1;
reindex index idx_dedup_2;
\di+

alter index idx_dedup_1 set (deduplication=on);
alter index idx_dedup_2 set (deduplication=off);
reindex index idx_dedup_1;
reindex index idx_dedup_2;
\di+

delete from t1 where c1 > 10 and c1 < 50;
delete from t1 where c1 > 3000 and c1 < 4000;
vacuum t1;
delete from t1;
vacuum t1;

drop table if exists t1;
create table t1(
    c1 int,
    c2 varchar(32),
    c3 timestamp without time zone,
    c4 int
) partition by range(c4)
(
    partition p1 values less than (10),
    partition p2 values less than (20),
    partition p13 values less than (maxvalue)
);
insert into t1 (c1, c2, c3, c4) select n, 't1' || n, '2020-01-01 20:35:00', m from generate_series(1, 25000) n, generate_series(0, 29) m;

create index idx_dedup_1_g on t1 using btree(c3) global with (deduplication=on);
create index idx_dedup_2_g on t1 using btree(c3) global with (deduplication=off);
\di+

drop index idx_dedup_1_g;
drop index idx_dedup_2_g;

create index idx_dedup_1_l on t1 using btree(c3) local with (deduplication=on);
create index idx_dedup_2_l on t1 using btree(c3) local with (deduplication=off);
\di+

delete from t1;
vacuum t1;

drop table if exists t1;
create table t1(
    c1 int,
    c2 varchar(32),
    c3 timestamp without time zone
) with (storage_type = ustore);

create index idx_dedup_1 on t1 using ubtree(c3) with (deduplication=on);

drop table if exists t1;