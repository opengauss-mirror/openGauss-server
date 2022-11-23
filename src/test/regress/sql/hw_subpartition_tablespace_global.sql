DROP SCHEMA subpartition_tablespace CASCADE;
CREATE SCHEMA subpartition_tablespace;
SET CURRENT_SCHEMA TO subpartition_tablespace;

create table t1(id int)
partition by range(id)(
partition p1 values less than(100),
partition p2 values less than(200) tablespace pg_global);
drop table if exists t1;

create table t1(id int)
partition by range(id)(
partition p1 values less than(100),
partition p2 values less than(200));
alter table t1 add partition p3 values less than(300);
alter table t1 add partition p4 values less than(400) tablespace pg_global;
drop table if exists t1;

create table b_range_hash_t01(c1 int primary key,c2 int,c3 text)
partition by range(c1) subpartition by hash(c2)
(
partition p1 values less than (100)
(
subpartition p1_1 tablespace pg_global,
subpartition p1_2
),
partition p2 values less than (200)
(
subpartition p2_1,
subpartition p2_2
),
partition p3 values less than (300)
(
subpartition p3_1,
subpartition p3_2
)
);
drop table if exists b_range_hash_t01;

create table b_range_hash_t01(c1 int primary key,c2 int,c3 text)
partition by range(c1) subpartition by hash(c2)
(
partition p1 values less than (100)
(
subpartition p1_1,
subpartition p1_2
),
partition p2 values less than (200)
(
subpartition p2_1,
subpartition p2_2
)
);
alter table b_range_hash_t01 add partition p3 values less than (300)
(
    subpartition p3_1,
    subpartition p3_2
);
alter table b_range_hash_t01 add partition p4 values less than (400)
(
    subpartition p4_1 tablespace pg_global,
    subpartition p4_2
);
drop table if exists b_range_hash_t01;

DROP SCHEMA subpartition_tablespace CASCADE;
RESET CURRENT_SCHEMA;