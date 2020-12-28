--- This test is used to ensure the the patterns of auxiliary relations names, like toast table names and toast table index names.

create schema redis_fence_001;
set search_path to redis_fence_001;

--- We do not really need to check the columns of pg_class and pg_partition here. Just remind someone who adds other auxiliary relations 
--- to consider the impact on the redistribution.
\d pg_class
\d pg_partition

--- row tables.
create table row_table_001(a int, b text);

select B.relname='pg_toast_'||A.oid as relname_matched,C.relname='pg_toast_'||A.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_class C where A.relname='row_table_001' and B.oid=A.reltoastrelid and C.oid=B.reltoastidxid;
select pd.deptype from pg_class pc, pg_depend pd where pd.objid=pc.reltoastrelid and pd.refobjid=pc.oid and pc.relname='row_table_001';

create table row_table_002(a int, b text) partition by range(a)(
partition row_table_002_partition_a values less than(10),
partition row_table_002_partition_b values less than(20),
partition row_table_002_partition_c values less than(30)
);

select B.relname from pg_partition A, pg_partition B where A.relname='row_table_002' and B.parentid=A.parentid and B.relfilenode!=0 order by relname;
select A.relname='pg_toast_part_'||C.oid as relname_matched, B.relname='pg_toast_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.reltoastrelid and B.oid=A.reltoastidxid and C.relname='row_table_002_partition_a';
select A.relname='pg_toast_part_'||C.oid as relname_matched, B.relname='pg_toast_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.reltoastrelid and B.oid=A.reltoastidxid and C.relname='row_table_002_partition_b';
select A.relname='pg_toast_part_'||C.oid as relname_matched, B.relname='pg_toast_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.reltoastrelid and B.oid=A.reltoastidxid and C.relname='row_table_002_partition_c';

--- column tables.
create table col_table_001(a int, b text) with(orientation=column);

select B.relname='pg_toast_'||A.oid as relname_matched,C.relname='pg_toast_'||A.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_class C where A.relname='col_table_001' and B.oid=A.reltoastrelid and C.oid=B.reltoastidxid;
select pd.deptype from pg_class pc, pg_depend pd where pd.objid=pc.reltoastrelid and pd.refobjid=pc.oid and pc.relname='col_table_001';

--- FIX IT: column store don't support pg_delta_%d_index now.
select B.relname='pg_delta_'||A.oid as relname_matched, A.reldeltaidx=0 as relindex_matched from pg_class A, pg_class B where A.relname='col_table_001' and B.oid=A.reldeltarelid;
select pd.deptype from pg_class pc, pg_depend pd where pd.objid=pc.reldeltarelid and pd.refobjid=pc.oid and pc.relname='col_table_001';

select B.relname='pg_cudesc_'||A.oid as relname_matched,C.relname='pg_cudesc_'||A.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_class C where A.relname='col_table_001' and B.oid=A.relcudescrelid and C.oid=A.relcudescidx;
select pd.deptype from pg_class pc, pg_depend pd where pd.objid=pc.relcudescrelid and pd.refobjid=pc.oid and pc.relname='col_table_001';

create table col_table_002(a int, b text) with(orientation=column) partition by range(a)(
partition col_table_002_partition_a values less than(10),
partition col_table_002_partition_b values less than(20),
partition col_table_002_partition_c values less than(30)
);

select B.relname from pg_partition A, pg_partition B where A.relname='col_table_002' and B.parentid=A.parentid and B.relfilenode!=0 order by relname;
select A.relname='pg_toast_part_'||C.oid as relname_matched, B.relname='pg_toast_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.reltoastrelid and B.oid=A.reltoastidxid and C.relname='col_table_002_partition_a';
select A.relname='pg_toast_part_'||C.oid as relname_matched, B.relname='pg_toast_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.reltoastrelid and B.oid=A.reltoastidxid and C.relname='col_table_002_partition_b';
select A.relname='pg_toast_part_'||C.oid as relname_matched, B.relname='pg_toast_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.reltoastrelid and B.oid=A.reltoastidxid and C.relname='col_table_002_partition_c';

--- FIX IT: column store don't support pg_delta_part_%d_index now.
select A.relname='pg_delta_part_'||B.oid as relname_matched, B.reldeltaidx=0 as relindex_matched from pg_class A, pg_partition B where A.oid=B.reldeltarelid and B.relname='col_table_002_partition_a';
select A.relname='pg_delta_part_'||B.oid as relname_matched, B.reldeltaidx=0 as relindex_matched from pg_class A, pg_partition B where A.oid=B.reldeltarelid and B.relname='col_table_002_partition_b';
select A.relname='pg_delta_part_'||B.oid as relname_matched, B.reldeltaidx=0 as relindex_matched from pg_class A, pg_partition B where A.oid=B.reldeltarelid and B.relname='col_table_002_partition_c';

select A.relname='pg_cudesc_part_'||C.oid as relname_matched, B.relname='pg_cudesc_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.relcudescrelid and B.oid=C.relcudescidx and C.relname='col_table_002_partition_a';
select A.relname='pg_cudesc_part_'||C.oid as relname_matched, B.relname='pg_cudesc_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.relcudescrelid and B.oid=C.relcudescidx and C.relname='col_table_002_partition_b';
select A.relname='pg_cudesc_part_'||C.oid as relname_matched, B.relname='pg_cudesc_part_'||C.oid||'_index' as relindex_matched from pg_class A, pg_class B, pg_partition C where A.oid=C.relcudescrelid and B.oid=C.relcudescidx and C.relname='col_table_002_partition_c';

drop schema redis_fence_001 cascade;