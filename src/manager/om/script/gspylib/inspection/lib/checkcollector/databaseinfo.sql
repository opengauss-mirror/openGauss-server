select 'table count:'||count(1) as point from pg_class where relkind = 'r' and oid > 16384; 
select 'foreign_table count:'||count(1) as point from pg_foreign_table;
select 'view count:'||count(1) as point from pg_class where relkind = 'v' and oid > 16384;
select 'index count:'||count(1) as point from pg_class where relkind = 'i' and oid > 16384;
select 'tablespace count:'||count(1)-2 as point from pg_tablespace;
select 'database count:'||count(1)-2 as point from pg_database;
select 'unlogged table count:'||count(*) as point from pg_class where relkind='r' and relpersistence='u';
select 'schema count:'||count(1) -9  as point from pg_namespace;
select 'partition table count:'||count(1) as point from DBA_PART_TABLES;
select 'all partition count:'||sum(partition_count) as point from DBA_PART_TABLES;
select 'max part_table partition count:'||max(partition_count) as point from DBA_PART_TABLES;
select 'row count:'||count(1) as point from pg_class where relkind = 'r' and oid > 16384 and reloptions::text not like '%column%' and reloptions::text not like '%internal_mask%';
select 'column count:'||count(1) as point from pg_class where relkind = 'r' and oid > 16384 and reloptions::text like '%column%';
select 'function count:'||count(1)-2943 as point from pg_proc;

