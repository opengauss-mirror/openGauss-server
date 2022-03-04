select * from gs_gsc_dbstat_info() limit 1;
select * from gs_gsc_dbstat_info(-1) limit 1;
select * from gs_gsc_dbstat_info(0) limit 1;
select * from gs_gsc_dbstat_info(1) limit 1;
select * from gs_gsc_dbstat_info(2) limit 1;

select * from gs_gsc_catalog_detail() limit 1;
select * from gs_gsc_catalog_detail(-1) limit 1;
select * from gs_gsc_catalog_detail(0) limit 1;
select * from gs_gsc_catalog_detail(1) limit 1;
select * from gs_gsc_catalog_detail(-1, 1262) limit 1;
select * from gs_gsc_catalog_detail(0, 1262) limit 1;
select * from gs_gsc_catalog_detail(1, 1262) limit 1;
select * from gs_gsc_catalog_detail(-1, 1259) limit 1;
select * from gs_gsc_catalog_detail(0, 1259) limit 1;
select * from gs_gsc_catalog_detail(1, 1259) limit 1;
select * from gs_gsc_catalog_detail(2, 1259) limit 1;

select * from gs_gsc_table_detail() limit 1;
select * from gs_gsc_table_detail(-1) limit 1;
select * from gs_gsc_table_detail(0) limit 1;
select * from gs_gsc_table_detail(1) limit 1;
select * from gs_gsc_table_detail(-1, 1262) limit 1;
select * from gs_gsc_table_detail(0, 1262) limit 1;
select * from gs_gsc_table_detail(1, 1262) limit 1;
select * from gs_gsc_table_detail(-1, 1259) limit 1;
select * from gs_gsc_table_detail(0, 1259) limit 1;
select * from gs_gsc_table_detail(1, 1259) limit 1;
select * from gs_gsc_table_detail(2, 1259) limit 1;

select * from gs_gsc_clean() limit 1;
select * from gs_gsc_clean(-1) limit 1;
select * from gs_gsc_clean(0) limit 1;
select * from gs_gsc_clean(1) limit 1;
select * from gs_gsc_clean(2) limit 1;