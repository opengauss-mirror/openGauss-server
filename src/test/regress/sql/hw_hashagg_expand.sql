set enable_codegen = off;
CREATE TABLE tb_evt (
    begintime bigint,
    event character varying(60),
    usernum character varying(60),
    homearea character varying(60),
    relatenum character varying(60),
    relatehomeac character varying(60),
    imsi character varying(60),
    imei character varying(60),
    curarea character varying(60),
    neid character varying(60),
    lai integer,
    ci character varying(60),
    longitude double precision,
    latitude double precision,
    oldlai integer,
    oldci character varying(60),
    oldlongitude double precision,
    oldlatitude double precision,
    sid character varying(60),
    state integer,
    idflag integer,
    dtmf character varying(60),
    tmsi character varying(60),
    spcode character varying(60)
)

PARTITION BY RANGE (begintime)
(
    PARTITION tb_evt_2015011823 VALUES LESS THAN (2421596800)
);

insert into tb_evt(lai,ci,begintime) values(1,'a',1);
insert into tb_evt(lai,ci,begintime) values(1,'b',2);
insert into tb_evt(lai,ci,begintime) values(1,'c',3);
insert into tb_evt(lai,ci,begintime) values(2,'a',4);
insert into tb_evt(lai,ci,begintime) values(1,'b',5);
insert into tb_evt(lai,ci,begintime) values(1,'c',6);
insert into tb_evt(lai,ci,begintime) values(2,'d',7);

set enable_hashagg to off;
analyze tb_evt;
explain analyze select lai,ci from tb_evt group by lai,ci;
drop table tb_evt;

reset enable_hashagg;
analyze base_tab_000;
analyze base_type_tab_000;
set work_mem=64;
select count(*)
from
(
select avg(a.col_numeric),
       sum(b.col_interval),
       max(a.col_bigint),
       count(b.col_timestamp),
       coalesce(a.col_int, 0)
  from base_tab_000 a
  join base_type_tab_000 b
    on coalesce(a.col_int, 0) + 1 = coalesce(b.col_int, 0)
 group by coalesce(a.col_int, 0)
 order by 1, 2, 3, 4, 5
);

select count(*)
from base_tab_000
where col_numeric > (select avg(col_numeric) from base_tab_000 )
;

set enable_compress_spill = off;
select count(*)
from
(
select avg(a.col_numeric),
       sum(b.col_interval),
       max(a.col_bigint),
       count(b.col_timestamp),
       coalesce(a.col_int, 0)
  from base_tab_000 a
  join base_type_tab_000 b
    on coalesce(a.col_int, 0) + 1 = coalesce(b.col_int, 0)
 group by coalesce(a.col_int, 0)
 order by 1, 2, 3, 4, 5
);

select count(*)
from base_tab_000
where col_numeric > (select avg(col_numeric) from base_tab_000 )
;
reset enable_compress_spill;
select count(*) from base_tab_000
where col_numeric > (select avg(a.col_numeric) from base_tab_000 a join base_type_tab_000 b on coalesce(a.col_int, 0) + 1 = coalesce(b.col_int, 0));

drop table base_tab_000;
drop table base_type_tab_000;


create table col_TMP_CUST_ASSET_SUM_1
(
Party_Id                 VARCHAR(30)    NOT NULL,
Zone_Num                 CHAR(5)        NOT NULL,
Asset_Max_Belong_Org_Num VARCHAR(30)    NOT NULL
) with(orientation =column);

insert into col_TMP_CUST_ASSET_SUM_1 select * from TMP_CUST_ASSET_SUM_1;
select count(*) from col_TMP_CUST_ASSET_SUM_1;

set work_mem=64;
set explain_perf_mode=pretty;
\o hashagg_expand.out
explain (analyze on, detail on, timing off)select count(*) from col_tmp_cust_asset_sum_1 group by zone_num;
\o
\! rm hashagg_expand.out
set explain_perf_mode=normal;

set hashagg_table_size = 4100;

explain (analyze on,detail on, costs off, timing off)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM col_TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;

set work_mem='64MB';
explain (analyze on, detail on, costs off, timing off)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM col_TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;

--test setop
explain (analyze on, detail on, costs off, timing off)
SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM col_TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3 intersect all
SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM col_TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3 ;


--test segment hashtable
analyze col_tmp_cust_asset_sum_1;
update pg_statistic set stadndistinct = 2147483647 , stadistinct = 2147483647.0 * 12 where starelid = (select oid from pg_class where relname='col_tmp_cust_asset_sum_1');
update pg_class set reltuples = 2147483647 where relname='col_tmp_cust_asset_sum_1';

set work_mem='4GB';
explain (analyze on,detail on, costs off, timing off)
select count(*) from col_tmp_cust_asset_sum_1 group by zone_num;

set enable_codegen=off;
explain (analyze on,detail on, costs off, timing off)
select count(*) from col_tmp_cust_asset_sum_1 group by zone_num;

drop table col_TMP_CUST_ASSET_SUM_1;
