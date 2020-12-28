/*
 * This file is used to test pull down of count(distinct) expression
 */
drop schema if exists distribute_count_distinct_part4 cascade;
create schema distribute_count_distinct_part4;
set current_schema = distribute_count_distinct_part4;

-- multi count distinct with AP function
create table t (a int not null, b int, c int);
insert into t values (1,1,1);
select a, count(distinct b), max(distinct c)
from t
group by cube(a)
order by 1,2,3;
drop table t;

create table llvm_call_center (cc_call_center_sk integer not null) /*distribute by replication*/;
create table llvm_web_site (web_site_sk integer not null,web_site_id char(17) not null,web_rec_end_date date) /*distribute by hash(web_site_sk)*/;
create table date_dim_less (d_date_sk varchar(19) not null,d_year bigint,d_current_month integer(39,38) default null,d_current_quarter numeric(29,10)) /*distribute by hash (d_current_month,d_year)*/;
create table llvm_customer (c_customer_id char(17) not null,c_birth_day smallint,c_birth_month integer) /*distribute by hash(c_birth_day,c_birth_month)*/;
--inner join
explain (costs off)
SELECT dt.web_site_sk * d_current_quarter
    , min(web_rec_end_date)
    , avg(DISTINCT dt.web_site_sk * d_current_quarter)
FROM llvm_call_center AS cc
CROSS JOIN (
    SELECT web_site_sk
        , min(web_rec_end_date + 5) web_rec_end_date
    FROM llvm_web_site
    WHERE web_site_id IN (
            SELECT c_customer_id
            FROM llvm_customer
            GROUP BY 1
                , substr(c_customer_id, - 1)
            )
    GROUP BY web_site_sk
        , substr(web_site_id, - 2)
    ) dt
INNER JOIN date_dim_less ON substr(d_date_sk, - 1) = web_site_sk
WHERE d_current_quarter <> 0.0
GROUP BY 1
    , web_site_sk
HAVING dt.web_site_sk > 1
    AND dt.web_site_sk * d_current_quarter IS NOT NULL
    AND min(DISTINCT web_rec_end_date) IS NOT NULL
ORDER BY 1
    , 2
    , 3;
--full join
explain (costs off)
SELECT dt.web_site_sk * d_current_quarter
    , min(web_rec_end_date)
    , avg(DISTINCT dt.web_site_sk * d_current_quarter)
FROM llvm_call_center AS cc
CROSS JOIN (
    SELECT web_site_sk
        , min(web_rec_end_date + 5) web_rec_end_date
    FROM llvm_web_site
    WHERE web_site_id IN (
            SELECT c_customer_id
            FROM llvm_customer
            GROUP BY 1
                , substr(c_customer_id, - 1)
            )
    GROUP BY web_site_sk
        , substr(web_site_id, - 2)
    ) dt
FULL JOIN date_dim_less ON substr(d_date_sk, - 1) = web_site_sk
WHERE d_current_quarter <> 0.0
GROUP BY 1
    , web_site_sk
HAVING dt.web_site_sk > 1
    AND dt.web_site_sk * d_current_quarter IS NOT NULL
    AND min(DISTINCT web_rec_end_date) IS NOT NULL
ORDER BY 1
    , 2
    , 3;
--inner join
explain (costs off)
select dt.web_site_sk * d_current_quarter,
avg(distinct dt.web_site_sk)
from (select web_site_sk, web_rec_end_date
from llvm_web_site) dt
inner join date_dim_less
on substr(d_date_sk, -1) = web_site_sk
group by 1, web_site_sk
having dt.web_site_sk > 1 and min(distinct web_rec_end_date) is not null
order by 1, 2;
--left join
explain (costs off)
select dt.web_site_sk * d_current_quarter,
avg(distinct dt.web_site_sk)
from (select web_site_sk, web_rec_end_date
from llvm_web_site) dt
left join date_dim_less
on substr(d_date_sk, -1) = web_site_sk
group by 1, web_site_sk
having dt.web_site_sk > 1 and min(distinct web_rec_end_date) is not null
order by 1, 2;
--right join
explain (costs off)
select dt.web_site_sk * d_current_quarter,
avg(distinct dt.web_site_sk)
from (select web_site_sk, web_rec_end_date
from llvm_web_site) dt
right join date_dim_less
on substr(d_date_sk, -1) = web_site_sk
group by 1, web_site_sk
having dt.web_site_sk > 1 and min(distinct web_rec_end_date) is not null
order by 1, 2;
explain (costs off)
select dt.web_site_sk * d_current_quarter,
       avg(distinct dt.web_site_sk)
  from (select web_site_sk, web_rec_end_date
               from llvm_web_site) dt
 right join date_dim_less
    on substr(d_date_sk, -1) = web_site_sk
 group by 1, web_site_sk+1
having coalesce(dt.web_site_sk+1,2)>1 and min(distinct web_rec_end_date) is not null
 order by 1, 2;
explain (costs off)
select dt.web_site_sk * d_current_quarter,
       avg(distinct dt.web_site_sk)
  from (select web_site_sk, web_rec_end_date
               from llvm_web_site) dt
 right join date_dim_less
    on substr(d_date_sk, -1) = web_site_sk
 group by 1, web_site_sk+web_site_sk
having coalesce(dt.web_site_sk+web_site_sk,2)>1 and min(distinct web_rec_end_date) is not null
 order by 1, 2;

set explain_perf_mode=pretty;

CREATE TABLE m_inte_counter_detail (
    rpt_dt character varying(36),
    rpt_time character varying(32),
    org_id character varying(128),
    sec_bank character varying(128),
    thd_bank character varying(128),
    resp_code character varying(800),
    term_code character varying(128),
    seqno character varying(256),
    trace_no character varying(48),
    yw_account character varying(128),
    counter_code character varying(256),
    menu_code character varying(128),
    retriveref_no character varying(56),
    txn_amount numeric,
    fee_income numeric,
    fund_type character varying(40),
    card_type character varying(40),
    settdate character varying(36),
    ywbh character varying(40),
    ywlx character varying(40),
    is_type character varying(4),
    sb_type character varying(20),
    on_flag character varying(4),
    sys_flag character varying(4)
)
WITH (orientation=column)
/*DISTRIBUTE BY HASH (rpt_dt, org_id)*/;


CREATE TABLE m_pub_org_stat_stt (
    net_bank character varying(10) NOT NULL,
    sec_bank character varying(10) NOT NULL,
    thd_bank character varying(10) NOT NULL,
    main_bank character varying(10) NOT NULL,
    net_bank_name character varying(50),
    sec_bank_name character varying(50),
    thd_bank_name character varying(50),
    main_bank_name character varying(50),
    org_flag character varying(1),
    district_flag character varying(10),
    ods_data_dt character varying(10),
    township_flag character varying(10)
)
WITH (orientation=column)
/*DISTRIBUTE BY HASH (net_bank)*/;

-- original test
explain (verbose, costs off)
select '20180831' rpt_Dt, org_id org_id,
       count(case
               when a.rpt_dt = '20180831' and
                    a.ywbh in ('223', '478', '819', '886') then
                1
             end) kk,
       count(distinct case
               when a.is_type in ('TTS', 'GXH') then
                a.term_code
             end) tts,
       count(distinct case
               when a.rpt_dt = '20180831' and a.is_type in ('TTS', 'GXH') then
                a.term_code
             end) tts2,
       count(distinct case
               when a.is_type = 'BXS' then
                a.term_code
             end) bxs
  from m_inte_counter_detail a left join m_pub_org_stat_stt b on a.org_id = b.net_bank
 group by a.org_id;

-- targetlist & grouplist with different varno
explain (verbose, costs off)
select '20180831' rpt_Dt, a.org_id,
       count(case
               when a.rpt_dt = '20180831' and
                    a.ywbh in ('223', '478', '819', '886') then
                1
             end) kk,
       count(distinct case
               when a.is_type in ('TTS', 'GXH') then
                a.term_code
             end) tts,
       sum(distinct case
               when a.rpt_dt = '20180831' and a.is_type in ('TTS', 'GXH') then
                a.term_code
             end) tts2,
       sum(distinct case
               when a.is_type = 'BXS' then
                a.term_code
             end) bxs
  from m_inte_counter_detail a left join m_pub_org_stat_stt b on a.org_id = b.net_bank
 group by org_id;

-- explore count-distinct in having clause
explain (verbose, costs off)
select '20180831' rpt_Dt, a.org_id,
       count(case
               when a.rpt_dt = '20180831' and
                    a.ywbh in ('223', '478', '819', '886') then
                1
             end) kk,
       sum(distinct case
               when a.is_type = 'BXS' then
                a.term_code
             end) bxs
  from m_inte_counter_detail a left join m_pub_org_stat_stt b on a.org_id = b.net_bank
 group by org_id
 having count(distinct a.is_type) > 100;

-- explore count-distinct in having clause with expressions
explain (verbose, costs off)
select '20180831' rpt_Dt, org_id,
       count(case
               when a.rpt_dt = '20180831' and
                    a.ywbh in ('223', '478', '819', '886') then
                1
             end) kk,
       sum(distinct case
               when a.is_type = 'BXS' then
                a.term_code
             end) bxs
  from m_inte_counter_detail a left join m_pub_org_stat_stt b on a.org_id = b.net_bank
 group by a.org_id
 having sum(distinct a.is_type) + avg(distinct org_id)> 100;

-- explore count-distinct in order by clause
explain (verbose, costs off)
select '20180831' rpt_Dt, org_id,
       count(case
               when a.rpt_dt = '20180831' and
                    a.ywbh in ('223', '478', '819', '886') then
                1
             end) kk,
       count(distinct case
               when a.is_type = 'BXS' then
                a.term_code
             end) bxs
  from m_inte_counter_detail a left join m_pub_org_stat_stt b on a.org_id = b.net_bank
 group by a.org_id
 having sum(distinct a.is_type) + avg(distinct org_id)> 100
 order by 2;

-- explore count-distinct in having clause of order by clause
explain (verbose, costs off)
select '20180831' rpt_Dt, org_id,
       count(case
               when a.rpt_dt = '20180831' and
                    a.ywbh in ('223', '478', '819', '886') then
                1
             end) kk,
       count(distinct case
               when is_type = 'BXS' then
                a.term_code
             end) bxs
  from m_inte_counter_detail a left join m_pub_org_stat_stt b on a.org_id = b.net_bank
 group by a.org_id
 having sum(distinct a.is_type) + avg(distinct (a.org_id + org_id))> 100
 order by 2;

-- explore count-distinct in AP function and window function
explain (verbose, costs off)
select '20180831' rpt_Dt, org_id,
       count(case
               when a.rpt_dt = '20180831' and
                    a.ywbh in ('223', '478', '819', '886') then
                1
             end) kk,
       count(distinct case
               when is_type = 'BXS' then
                a.term_code
             end) bxs,
	grouping(org_id),
	rank() over (partition by org_id, a.org_id order by org_id, a.org_id)
  from m_inte_counter_detail a left join m_pub_org_stat_stt b on a.org_id = b.net_bank
 group by cube(a.org_id)
 having sum(distinct a.is_type) + avg(distinct (a.org_id + org_id))> 100
 order by 2;

reset explain_perf_mode;

reset current_schema;
drop schema if exists distribute_count_distinct_part4 cascade;
