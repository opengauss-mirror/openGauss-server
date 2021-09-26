--
-- Test that an IN implemented using a UniquePath does unique-ification
-- with the right semantics, as per bug #4113.  (Unfortunately we have
-- no simple way to ensure that this test case actually chooses that type
-- of plan, but it does in releases 7.4-8.3.  Note that an ordering difference
-- here might mean that some other plan type is being used, rendering the test
-- pointless.)
--

-- create temp table numeric_table (num_col numeric);
create  table numeric_table (num_col numeric);
insert into numeric_table values (1), (1.000000000000000000001), (2), (3);

-- create temp table float_table (float_col float8);
create  table float_table (float_col float8);
insert into float_table values (1), (2), (3);

select * from float_table
  where float_col in (select num_col from numeric_table) 
  ORDER BY float_col;

select * from numeric_table
  where num_col in (select float_col from float_table) 
  ORDER BY num_col;
--
-- Test case for bug #4290: bogus calculation of subplan param sets
--

-- create temp table ta (id int primary key, val int);
create  table ta (id int primary key, val int);

insert into ta values(1,1);
insert into ta values(2,2);

-- create temp table tb (id int primary key, aval int);
create  table tb (id int primary key, aval int);

insert into tb values(1,1);
insert into tb values(2,1);
insert into tb values(3,2);
insert into tb values(4,2);

-- create temp table tc (id int primary key, aid int);
create  table tc (id int primary key, aid int);

insert into tc values(1,1);
insert into tc values(2,2);

select
  ( select min(tb.id) from tb
    where tb.aval = (select ta.val from ta where ta.id = tc.aid) ) as min_tb_id
from tc 
ORDER BY min_tb_id;

--
-- Test case for 8.3 "failed to locate grouping columns" bug
--

-- create temp table t1 (f1 numeric(14,0), f2 varchar(30));
create  table subselect_t1 (f1 numeric(14,0), f2 varchar(30));

select * from
  (select distinct f1, f2, (select f2 from subselect_t1 x where x.f1 = up.f1) as fs
   from subselect_t1 up) ss
group by f1,f2,fs;

--
-- Test case for bug #5514 (mishandling of whole-row Vars in subselects)
--

-- create temp table table_a(id integer);
create  table table_a(id integer);
insert into table_a values (42);

-- create temp view view_a as select * from table_a;
create  view view_a as select * from table_a;

select view_a from view_a;

select (select view_a) from view_a;
select (select (select view_a)) from view_a;
select (select (a.*)::text) from view_a a;

--
-- Check that whole-row Vars reading the result of a subselect don't include
-- any junk columns therein
--
select q from (select max(f1) from int4_tbl group by f1 order by f1) q;
with q as (select max(f1) from int4_tbl group by f1 order by f1)
  select q from q;

--
-- Test case for sublinks pushed down into subselects via join alias expansion
--

select
  (select sq1) as qq1
from
  (select exists(select 1 from int4_tbl where f1 = q2) as sq1, 42 as dummy
   from int8_tbl) sq0
  join
  int4_tbl i4 on dummy = i4.f1;

--
-- Test case for cross-type partial matching in hashed subplan (bug #7597)
--

-- create temp table outer_7597 (f1 int4, f2 int4);
create  table outer_7597 (f1 int4, f2 int4);
insert into outer_7597 values (0, 0);
insert into outer_7597 values (1, 0);
insert into outer_7597 values (0, null);
insert into outer_7597 values (1, null);

-- create temp table inner_7597(c1 int8, c2 int8);
create  table inner_7597(c1 int8, c2 int8);
insert into inner_7597 values(0, null);

select * from outer_7597 where (f1, f2) not in (select * from inner_7597) order by 1, 2;

--
-- Test case for premature memory release during hashing of subplan output
--

select '1'::text in (select '1'::name union all select '1'::name);

--
-- Test case for planner bug with nested EXISTS handling
--
select a.thousand from tenk1 a, tenk1 b
where a.thousand = b.thousand
  and exists ( select 1 from tenk1 c where b.hundred = c.hundred
                   and not exists ( select 1 from tenk1 d
                                    where a.thousand = d.thousand ) );

--adde minus test
create table tb1(id int);
create table tb2(id int);
insert into tb1 values(1),(2),(3);
insert into tb2 values(2);

select * from tb1 minus select * from tb2 order by 1;

drop table tb1;
drop table tb2;

create table tb1(a int, b varchar);
create table tb2(a int, b varchar, c date);
insert into tb1 values(1,'a');
insert into tb1 values(2,'b');
insert into tb2 values(2,'b','2010-10-01');
insert into tb2 values(3,'c','2010-09-04');

select a,b from tb1 minus select a,b from tb2;

delete from tb1;

insert into tb1 values (0, NULL);
insert into tb1 values (1, NULL);
insert into tb1 values (2, NULL);
insert into tb1 values (3, NULL);

select boo_1.a  
from tb1 boo_1 inner join tb1 boo_2
on ( case when boo_1.a in ( 
select boo_3.a  from tb1 boo_3 ) then boo_1.a end ) in 
(select max( boo.a ) column_009 from tb1 boo ) ;

drop table tb1;
drop table tb2;
DROP table numeric_table CASCADE;
DROP table float_table CASCADE;
DROP table ta CASCADE;
DROP table tb CASCADE;
DROP table tc CASCADE;
DROP table subselect_t1 CASCADE;
DROP table table_a CASCADE;
DROP table outer_7597 CASCADE;
DROP table inner_7597 CASCADE;

---
-- Test subquery and sublink together with insert into ... select ...
---
create schema ediods;
CREATE TABLE EDIODS.MF1_NTHLNSUB2( ACCNO DECIMAL(17,0)
 ,AGACCNO CHAR(17)
 ,LOANNO CHAR(17)
 ,LOANSQNO CHAR(3)
) ;

CREATE TABLE EDIODS.CCM_TA2002612( 
  TA200261001 VARCHAR(20)
 ,TA200261004 VARCHAR(30)
 ,TA200261009 VARCHAR(30)
) ;

CREATE TABLE EDIODS.GCC_ACR_CREDIT_LIST2( 
  CREDIT_OCCUPY_CINO VARCHAR(20)
 ,BUSI_BELONG_CINO VARCHAR(20)
 ,ASSURE_COEFF DECIMAL(10,4)
) ;

CREATE TABLE EDIODS.C03_CORP_LOAN_AGT( DATA_DT DATE 
 ,ORGANNO CHAR(10) 
 ,CINO VARCHAR(15) 
 ,Guar_Coef DECIMAL(9,6) 
 ,VALUEDAY DATE 
 ,Debt_Breach_Lossrate DECIMAL(7,4)
);

CREATE TABLE EDIODS.CCM_CM_NEW_SEQ( 
  ENTERPRISE_CODE VARCHAR(20)
 ,SEQ_CODE VARCHAR(20)
 ,OLD_SEQ VARCHAR(50)
 ,SEQ_NUMBER VARCHAR(20)
 ,Record_Del_Dt DATE
) ;


EXPLAIN
INSERT INTO EDIODS.C03_CORP_LOAN_AGT(
   ORGANNO
  ,CINO
  ,GUAR_COEF
)
SELECT
       T1.LOANNO
      ,COALESCE(T4.TA200261001,'')
      ,COALESCE(T7.ASSURE_COEFF,0)
FROM  EDIODS.MF1_NTHLNSUB2 T1
LEFT  JOIN EDIODS.CCM_TA2002612 T4
   ON T1.AGACCNO = SUBSTR(T4.TA200261004,1,17)
  AND T4.TA200261009 NOT IN (SELECT OLD_SEQ
                               FROM EDIODS.CCM_CM_NEW_SEQ
                              WHERE SEQ_CODE = 'TA200261009' )
LEFT JOIN (
                       SELECT BUSI_BELONG_CINO
                  ,ASSURE_COEFF
                       FROM
                   (SELECT BUSI_BELONG_CINO
                          ,ASSURE_COEFF
                          ,ROW_NUMBER() OVER (PARTITION BY BUSI_BELONG_CINO ORDER BY ASSURE_COEFF desc) AS QUA_ROW_NUM_1
                   FROM EDIODS.GCC_ACR_CREDIT_LIST2
                   ) AA WHERE QUA_ROW_NUM_1 = 1
          ) T7
  ON T4.TA200261001 = T7.BUSI_BELONG_CINO;

drop schema ediods cascade;

---
--Test early free for sublinks
---
create table location_type
(
    location_type_cd varchar(50) not null ,
    location_type_desc varchar(250) not null
)with (orientation=column)
 ;

drop table if exists item_inventory_plan cascade;
create table item_inventory_plan
(
    item_inventory_plan_dt date not null ,
    location_id number(35,0) not null ,
    item_id number(20,5) not null ,
    plan_on_hand_qty decimal(18,4) null,
    plan_on_hand_retail_amt number(18,4) null,
partial cluster key(item_inventory_plan_dt,location_id,item_id,plan_on_hand_retail_amt)
)with (orientation=column)   ;

drop table if exists item_inventory cascade;
create table item_inventory
(
    LOCATION_ID number(15,0) NOT NULL ,
    ITEM_INV_DT DATE NOT NULL ,
    ITEM_ID number(38,5) NOT NULL ,
    ON_HAND_UNIT_QTY DECIMAL(38,10) NOT NULL ,
    ON_HAND_AT_RETAIL_AMT DECIMAL(38,14) NOT NULL ,
    ON_HAND_COST_AMT NUMBER(38,24) NOT NULL ,
    ON_ORDER_QTY NUMBER(38,34) NULL ,
    LOST_SALES_DAY_IND CHAR(3) NULL ,
partial cluster key(LOCATION_ID,ON_HAND_COST_AMT,ON_ORDER_QTY)
) with (orientation=column)  ;

INSERT INTO LOCATION_TYPE VALUES ('A', 'A');
INSERT INTO LOCATION_TYPE VALUES ('B', 'B');
INSERT INTO LOCATION_TYPE VALUES ('C', 'SHANGHAI');
INSERT INTO LOCATION_TYPE VALUES ('D', 'A');
INSERT INTO LOCATION_TYPE VALUES ('E', 'E');
INSERT INTO LOCATION_TYPE VALUES ('F', ' T');

INSERT INTO ITEM_INVENTORY_PLAN VALUES (DATE '1970-01-01', 1, 0.12, 0.30 , NULL);
INSERT INTO ITEM_INVENTORY_PLAN VALUES (DATE '1973-01-01', 1, 0.12, NULL, 1.0);
INSERT INTO ITEM_INVENTORY_PLAN VALUES (DATE '1976-01-01', 2, 1.3, 2.0 , 2.0);
INSERT INTO ITEM_INVENTORY_PLAN VALUES (DATE '1979-01-01', 3, 5.01, 3.0 , 3.0);
INSERT INTO ITEM_INVENTORY_PLAN VALUES (DATE '1982-01-01', 4, 5.01, 4.0 , NULL);
INSERT INTO ITEM_INVENTORY_PLAN VALUES (DATE '1985-01-01', 5, 5.01, NULL, 5.0);
INSERT INTO ITEM_INVENTORY_PLAN VALUES (DATE '1988-01-01', 5, 9.12, 6.0 , 6.0);

INSERT INTO ITEM_INVENTORY VALUES ( 38, DATE '1970-01-01',  0.12, 0.70, 0878.0, 0.70, NULL, 'A');
INSERT INTO ITEM_INVENTORY VALUES ( 1, DATE '1973-01-01',  1.3, 178787.0, 1.0, 1787.0, 1.0 , NULL);
INSERT INTO ITEM_INVENTORY VALUES ( 2, DATE '1976-01-01',  2.23, 2.0, 2.0, 2787.0, 2.0 , 'C');
INSERT INTO ITEM_INVENTORY VALUES ( 3, DATE '1979-01-01',  3.33, 3.0, 3676.0, 3.0, 3.0 , '  D');
INSERT INTO ITEM_INVENTORY VALUES ( 4, DATE '1982-01-01',  4.98, 4.0, 4.6760, 4.0, 4.0 , 'E');
INSERT INTO ITEM_INVENTORY VALUES ( 5, DATE '1985-01-01',  5.01, 5.0, 5.0, 5787.0, NULL, ' F');

create or replace view usview03 as
    select location_id, item_inv_dt
    from item_inventory
    union all
    select location_id, item_inventory_plan_dt
    from item_inventory_plan ;

analyze location_type;
analyze ITEM_INVENTORY_PLAN;
analyze ITEM_INVENTORY;

SELECT 1
FROM location_type
WHERE location_type_desc NOT IN (
SELECT 'F'
FROM location_type
WHERE ('B') IN (
SELECT location_type_desc
FROM usview03 , location_type
WHERE (item_inv_dt) NOT IN (
SELECT location_type_desc
FROM location_type , usview03
WHERE location_type_desc LIKE '%Y'
AND location_type_desc NOT LIKE 'T_')
INTERSECT
SELECT location_type_cd FROM location_type
UNION ALL
SELECT location_type_cd FROM location_type
WHERE location_type_desc = location_type_cd)
INTERSECT
SELECT 'z'
FROM location_type
WHERE 1=0);


drop table if exists location_type cascade;
drop table if exists item_inventory_plan cascade;
drop table if exists item_inventory cascade;
drop  view usview03;

