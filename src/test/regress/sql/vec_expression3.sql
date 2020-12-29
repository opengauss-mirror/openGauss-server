/*
 * This file is used to test the function of vecexpression.cpp --- test(3)
 */
/*******************************
Expression Type:
	T_Var,
	T_Const,
	T_Param,
	T_Aggref,
	T_WindowFunc,
	T_ArrayRef,
	T_FuncExpr,
	T_NamedArgExpr,
	T_OpExpr,
	T_DistinctExpr,
	T_NullIfExpr,
	T_ScalarArrayOpExpr,
	T_BoolExpr,
	T_SubLink,
	T_SubPlan,
	T_AlternativeSubPlan,
	T_FieldSelect,
	T_FieldStore,
	T_RelabelType,
	T_CoerceViaIO,
	T_ArrayCoerceExpr,
	T_ConvertRowtypeExpr,
	T_CollateExpr,
	T_CaseExpr,
	T_CaseWhen,
	T_CaseTestExpr,
	T_ArrayExpr,
	T_RowExpr,
	T_RowCompareExpr,
	T_CoalesceExpr,
	T_MinMaxExpr,
	T_XmlExpr,
	T_NullTest,
	T_BooleanTest
	
Using Type:
	qual
	targetlist
*********************************/

----
--- test 16: tidout for column table
----
CREATE TABLE tidout_table(a INT) WITH (ORIENTATION=COLUMN);
INSERT INTO tidout_table VALUES(1);
SELECT ctid FROM tidout_table;
SELECT ctid::TEXT FROM tidout_table;
SELECT ctid||'aa' FROM tidout_table;
SELECT ctid||a FROM tidout_table;
SELECT ctid,a FROM tidout_table;
DROP TABLE tidout_table;

----
--- case ICBC: Special Case
----
SELECT Agt_Num,Agt_Modif_Num, Party_id, Int_Org_Num, Curr_Cd, Open_Dt, avgbal
FROM
(SELECT
   T1.Agt_Num
   ,T1.Agt_Modif_Num
   ,T1.Party_id
   ,CASE WHEN T1.Proc_Org_Num <> '' AND SUBSTR(T1.Proc_Org_Num,9,4) NOT IN ('0000','9999')
           THEN T1.Proc_Org_Num 
           ELSE T1.Int_Org_Num 
    END AS Int_Org_Num
   ,T1.Curr_Cd
   ,T1.Open_Dt
   ,CAST(T1.Year_Dpsit_Accum/(TO_DATE('20140825', 'YYYYMMDD')-TO_DATE('20131231', 'YYYYMMDD')) AS DECIMAL(18,2)) AS avgbal
FROM dwSumData_act.C03_SEMI_CRDT_CARD_ACCT T1
WHERE T1.Data_Dt<=TO_DATE('20140825', 'YYYYMMDD')
   AND T1.Data_Dt>=TO_DATE('20140101', 'YYYYMMDD')
   AND T1.Party_Class_Cd=0
) A  order by 1,2,3,4,5,6,7;
SELECT Agt_Num,Agt_Modif_Num, Party_id, Int_Org_Num, Curr_Cd, Open_Dt, avgbal
FROM
(SELECT
   T1.Agt_Num
   ,T1.Agt_Modif_Num
   ,T1.Party_id
   ,CASE WHEN T1.Proc_Org_Num <> '' AND SUBSTR(T1.Proc_Org_Num,9,4) NOT IN ('0000','9999')
           THEN T1.Proc_Org_Num 
           ELSE T1.Int_Org_Num 
    END AS Int_Org_Num
   ,T1.Curr_Cd
   ,T1.Open_Dt
   ,CAST(T1.Year_Dpsit_Accum/(TO_DATE('20140825', 'YYYYMMDD')-TO_DATE('20131231', 'YYYYMMDD')) AS DECIMAL(18,2)) AS avgbal
   ,ROW_NUMBER() OVER(PARTITION BY T1.Agt_Num,T1.Agt_Modif_Num ORDER BY T1.Data_Dt DESC)  AS Agt_Num_ORDER
FROM dwSumData_act.C03_SEMI_CRDT_CARD_ACCT T1
WHERE T1.Data_Dt<=TO_DATE('20140825', 'YYYYMMDD')
   AND T1.Data_Dt>=TO_DATE('20140101', 'YYYYMMDD')
   AND T1.Party_Class_Cd=0
) A WHERE Agt_Num_ORDER = 1 order by 1,2,3,4,5,6,7;  



create table tr_case(
rn bigint,
c1 character varying(60),
c2 character varying(60),
c3 date,
c4 character varying(60),
c5 date,
c6 character varying(60)
);
insert into tr_case values(299295,'2','99991231','2014-10-22','00000000000000001',null,'1');
insert into tr_case values(299296,'2','99991231','2014-10-22','00000000000000001',null,'1');
insert into tr_case values(299294,'2','99991231','2014-10-22','00000000000000001',null,'1');
insert into tr_case values(299290,'2','99991231','2014-10-22','00000000000000001',null,'1');


create table tc_case with (orientation = column) as select * from tr_case;

select case when
    (case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2) and (case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1)  
	     then c3 
		 else '19000102' end from tc_case;

select rn, c1,c2,c3 ,c4,c5,c6,
case when (case when c1 = '' then 0  else cast(c1 as decimal(20,0)) end = 1 ) 
     then cast(c2 as date)
     when (case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2) and (case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1) 
     then c3 + case when c4 = '' then 0 else cast(c4 as decimal (17,0)) end
     when (case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 2)
     then coalesce(c5, cast('19000102' as date))
     else cast('19000102' as date)
end
from tc_case order by 1;

select rn, c1,c2,c3 ,c4,c5,c6,
case when case when c1='' then 0 else cast(c1 as decimal(20,0)) end  = 1 then cast(c2 as date)
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1
  then c3 + case when c4 = '' then 0 else cast(c4 as decimal (17,0)) end
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 2
  then coalesce(c5, cast('19000102' as date))
 else cast('19000102' as date)
end
from tc_case 
minus all
select rn, c1,c2,c3 ,c4,c5,c6,
case when case when c1='' then 0 else cast(c1 as decimal(20,0)) end  = 1 then cast(c2 as date)
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1
  then c3 + case when c4 = '' then 0 else cast(c4 as decimal (17,0)) end
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 2
  then coalesce(c5, cast('19000102' as date))
 else cast('19000102' as date)
end
from tr_case order by 1;

select case when
    (case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2) and (case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1)  
	     then c3 
		 else '19000102' end from tc_case;

select rn, c1,c2,c3 ,c4,c5,c6,
case when (case when c1 = '' then 0  else cast(c1 as decimal(20,0)) end = 1 ) 
     then cast(c2 as date)
     when (case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2) and (case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1) 
     then c3 + case when c4 = '' then 0 else cast(c4 as decimal (17,0)) end
     when (case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 2)
     then coalesce(c5, cast('19000102' as date))
     else cast('19000102' as date)
end
from tc_case order by 1;

select rn, c1,c2,c3 ,c4,c5,c6,
case when case when c1='' then 0 else cast(c1 as decimal(20,0)) end  = 1 then cast(c2 as date)
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1
  then c3 + case when c4 = '' then 0 else cast(c4 as decimal (17,0)) end
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 2
  then coalesce(c5, cast('19000102' as date))
 else cast('19000102' as date)
end
from tc_case 
minus all
select rn, c1,c2,c3 ,c4,c5,c6,
case when case when c1='' then 0 else cast(c1 as decimal(20,0)) end  = 1 then cast(c2 as date)
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 1
  then c3 + case when c4 = '' then 0 else cast(c4 as decimal (17,0)) end
 when case when c1 = '' then 0 else cast(c1 as decimal(20,0)) end = 2 and case when c6 = '' then 0 else cast(c6 as decimal(20,0)) end = 2
  then coalesce(c5, cast('19000102' as date))
 else cast('19000102' as date)
end
from tr_case order by 1;

----
--- Clean Resource and Tables
----
CREATE TABLE t1_case_col
(
      TRANSACTION_ID           CHAR(6)         
      ,AVG_CPU                 DECIMAL(12,4)   
      ,TRANNUM                 DECIMAL(10)     
      ,SUM_CPU                 DECIMAL(12,3)   
      ,DATA_DT                 CHAR(8)         
)
with(orientation=column)
DISTRIBUTE BY HASH (TRANSACTION_ID)
;
insert into t1_case_col values('999999',  0, 0, 0, '20150317');

SELECT (SUM( CASE WHEN TRANSACTION_ID = '999999'
              THEN SUM_CPU
              ELSE 0
              END               
            )/
         CASE  WHEN
         SUM( CASE WHEN TRANSACTION_ID <> '999999'
                   THEN TRANNUM
                   ELSE 0
              END
             ) = 0               
         THEN 99999999999999999   
         ELSE SUM( CASE WHEN TRANSACTION_ID <> '999999'
                        THEN TRANNUM
                        ELSE 0
                   END
                  )
         END )   AS WEIGHT          
  FROM t1_case_col;
drop table t1_case_col;

CREATE TABLE t1_hashConst_col(col_1 int, col_2 int) with(orientation=column);
insert into t1_hashConst_col values(generate_series(1, 100), generate_series(1, 100));
select col_1, case when col_1 >= 10 then not col_2 when col_1 < 10 then not col_1 end from  t1_hashConst_col order by 1;
drop table t1_hashConst_col;

create table t1_caseAnd_col(col_1 int, col_2 int, col_3 bool, col_4 bool)with(orientation=column);
copy t1_caseAnd_col FROM stdin;
0	2	0	0
0	5	1	1
0	5	1	1
\.
select col_1, col_2, col_3, col_4, (case when (col_1 = 0 and col_2 = 5) then (col_3 and col_4) when (col_1 = 0 and col_2 = 2) then (col_3 and col_4) else 0::bool end) from t1_caseAnd_col order by 1, 2;
drop table t1_caseAnd_col;


create table vector_expr_table_23(a int, b varchar(10), c text)with(orientation=column);
copy vector_expr_table_23 from stdin;
1	123	\N
1	123	456
1	2	28
\.

select * from vector_expr_table_23 where NULLIF(b, 3) < 9 OR Coalesce(c, '1') < 5000 order by 1,2,3;

execute direct on (datanode8) 'select concat(b,b), concat(c,c,c) from vector_expr_table_23 order by 1,2;';


create table customer_info_col 
(   ca_address integer not null,
    ca_id varchar(20) ,
    ca_email varchar(50)
)with (orientation=column) distribute by hash (ca_address);

create table date_text_col
(   d_text varchar(2000),
    d_cost int null
)with (orientation=column) distribute by hash (d_text);

insert into customer_info_col values (49597, '111111111111111111', 'aaaa' );
insert into customer_info_col values (49619, '222222222222222222', null );

insert into date_text_col values ('abcdefghijklmnopqrst', 15);
insert into date_text_col values ('abcdefghijklmnopqrst', null);

select t2.d_text,
    length(t1.ca_id) as start,
    length(t1.ca_email) as len,
    substr(t2.d_text,length(t1.ca_id),length(t1.ca_email)) as vtext_sub
from customer_info_col as t1 , date_text_col as t2 order by 1, 2, 3, 4;

select d_cost, to_hex(d_cost) from date_text_col, customer_info_col order by 1, 2;

CREATE TABLE item_less_1 (
    i_item_sk integer NOT NULL,
    i_item_id character(16) NOT NULL,
    i_rec_start_date timestamp(0) without time zone,
    i_rec_end_date timestamp(0) without time zone,
    i_item_desc character varying(200),
    i_current_price numeric(7,2),
    i_wholesale_cost numeric(20,6),
    i_brand_id integer,
    i_brand character(50),
    i_class_id integer,
    i_class character(50),
    i_category_id integer,
    i_category character(50),
    i_manufact_id integer,
    i_manufact character(50),
    i_size numeric(20,2),
    i_formulation character(50),
    i_color numeric(19,18),
    i_units numeric(19,0),
    i_container numeric(39,2),
    i_manager_id integer,
    i_product_name character(50)
)
WITH (orientation=column, compression=low)
DISTRIBUTE BY REPLICATION;

COPY item_less_1 (i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name) FROM stdin;
60	AAAAAAAAKDAAAAAA	2001-10-27 00:00:00	\N	Cuts may hold already; daughters can like exclusively pregnant, fresh police; actual,	0.00	3243655.503585	3004002	maxicorp #5                                       	9	womens watch                                      	6	Jewelry                                           	849	85.37                                             	0.00	\N	\N	60	898.40	19	barcally
\.

CREATE TABLE catalog_sales_less (
    cs_sold_date_sk integer,
    cs_sold_time_sk integer,
    cs_ship_date_sk integer,
    cs_bill_customer_sk character varying,
    cs_bill_cdemo_sk numeric(30,20),
    cs_bill_hdemo_sk character varying(19),
    cs_bill_addr_sk character varying(39),
    cs_ship_customer_sk bigint,
    cs_ship_cdemo_sk numeric(10,2),
    cs_ship_hdemo_sk numeric(20,7),
    cs_ship_addr_sk numeric(10,0),
    cs_call_center_sk double precision,
    cs_catalog_page_sk numeric(19,0),
    cs_ship_mode_sk numeric(20,18),
    cs_warehouse_sk numeric(30,19),
    cs_item_sk integer NOT NULL,
    cs_promo_sk bigint,
    cs_order_number bigint NOT NULL,
    cs_quantity text,
    cs_wholesale_cost numeric(7,2),
    cs_list_price numeric(15,4),
    cs_sales_price character varying(30),
    cs_ext_discount_amt numeric(15,2),
    cs_ext_sales_price numeric(13,6),
    cs_ext_wholesale_cost double precision,
    cs_ext_list_price numeric(7,2),
    cs_ext_tax numeric(36,18),
    cs_coupon_amt numeric(27,8),
    cs_ext_ship_cost numeric(20,4),
    cs_net_paid numeric(7,2),
    cs_net_paid_inc_tax numeric(20,5),
    cs_net_paid_inc_ship numeric(25,5),
    cs_net_paid_inc_ship_tax numeric(18,9),
    cs_net_profit numeric(30,20)
)
WITH (orientation=column, compression=low)
DISTRIBUTE BY HASH (cs_ext_tax, cs_coupon_amt, cs_net_paid_inc_tax)
PARTITION BY RANGE (cs_net_paid_inc_tax)
(
    PARTITION p1 VALUES LESS THAN (-3768.01),
    PARTITION p2 VALUES LESS THAN (-603.67),
    PARTITION p3 VALUES LESS THAN (-339.35),
    PARTITION p4 VALUES LESS THAN (-46.92),
    PARTITION p5 VALUES LESS THAN (84.24),
    PARTITION p6 VALUES LESS THAN (1098.66),
    PARTITION p7 VALUES LESS THAN (MAXVALUE)
)
ENABLE ROW MOVEMENT;

COPY catalog_sales_less (cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit) FROM stdin;
2451114	34062	2451155	9512	1743406.00000000000000000000	2547	43641	9512	1743406.00	2547.0000000	43641	2	202	5.000000000000000000	3.0000000000000000000	550	67	20797	41	7.59	8.2700	5.70	105.37	233.700000	311.189999999999998	339.07	2.330000000000000000	0.00000000	77.9000	233.70	236.03000	311.60000	313.930000000	-77.49000000000000000000
\.

select cs_wholesale_cost, cs_ext_list_price, i_color, cs_coupon_amt, cs_net_paid_inc_tax, i_container, cs_bill_cdemo_sk from item_less_1 inner join catalog_sales_less on cs_wholesale_cost <> greatest(cs_ext_list_price * i_color, cs_coupon_amt / cs_net_paid_inc_tax) and i_container <> cs_bill_cdemo_sk;


drop table item_less_1;
drop table catalog_sales_less;

-- Both COALESCE and GREATEST Expression
create table test_coalesce_greatest (id int, val1 numeric, val2 numeric) with(orientation=column) distribute by hash(id);
insert into test_coalesce_greatest values (1, 1, NULL), (1, NULL, 2), (1, 3, NULL), (1, NULL, 4);
select id, coalesce(val1, greatest(val2)) from test_coalesce_greatest order by 1, 2;


