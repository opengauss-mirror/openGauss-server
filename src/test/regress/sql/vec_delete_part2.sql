/*
 * This file is used to test the function of ExecVecUnique()
 */
----
--- Create Table and Insert Data
----
create schema vector_delete_engine_part2;
set current_schema = vector_delete_engine_part2;

create table vector_delete_engine_part2.VECTOR_DELETE_TABLE_02
(
   col_int	int
  ,col_date	date
)with(orientation=column)  ;

create table vector_delete_engine_part2.VECTOR_DELETE_TABLE_03
(
   col_int	int
  ,col_date	date
)with(orientation=column)  ;

COPY VECTOR_DELETE_TABLE_02(col_int, col_date) FROM stdin;
1	2015-02-26
\.

COPY VECTOR_DELETE_TABLE_03(col_int, col_date) FROM stdin;
2	2015-02-26
1	2015-02-26
2	2015-01-26
\.

analyze vector_delete_table_02;
analyze vector_delete_table_03;

----
--- case 4: non-correlated subquery
----
explain (verbose on, costs off) delete from vector_delete_table_03 where exists(select col_date from vector_delete_table_02);
delete from vector_delete_table_03 where exists(select col_date from vector_delete_table_02);


--test for modify table
create table sales_transaction(
sales_tran_id number(18,10) null,
visit_id number(18,10) null,
tran_status_cd varchar(100) null,
reported_as_dttm timestamp null,
tran_type_cd text null,
mkb_cost_amt number(18,0) null,
mkb_item_qty bigserial not null,
mkb_int_unique_items_qty number(18,6) null,
mkb_rev_amt number(18,0) null,
associate_party_id decimal null,
tran_start_dttm_dd date null,
tran_date timestamp(6) null,
tran_end_dttm_dd timestamp(6) null,
tran_end_hour char(40) null,
tran_end_minute decimal null,
reward_cd char(100) null
)with (orientation=column)
 
;

create table sales_transaction_line(
sales_tran_id decimal null,
sales_tran_line_num char(40) null,
item_id smallserial not null,
item_qty serial not null,
unit_selling_price_amt bigserial not null,
unit_cost_amt clob null,
tran_line_status_cd varchar(50) null,
sales_tran_line_start_dttm date null,
tran_line_sales_type_cd varchar(100) null,
sales_tran_line_end_dttm timestamp null,
tran_line_date date null,
tx_time number null,
location decimal null
)with (orientation=column)
 
;

create table usview19(
sales_tran_id serial not null,
visit_id number(18,0) null,
tran_date timestamp null
)with (orientation=row)
 
;

set enable_force_vector_engine = on;
explain (costs off) 
insert into usview19
    select sales_tran_id,visit_id,tran_date
    from sales_transaction s
    where exists
    (
        select *
        from sales_transaction_line st
        where st.sales_tran_id = s. sales_tran_id
            and unit_selling_price_amt <= unit_cost_amt
            and unit_cost_amt > 5
    )
    or exists
    (
        select *
        from sales_transaction_line st
        where st.sales_tran_id = s. sales_tran_id
            and item_qty > 3.5
    );

insert into usview19
    select sales_tran_id,visit_id,tran_date
    from sales_transaction s
    where exists
    (
        select *
        from sales_transaction_line st
        where st.sales_tran_id = s. sales_tran_id
            and unit_selling_price_amt <= unit_cost_amt
            and unit_cost_amt > 5
    )
    or exists
    (
        select *
        from sales_transaction_line st
        where st.sales_tran_id = s. sales_tran_id
            and item_qty > 3.5
    );


----
--- Clean Table and Resource
----
drop schema vector_delete_engine_part2 cascade;
