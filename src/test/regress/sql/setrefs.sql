create table associate_benefit_expense
(
period_end_dt           date          ,
associate_expns_type_cd text
)with(orientation=column);


CREATE  TABLE offers_20050701 (
promo_id text,
party_firstname character varying(50),
party_lastname character varying(50)
)WITH (orientation=column);

WITH WITH_001 AS (
    SELECT CAST(associate_expns_type_cd AS varchar) c1
    FROM associate_benefit_expense
    GROUP BY CUBE(c1))
SELECT PARTY_FIRSTNAME
FROM WITH_001,offers_20050701
WHERE WITH_001.c1 LIKE '%c_'
GROUP BY ROLLUP(PARTY_FIRSTNAME);

explain  (costs off, verbose on)
WITH WITH_001 AS materialized (
    SELECT CAST(associate_expns_type_cd AS varchar) c1
    FROM associate_benefit_expense
    GROUP BY CUBE(c1))
SELECT PARTY_FIRSTNAME
FROM WITH_001,offers_20050701
WHERE WITH_001.c1 LIKE '%c_'
GROUP BY ROLLUP(PARTY_FIRSTNAME);

explain  (costs off, verbose on)
WITH WITH_001 AS (
    SELECT CAST(associate_expns_type_cd AS varchar) c1
    FROM associate_benefit_expense
    GROUP BY CUBE(c1))
SELECT PARTY_FIRSTNAME
FROM WITH_001,offers_20050701
WHERE WITH_001.c1 LIKE '%c_'
GROUP BY ROLLUP(PARTY_FIRSTNAME);

drop table if exists associate_benefit_expense,offers_20050701;

create table position_grade(
position_grade_cd varchar(50) ,
position_grade_desc   text
);
;

create table sales_transaction_line(
sales_tran_id int ,
item_id varchar(40)
);
;

explain (verbose on, costs off)
MERGE INTO position_grade t1
USING (
SELECT ITEM_ID c2
FROM sales_transaction_line ) t2
ON ( t2.c2 = t1.position_grade_cd )
WHEN NOT MATCHED THEN INSERT VALUES ( t2.c2,t2.c2 );

MERGE INTO position_grade t1
USING (
SELECT ITEM_ID c2
FROM sales_transaction_line ) t2
ON ( t2.c2 = t1.position_grade_cd )
WHEN NOT MATCHED THEN INSERT VALUES ( t2.c2,t2.c2 );

explain (verbose on, costs off)
insert into position_grade select t2.c2,t2.c2 from position_grade t1 left join (SELECT ITEM_ID c2
FROM sales_transaction_line ) t2 ON ( t2.c2 = t1.position_grade_cd );

insert into position_grade select t2.c2,t2.c2 from position_grade t1 left join (SELECT ITEM_ID c2
FROM sales_transaction_line ) t2 ON ( t2.c2 = t1.position_grade_cd );

drop table position_grade;
drop table sales_transaction_line;

create table merge_sort_t1(a int, b varchar) ;
create table merge_sort_t2(c int, d char(10));

insert into merge_sort_t1 values(1, ' abc ');
insert into merge_sort_t1 values(1, 'abc ');
insert into merge_sort_t2 values(1, ' abc');
insert into merge_sort_t2 values(1, ' abc');

set enable_hashjoin=off;
set enable_nestloop=off;
set enable_mergejoin=on;
explain (verbose on, costs off)
select count(*) from merge_sort_t1 t1 join merge_sort_t2 t2 on t1.b=t2.d where t1.a=1;

select count(*) from merge_sort_t1 t1 join merge_sort_t2 t2 on t1.b=t2.d where t1.a=1;

drop table merge_sort_t1;
drop table merge_sort_t2;
