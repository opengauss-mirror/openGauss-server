create schema aggregate;
set current_schema='aggregate';
create table t1 (a int , b int);
insert into t1 values(1,2);

explain (costs off)
select count(*) from (
  select row_number() over(partition by a, b) as rn,
      first_value(a) over(partition by b, a) as fv,
      * from t1
  )
where rn = 1;
set qrw_inlist2join_optmode = 'disable';
explain  (costs off)
select count(*) from (
  select row_number() over(partition by a, b) as rn,
      first_value(a) over(partition by b, a) as fv,
      * from t1
  )
where rn = 1;
reset qrw_inlist2join_optmode;

set enable_hashagg = off;
--force hash agg, if used sort agg will report error.
select a , count(distinct  generate_series(1,2)) from t1 group by a;
explain (verbose, costs off)
select a , count(distinct  generate_series(1,2)) from t1 group by a;
set query_dop = 2;
select a , count(distinct  generate_series(1,2)) from t1 group by a;
reset query_dop;

--test const-false agg
CREATE TABLE bmsql_item (
i_id int4 NoT NULL,i_name varchar(24),i_price numeric(5,2),i_data varchar( 50),i_im_id int4,
coNSTRAINT bmsql_item_pkey PRIMARY KEY (i_id)
);
insert into bmsql_item values ('1','sqltest_varchar_1','0.01','sqltest_varchar_1','1');
insert into bmsql_item values ('2','sqltest_varchar_2','0.02','sqltest_varchar_2','2');
insert into bmsql_item values ('3','sqltest_varchar_3','0.03','sqltest_varchar_3','3');
insert into bmsql_item values ('4','sqltest_varchar_4','0.04','sqltest_varchar_4','4');
insert into bmsql_item values ('5');

CREATE TABLE bmsql_new_order (
no_w_id int4 NOT NULL,
no_d_id int4 NOT NULL,no_o_id int4 NOT NULL
);

insert into bmsql_new_order values('1','1','1');
insert into bmsql_new_order values('2','2','2');
insert into bmsql_new_order values('3','3','3');
insert into bmsql_new_order values('4','4','4');
insert into bmsql_new_order values('5','5','5');

SELECT
   (avg(alias24.alias17)>2) AS alias32
FROM

    bmsql_item,
    (
        SELECT alias12.alias5 AS alias17
		FROM ( SELECT sin(bmsql_new_order.no_o_id)alias5 FROM bmsql_new_order )alias12
	)alias24
	GROUP BY bmsql_item.i_im_id HAVING 1>2
UNION
SELECT TRUE FROM  bmsql_item;

explain (verbose,costs off)
SELECT
   (avg(alias24.alias17)>2) AS alias32
FROM

    bmsql_item,
    (
        SELECT alias12.alias5 AS alias17
		FROM ( SELECT sin(bmsql_new_order.no_o_id)alias5 FROM bmsql_new_order )alias12
	)alias24
	GROUP BY bmsql_item.i_im_id HAVING 1>2
UNION
SELECT TRUE FROM  bmsql_item;

create table test_agg_false(a int, b varchar(20),c text, d numeric(5,2));
explain (verbose ,costs off) select sum(a),sum(b) , d from test_agg_false where 0=1 group by d;
select sum(a),sum(b) , d from test_agg_false where 0=1 group by d;

explain (verbose, costs off) select sum(a)+sum(b) , d from test_agg_false where 0=1 group by d;
select sum(a)+sum(b) , d from test_agg_false where 0=1 group by d;

explain (verbose, costs off) select sin(sum(a)+sum(b)) , d from test_agg_false where 0=1 group by d;
select sin(sum(a)+sum(b)) , d from test_agg_false where 0=1 group by d;

explain (verbose ,costs off) select sum(a)+sum(b) , d , 1 from test_agg_false where 0=1 group by d;
select sum(a)+sum(b) , d ,1 from test_agg_false where 0=1 group by d;

drop table t1;
drop schema aggregate CASCADE;
