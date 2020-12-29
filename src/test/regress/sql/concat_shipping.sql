set current_schema='shipping_schema';

----row store table----
--case1: shipping--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t1,t1.c) from shipping_test_row t1, shipping_test_row t2 where t1.b=t1.b;
--case2: function and cast nested shipping--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t2.a,'--',min((t1.b::numeric))) from shipping_test_row t1, shipping_test_row t2 where t1.b=t1.b group by t2.a;
--ArrayExpr--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(array[t1.a,t1.b],t2.a) from shipping_test_row t1, shipping_test_row t2 where t1.a=t1.a;
--function nested--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t2.a,'--',min(array[t1.a,t1.b])) from shipping_test_row t1, shipping_test_row t2 where t1.b=t1.b group by t2.a;
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t2.a,'--','t2.b',concat(concat((array[t1.a,t1.b]),t1.a),'XXX'),'t2.b') from shipping_test_row t1, shipping_test_row t2 where t1.b=t1.b group by t1.a,t1.b,t2.a;
--function nested and ArrayCoerExpr--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t2.a,'--',min(t1.d::float[])) from shipping_test_row t1, shipping_test_row t2 where t1.b=t1.b group by t2.a;

explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t2.a,'--',min(array[t1.a,t1.b])) from shipping_test_row t1, shipping_test_row t2 where t1.b=t1.b group by t2.a;

--col store table--
--case1: shipping--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t2.a,'--',min((t1.b::numeric))) from shipping_test_col t1, shipping_test_col t2 where t1.b=t1.b group by t2.a;
--RowExpr--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t1,t1.c) from shipping_test_col t1, shipping_test_col t2 where t1.b=t1.b;
--function nested and ArrayCoerExpr--
explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select concat(t2.a,'--',min(array[t1.a,t1.b])) from shipping_test_col t1, shipping_test_col t2 where t1.b=t1.b group by t2.a;

explain (analyze off ,verbose off ,costs off ,cpu off, detail off, num_buffers off, timing off)
select * from (select concat(t1.acct_id,t2.acct_id)as acct_id from  t1
full outer join t2 on t1.acct_id=t2.acct_id ) A full join t3 on a.acct_id=t3.acct_id
full join t1 on concat(a.acct_id,t3.acct_id)=t1.acct_id;


