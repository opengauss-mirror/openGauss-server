/*
################################################################################
# TESTCASE NAME : skew_hint_01.py 
# COMPONENT(S)  : skew hint功能测试: skew hint DFX测试
# PREREQUISITE  : skew_setup.py
# PLATFORM      : all
# DESCRIPTION   : skew hint
# TAG           : hint
# TC LEVEL      : Level 1
################################################################################
*/

--I1.设置guc参数
--S1.设置schema
set current_schema = skew_hint;
--S1.关闭sort agg和nestloop
set enable_sort = off;
set enable_nestloop = off;
--S2.关闭query下推
--S3.设置计划格式
set explain_perf_mode = normal;
--S3.设置query_dop使得explain中倾斜优化生效
set query_dop = 1002;

--I2.完整性测试
--S1.正确示例
explain(verbose on, costs off) select /*+ skew(skew_t1 (b) (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S2.表缺省
explain(verbose on, costs off) select /*+ skew(b (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S3.列缺省
explain(verbose on, costs off) select /*+ skew(skew_t1 (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S3.值缺省
explain(verbose on, costs off) select /*+ skew(skew_t1 (b)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;

--I3.格式错误测试
--S1.正确示例
explain(verbose on, costs off) select /*+ skew(skew_t1 (b) (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S2.多表无括号
explain(verbose on, costs off) select /*+ skew(skew_t1 t2 (b) (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S3.列无括号
explain(verbose on, costs off) select /*+ skew(skew_t1 b (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S4.值无括号
explain(verbose on, costs off) select /*+ skew(skew_t1 (b) 10) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;

--I4.重复hint测试
--S1.完全相同
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (10)) skew(skew_t1 (a) (10))*/ * from  skew_t1 join hint.hint_t1 on skew_t1.a =  hint.hint_t1.a;
--S2.表相同，列存在包含情况
explain(verbose on, costs off) select /*+ skew(skew_t1 (a) (10)) skew(skew_t1 (a b) (10 10))*/ * from  skew_t1 join hint.hint_t1 on skew_t1.a =  hint.hint_t1.a;
--S3.不同层则不算重复:不提升可能出现hint未使用
explain(verbose on, costs off) select /*+ skew(s (b) (10))*/ * from  skew_t1 s join (select /*+ skew(s (b) (10)) */count(*) as a from skew_t1 s, skew_t2 t2 where s.b = t2.c)tp(a) on s.b = tp.a; 
--S4.不同层，不提升出现同名hint未用时，可以通过修改别名判断是哪个未用
explain(verbose on, costs off) select /*+ skew(s (b) (10))*/ * from  skew_t1 s join (select /*+ skew(ss (b) (10)) */count(*) as a from skew_t1 ss, skew_t2 t2 where ss.b = t2.c)tp(a) on s.b = tp.a;
--S5.不同层，提升后，无论hint指定的别名表实际上是否相同，都会出现：relation name "xx" is ambiguous的提示
--实际表相同
explain(verbose on, costs off) select /*+ skew(s (b) (10))*/ * from  skew_t1 s join (select /*+ skew(s (b) (10)) */s.a as sa from skew_t1 s, skew_t2 t2 where s.b = t2.c)tp(a) on s.b = tp.a;
--实际表不同
explain(verbose on, costs off) select /*+ skew(s (b) (10))*/ * from  skew_t1 s join (select /*+ skew(s (b) (10)) */s.a as sa from skew_t2 s, skew_t3 t3 where s.b = t3.c)tp(a) on s.b = tp.a;
--S6.不同层，提升后，可以通过修改别名避免出现S5中的错误
explain(verbose on, costs off) select /*+ skew(s (b) (10))*/ * from  skew_t1 s join (select /*+ skew(ss (b) (10)) */ss.a as sa from skew_t1 ss, skew_t2 t2 where ss.b = t2.c)tp(a) on s.b = tp.a; 
explain(verbose on, costs off) select /*+ skew(s (b) (10))*/ * from  skew_t1 s join (select /*+ skew(ss (b) (10)) */ss.a as sa from skew_t2 ss, skew_t3 t3 where ss.b = t3.c)tp(a) on s.b = tp.a; 

--I5.表提示测试
--S1.表未使用别名
explain(verbose on, costs off) select /*+ skew(skew_t2 (b) (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S2.表在query中不存在
explain(verbose on, costs off) select /*+ skew(skew_t3 (b) (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S3.表名存在歧义
explain(verbose on, costs off) select /*+ skew(skew_t1 (b) (10)) */count(*) from skew_hint.skew_t1, hint.skew_t1 where skew_hint.skew_t1.b  = hint.skew_t1.a;
--S4.表为不支持类型

--I6.列提示测试
--S1.列找不到
explain(verbose on, costs off) select /*+ skew(skew_t1 (aa) (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;
--S2.列名存在歧义
explain(verbose on, costs off) select /*+ skew((skew_t1 t2) (a) (10)) */count(*) from skew_t1, skew_t2 t2 where skew_t1.b  = t2.a;

--I7.不支持重分布类型测试
--对于不支持重分布的类型，在hint中，应支持输入（不报语法错误），不支持解析（提示不能支持重分布）
--S1.money类型
explain(verbose on, costs off) select /*+ skew(typetest (col_money) ('59'))*/ * from  typetest join skew_t1 on col_integer = 10;
--S2.十六进制
create table hex_t(a int, b raw) distribute by hash(a);
insert into hex_t values(1,'7fffffff');
select * from hex_t;
analyze hex_t;
explain(verbose on, costs off) select /*+skew(hex_t(b)('7fffffff'))*/ * from hex_t, text_t where hex_t.a=text_t.a;
--S3.bool型
create table bool_t(a int,b bool) distribute by hash(a);
insert into bool_t values(1,true);
explain(verbose on, costs off) select /*+skew(bool_t(b)(false))*/ * from bool_t, text_t where text_t.a=text_t.a; 
--S4.位串
create table bit_t(a int, b bit(3)) distribute by hash(a);
insert into bit_t values(1,B'101');
select * from bit_t;
explain(verbose on, costs off) select /*+skew(bit_t(b)(B'101'))*/ * from bit_t, text_t where bit_t.a=text_t.a;

--I8.值无法转换为datum：通常出现在string类型的值未使用单引号场景或者值越界无法转换的场景
create table c_t(a int, b char) distribute by hash(a);
insert into c_t values(generate_series(1,10),1);
--S1.错误场景
explain(verbose on, costs off) select /*+ skew(c_t (b) (1)) */count(*) from char_t, c_t where char_t.c  = c_t.b;
--S2.正确场景
explain(verbose on, costs off) select /*+ skew(c_t (b) ('1')) */count(*) from char_t, c_t where char_t.c  = c_t.b;
--S3.值范围问题
explain(verbose on, costs off) select /*+ skew(c_t (a) (111111111111111111111111111111)) */count(*) from char_t, c_t where char_t.c  = c_t.b;
--S4.值与column数量不符
explain(verbose on, costs off) select /*+ skew(c_t (a b) (1 'a' 2)) */count(*) from char_t, c_t where char_t.c  = c_t.b;
--S5.单列倾斜值超过10个
explain(verbose on, costs off) select /*+ skew(c_t (a) (1 2 3 4 5 6 7 8 9 10 11)) */count(*) from char_t, c_t where char_t.c  = c_t.b;

--I9.Hint没有被使用时提示测试:unused hint
--S1.正确hint的指定
explain(verbose on, costs off) select /*+ skew(s (b) (10)) */count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;
--S2.列没有包含分布键
explain(verbose on, costs off) select /*+ skew(skew_t2 (a c) (1 1)) */ count(distinct a) from skew_t2 group by a,b;
--S3.hint指定有误:skew_t1表倾斜，却指定了skew_t2表。
explain(verbose on, costs off) select /*+ skew(t2 (b) (10)) */count(*) from skew_t1 s, skew_t2 t2 where s.b  = t2.c;


--I7.还原设置
--S1.还原query_dop
set query_dop = 2002;
drop table if exists warehouse;
create table warehouse
(
    w_warehouse_sk            numeric(100,4)                null,
    w_warehouse_id            char(16)              not null,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)
 )with(orientation = row,compression=no)  distribute by replication
 partition by range(w_warehouse_sk)
 (partition p1 values less than(maxvalue));

drop table if exists store_returns;

create table store_returns
(
    sr_returned_date_sk       integer(1000,99)  null ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          bigint               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)
 )with(orientation = row  ,compression=yes)
 distribute by hash (sr_returned_date_sk);


create index ss on store_returns(sr_returned_date_sk);

explain  select/*+hashjoin(warehouse store_returns) indexscan(store_returns ss)*/
 min( case when sr_return_time_sk-sr_item_sk>sr_item_sk
                 then sr_return_time_sk
                  when sr_return_time_sk-sr_item_sk <> sr_item_sk
                   then (case when sr_return_time_sk>sr_item_sk then sr_return_time_sk else sr_return_time_sk-1 end)
                    else  sr_item_sk end), count(distinct sr_item_sk )
from warehouse
 RIGHT join store_returns on sr_returned_date_sk=w_warehouse_sk
 group by  sr_returned_date_sk having
 count(distinct case when sr_returned_date_sk>sr_returned_date_sk-sr_item_sk then sr_item_sk else sr_item_sk+2 END)<>avg(sr_item_sk)
 and count(distinct sr_item_sk )<3;

