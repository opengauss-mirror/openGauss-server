# 谓词下推引起的查询报错

## 问题现象<a name="zh-cn_topic_0283137100_zh-cn_topic_0059778167_s7a2ed06fefd0448fae90f40fe4291f8d"></a>

计划中出现谓词下推时，按照SQL标准中的查询执行顺序本不应该报错，结果执行出错。

```
openGauss=# select * from tba;
 a 
---
 -1
  2
(2 rows)

openGauss=# select * from tbb;
 b 
---
 -1
  1
(2 rows)

openGauss=# select * from tba join tbb on a > b where b > 0 and sqrt(a) > 1;
ERROR: cannot table square root of a negative number
```

按照SQL执行标准流程:
1、执行FROM子句，能够保证所有数据满足`a > b`。
2、执行WHERE子句中`b > 0`，若结果为`true`则能够推导出`a > 0`，并继续执行；若`false`则结束，后面的条件被短路，不会执行。
3、执行WHERE子句中`sqrt(a) > 1`。

但是实际却报错入参为负值。

## 原因分析<a name="zh-cn_topic_0283137100_zh-cn_topic_0059778167_s74d2dfcb815b4d8ca504c549a923e5ed"></a>

```
openGauss=# explain (costs off) select * from tba join tbb on a > b where b > 0 and sqrt(a) > 1;
            QUERY PLAN            
----------------------------------
Nest loop
  Join Filter: (a > b)
  -> Seq Scan on public.tba
       Filter: (sqrt(a) > 1)
  -> Materialize
       -> Seq Scan on public.tbb
            Filter: (b > 0)
(7 rows)
```

分析计划可知，原本`a > b`, `b > 0`, `sqrt(a) > 1`的三个条件，被拆分下推到了不同的算子之中，从而并非按顺序执行.
且当前的等价类推理仅支持等号推理，因此无法自动推理补充出`a > 0`。
最终查询报错。

## 处理办法<a name="zh-cn_topic_0283137100_section485620163250"></a>

谓词下推可以极大的提升查询性能，且此种短路、推导的特殊场景，在大多数数据库优化器下都没有过多考虑，因此建议修改查询语句，在相关的条件下手动添加`a > 0`。

```
openGauss=# select * from tba join tbb on a > b where b > 0 and a > 0 and sqrt(a) > 1;
 a | b 
---+---
 2 | 1 
(1 row)

openGauss=# explain (costs off) select * from tba join tbb on a > b where b > 0 and a > 0 and sqrt(a) > 1;
              QUERY PLAN              
--------------------------------------
Nest loop
  Join Filter: (a > b)
  -> Seq Scan on public.tba
       Filter: (a > 0 and sqrt(a) > 1)
  -> Materialize
       -> Seq Scan on public.tbb
            Filter: (b > 0)
(7 rows)
```
