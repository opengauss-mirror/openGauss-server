# 同层参数化路径的Hint<a name="ZH-CN_TOPIC_0000001266694989"></a>

## 功能描述<a name="section290819468377"></a>

通过predpush\_same\_level Hint来指定同层表或物化视图之间参数化路径生成。

## 语法格式<a name="section530131664410"></a>

```
predpush_same_level(src, dest)
predpush_same_level(src1 src2 ..., dest)
```

>[!NOTE]说明
>
>本参数仅在rewrite\_rule中的predpushforce选项打开时生效。

## 示例<a name="section5736356154"></a>

准备参数和表及索引：

```
openGauss=# set rewrite_rule = 'predpushforce';
SET
openGauss=# create table t1(a int, b int);
CREATE TABLE
openGauss=# create table t2(a int, b int);
CREATE TABLE
openGauss=# create index idx1 on t1(a);
CREATE INDEX
openGauss=# create index idx2 on t2(a);
CREATE INDEX
```

执行语句查看计划：

```
openGauss=# explain select * from t1, t2 where t1.a = t2.a;
                            QUERY PLAN
------------------------------------------------------------------
 Hash Join  (cost=27.50..56.25 rows=1000 width=16)
   Hash Cond: (t1.a = t2.a)
   ->  Seq Scan on t1  (cost=0.00..15.00 rows=1000 width=8)
   ->  Hash  (cost=15.00..15.00 rows=1000 width=8)
         ->  Seq Scan on t2  (cost=0.00..15.00 rows=1000 width=8)
(5 rows)
```

可以看到t1.a = t2.a条件过滤在Join上面，此时可以通过predpush\_same\_level\(t1, t2\)将条件下推至t2的扫描算子上：

```
openGauss=# explain select /*+predpush_same_level(t1, t2)*/ * from t1, t2 where t1.a = t2.a;
                             QUERY PLAN
---------------------------------------------------------------------
 Nested Loop  (cost=0.00..335.00 rows=1000 width=16)
   ->  Seq Scan on t1  (cost=0.00..15.00 rows=1000 width=8)
   ->  Index Scan using idx2 on t2  (cost=0.00..0.31 rows=1 width=8)
         Index Cond: (a = t1.a)
(4 rows)
```

>[!TIP]须知
>
>- predpush\_same\_level可以指定多个src，但是所有的src必须在同一个条件中。
>- 如果指定的src和dest条件不存在，或该条件不符合参数化路径要求，则本hint不生效。
