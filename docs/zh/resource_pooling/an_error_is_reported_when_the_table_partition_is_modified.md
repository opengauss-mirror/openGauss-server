# 执行修改表分区操作时报错

## 问题现象<a name="section2456113514495"></a>

执行ALTER TABLE PARTITION时，报错如下。

```
ERROR:start value of partition "XX" NOT EQUAL up-boundary of last partition.
```

## 原因分析<a name="section14776440134911"></a>

在同一条ALTER TABLE PARTITION语句中，既存在DROP PARTITION又存在ADD PARTITION时，无论它们在语句中的顺序是什么，openGauss总会先执行DROP PARTITION再执行ADD PARTITION。执行完DROP PARTITION删除末尾分区后，再执行ADD PARTITION操作会出现分区间隙，导致报错。

## 处理办法<a name="section1838384914918"></a>

为防止出现分区间隙，需要将ADD PARTITION的START值前移。示例如下。

```
--创建分区表partitiontest。
openGauss=#  CREATE TABLE partitiontest
(  
c_int integer, 
c_time TIMESTAMP WITHOUT TIME ZONE 
) 
PARTITION BY range (c_int) 
( 
partition p1 start(100)end(108),  
partition p2 start(108)end(120) 
);
--使用如下两种语句会发生报错：
openGauss=#  ALTER TABLE partitiontest ADD PARTITION p3 start(120)end(130), DROP PARTITION p2; 
ERROR:  start value of partition "p3" NOT EQUAL up-boundary of last partition. 
openGauss=#  ALTER TABLE partitiontest DROP PARTITION p2,ADD PARTITION p3 start(120)end(130); 
ERROR:  start value of partition "p3" NOT EQUAL up-boundary of last partition.
--可以修改语句为：
openGauss=#  ALTER TABLE partitiontest ADD PARTITION p3 start(108)end(130), DROP PARTITION p2;
openGauss=#  ALTER TABLE partitiontest DROP PARTITION p2,ADD PARTITION p3 start(108)end(130);
```
