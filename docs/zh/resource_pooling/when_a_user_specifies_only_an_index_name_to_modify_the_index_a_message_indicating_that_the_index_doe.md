# 修改索引时只调用索引名提示索引不存在

## 问题现象<a name="section11297759192710"></a>

修改索引时只调用索引名提示索引不存在。举例如下。

```
--创建分区表索引HR_staffS_p1_index1，不指定索引分区的名称。 
CREATE INDEX HR_staffS_p1_index1 ON HR.staffS_p1 (staff_ID) LOCAL; 
--创建分区索引HR_staffS_p1_index2，并指定索引分区的名称。 
CREATE INDEX HR_staffS_p1_index2 ON HR.staffS_p1 (staff_ID) LOCAL 
(   
PARTITION staff_ID1_index,    
PARTITION staff_ID2_index TABLESPACE example3,    
PARTITION staff_ID3_index TABLESPACE example4 
) TABLESPACE example; 
--修改索引分区staff_ID2_index的表空间为example1，提示索引不存在。
ALTER INDEX HR_staffS_p1_index2 MOVE PARTITION staff_ID2_index TABLESPACE example1;
```

## 原因分析<a name="section13485101002814"></a>

推测是当前模式是public模式，而不是hr模式，导致检索不到该索引。

```
--执行如下命令验证推测，发现调用成功。
ALTER INDEX hr.HR_staffS_p1_index2 MOVE PARTITION staff_ID2_index TABLESPACE example1;
--修改当前会话的schema为hr。
ALTER SESSION SET CURRENT_SCHEMA TO hr;
--执行如下命令修改索引，即可执行成功。
ALTER INDEX HR_staffS_p1_index2 MOVE PARTITION staff_ID2_index TABLESPACE example1;
```

## 处理办法<a name="section12373188285"></a>

在操作表、索引、视图时加上schema引用，格式如下。

```
schema.table
```
