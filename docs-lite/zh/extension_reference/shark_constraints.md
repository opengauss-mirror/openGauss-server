# 约束

约束子句用于声明约束，新行或者更新的行必须满足这些约束才能成功插入或更新。如果存在违反约束的数据行为，行为会被约束终止。

约束可以在创建表时规定（通过 CREATE TABLE 语句），或者在表创建之后规定（通过 ALTER TABLE 语句）。

约束可以是列级或表级。列级约束仅适用于列，表级约束被应用到整个表。

openGauss中shark插件中的约束如下：

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。原openGauss请参考章节[约束](../brief_tutorial/constraints.md)。
- IDENTITY: 用于在表中创建一个标识列，仅在D模式下shark插件中生效。

## IDENTITY约束<a name="section11621339171820"></a>

IDENTITY约束语法：IDENTITY [ (seed , increment) ]

参数
seed
加载到表中的第一个行所使用的值。

increment
与前一个加载的行的标识值相加的增量值。

必须同时指定种子和增量，或者二者都不指定。 如果二者都未指定，则取默认值 (1,1)。

IDENTITY 属性可以分配给 tinyint、smallint、int、bigint、decimal(p, 0) 或 numeric(p, 0) 列，每个表只能创建一个标识列。

IDENTITY列显示插入值的行为由GUC参数IDENTITY_INSERT进行控制，默认值OFF表示不可以显示插入IDENTITY列值。

```
openGauss=# CREATE TABLE book
(
    bookId int NOT NULL PRIMARY KEY IDENTITY,
    bookname NVARCHAR(50),
    author NVARCHAR(50)
);
NOTICE:  CREATE TABLE will create implicit sequence "book_bookid_seq_identity" for serial column "book.bookid"
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "book_pkey" for table "book"
CREATE TABLE
```

在INSERT语句中，如果表不存在或者对表的INSERT操作无权限，VALUES子句中的值或者子查询中的值与插入的列类型不匹配(无法转换的类型之间，超过类型精度等情况)或者值的数量不等于插入的列数目、违背目标表上的触发器或者插入操作被触发器修改返回NULL(如BEFORE ROW INSERT TRIGGER，INSTEAD OF ROW INSERT TRIGGER)、VALUES子句/SELECT子句/RETURNING子句中存在不支持的语法时、以及INSERT语句存在语法级别的错误时，IDENTITY列的当前值不会因为这些错误场景而更新，但在INSERT操作违背列约束的错误情况下，IDENTITY列的当前值会进行更新(因为在约束检查前插入数据已经完全准备好了，identity列已经获取到下一个值)。

## 相关链接<a name="section156744489391"></a>

[约束](../brief_tutorial/constraints.md)
