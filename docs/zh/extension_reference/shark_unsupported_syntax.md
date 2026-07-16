# 不支持语法

shark插件内基于一些新增特性，会屏蔽某些语法特性。本章节主要列出在使用shark插件后，一些语法默认不再支持。

## 后缀操作符

### 语法变更说明

- openGauss唯一的内置后缀操作符为阶乘`!`，shark插件中移除了后缀操作符的语法，因此不再支持阶乘运算符`!`。相关功能请使用factorial函数替换。

```sql
-- 原先支持的语法如，后续将不再支持
select 10！;
-- 可用的相关语法
select factorial(10);
```

## ROWNUM

### 语法变更说明

- rownum不再作为伪列关键词。

```sql
-- 不再支持rownum作为伪列关键词
openGauss=# select * from test1 where ROWNUM < 2;
ERROR:  column "rownum" does not exist
LINE 1: select * from test1 where ROWNUM < 2;
                                  ^

--可用语法
--例如可以使用limit替代rownum小于语法
openGauss=# select * from test1 limit 1;
 id
----
  1
(1 row)

```
