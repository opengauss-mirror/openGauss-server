# ALIAS

## d\_format\_behavior\_compat\_options<a name="section203671436822"></a>

**功能描述**

实现等号别名语法，当开启参数disable_target_alias后，等号按照之前的语法解析。默认不开启参数，按照别名语法解析。

**注意事项**

- 本功能未对openGauss的语法做任何修改，是在语义分析时做的转换。

- 别名不支持单引号包裹，可使用更通用的双引号或方括号包裹。

**语法格式**

```sql
set d_format_behavior_compat_options = '';  --等号按照别名语法解析
set d_format_behavior_compat_options= 'disable_target_alias'; --等号按照原始等号语法解析
```

**示例**

```sql
openGauss=# CREATE TABLE test(a int, b int);
CREATE TABLE
openGauss=# INSERT INTO test VALUES (1,1), (2,3);
INSERT 0 2
openGauss=# SET d_format_behavior_compat_options = '';
SET
openGauss=# SELECT a = 1;
 a
---
 1
(1 row)
openGauss=# SELECT a = b FROM test;
 a
---
 1
 3
(2 rows)
openGauss=# SET d_format_behavior_compat_options = 'disable_target_alias';
SET
openGauss=# SELECT a = 1;
ERROR:  column "a" does not exist
LINE 1: SELECT a = 1;
			   ^
openGauss=# SELECT a = b FROM test;
 ?column?
----------
 t
 f
(2 rows)
```
