# ALIAS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-10-13T02:00:58.565Z pushedAt=2026-10-19T09:58:45.087Z -->

## d\_format\_behavior\_compat\_options<a name="section203671436822"></a>

**Function Description**

Implements the equals sign alias syntax. When the `disable_target_alias` parameter is enabled, the equals sign is parsed according to the previous syntax. By default, the parameter is disabled, and the equals sign is parsed according to the alias syntax.

**Precautions**

- This function does not modify the openGauss syntax. The conversion is performed during semantic analysis.

- Aliases do not support being enclosed in single quotes. Use the more general double quotes or square brackets instead.

**Syntax**

```sql
set d_format_behavior_compat_options = '';  --Parse the equals sign according to the alias syntax.
set d_format_behavior_compat_options= 'disable_target_alias'; --Parse the equals sign according to the original equals sign syntax.
```

**Example**

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
