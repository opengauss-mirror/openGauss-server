# DBCC CHECKIDENT<a name="ZH-CN_TOPIC_0289899950"></a>

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:31:40.580Z pushedAt=2026-09-21T10:01:10.407Z -->

## Function Description<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8a5c6264f78f49e3aa93f388d68cd3e6"></a>

DBCC CHECKIDENT is used to query or reset an identity column.

## NOTE<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8cb7444b58764d99913a4cc61f397f9f"></a>

- Querying an identity column requires the SELECT privilege on the table, and resetting an identity column requires the UPDATE privilege on the table.

## Syntax<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s29888afda1844d6f9fc677f1b59b5b7d"></a>

```
DBCC CHECKIDENT (table_name [ , { NORESEED | { RESEED [ , new_reseed_value ] } } ] ) [ WITH NO_INFOMSGS ]
```

## Parameter Description<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **table_name**

  The name of the table, which must contain an identity column.

- **NORESEED**

  Specifies that only the identity column is queried without being modified.

- **RESEED**

  Specifies that the identity column should be modified. If neither RESEED nor NORESEED is declared, the default is the RESEED operation.

- **new_reseed_value**

  The new value to be used as the current value of the identity column. The default value is the greater of the current identity value and the maximum value of the identity column.

- **WITH NO_INFOMSGS**

Suppress all informational messages.

## Examples<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s51d29fa208274032a4e5308b57638421"></a>

```
openGauss=# CREATE TABLE Employees (EmployeeID serial ,Name VARCHAR(100) NOT NULL);
NOTICE:  CREATE TABLE will create implicit sequence "employees_employeeid_seq" for serial column "employees.employeeid"
CREATE TABLE
openGauss=# insert into Employees(Name) values ('zhangsan');
INSERT 0 1
openGauss=# insert into Employees(Name) values ('lisi');
INSERT 0 1
openGauss=# insert into Employees(Name) values ('wangwu');
INSERT 0 1
openGauss=# insert into Employees(Name) values ('heliu');
INSERT 0 1
openGauss=# DBCC CHECKIDENT ('Employees', NORESEED);
NOTICE:  "Checking identity information: current identity value '4', current column value '4'."
CONTEXT:  referenced column: dbcc_check_ident_no_reseed
                              dbcc_check_ident_no_reseed
--------------------------------------------------------------------------------------
 Checking identity information: current identity value '4', current column value '4'.
(1 row)

openGauss=# DBCC CHECKIDENT ('Employees', RESEED, 10);
NOTICE:  "Checking identity information: current identity value '4'."
CONTEXT:  referenced column: dbcc_check_ident_reseed
                  dbcc_check_ident_reseed
------------------------------------------------------------
 Checking identity information: current identity value '4'.
(1 row)

openGauss=# DBCC CHECKIDENT ('Employees', NORESEED);
NOTICE:  "Checking identity information: current identity value '10', current column value '4'."
CONTEXT:  referenced column: dbcc_check_ident_no_reseed
                              dbcc_check_ident_no_reseed
---------------------------------------------------------------------------------------
 Checking identity information: current identity value '10', current column value '4'.
(1 row)
```