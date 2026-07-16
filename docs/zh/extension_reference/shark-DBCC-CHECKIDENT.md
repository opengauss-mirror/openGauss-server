# DBCC CHECKIDENT<a name="ZH-CN_TOPIC_0289899950"></a>

## 功能描述<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8a5c6264f78f49e3aa93f388d68cd3e6"></a>

DBCC CHECKIDENT用于对标识列进行获取或者重置功能。

## 注意事项<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s8cb7444b58764d99913a4cc61f397f9f"></a>

- 对标识列进行获取需要有表的select权限，对标识列执行重置需要有表的update权限。

## 语法格式<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s29888afda1844d6f9fc677f1b59b5b7d"></a>

```
DBCC CHECKIDENT (table_name [ , { NORESEED | { RESEED [ , new_reseed_value ] } } ] ) [ WITH NO_INFOMSGS ]
```

## 参数说明<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **table_name**

  表名，表中必须要有标识列。

- **NORESEED**

  指定仅做标识列的查询，不对标识列进行修改。

- **RESEED**

  指定应该对标识列进行修改。如果既不声明为RESEED，也不声明为NORESEED，默认为RESSED操作。

- **new_reseed_value**

  用作标识列的当前值的新值，默认值为选取标识列的当前标识值和当前标识列的最大值之间较大值。

- **WITH NO_INFOMSGS**

  取消显示所有信息性消息。

## 示例<a name="zh-cn_topic_0283136841_zh-cn_topic_0237122186_zh-cn_topic_0059779029_s51d29fa208274032a4e5308b57638421"></a>

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
