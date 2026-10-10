# USE db_name

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:32:44.502Z pushedAt=2026-09-29T02:34:56.699Z -->

## Description<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

Connects to the current database.

## Precautions<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- Only the databases currently connected to USE are allowed. Connecting to other databases is not supported. If the database name specified in USE is not the current database, an error is reported.

## Syntax Format<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

```
USE db_name
```

## Parameters<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **db_name**

  ​  Database name.

## Examples<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```sql
create database testd with dbcompatibility = 'd';
\c testd
create extension shark;

use testd;
NOTICE:  Already connected to database 'testd'.

use test1;
ERROR:  Use of non-current database 'test1' is not supported.
```

## Reference<a name="section156744489391"></a>

N/A
