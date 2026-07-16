# USE db_name

## 功能描述<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

连接到当前数据库。

## 注意事项<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- 仅支持USE当前连接的数据库，不支持连接到其他数据库。若USE的数据库名不是当前数据库，则报错。

## 语法格式<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

```
USE db_name
```

## 参数说明<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **db_name**

  ​  数据库名。

## 示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```sql
create database testd with dbcompatibility = 'd';
\c testd
create extension shark;

use testd;
NOTICE:  Already connected to database 'testd'.

use test1;
ERROR:  Use of non-current database 'test1' is not supported.
```

## 相关链接<a name="section156744489391"></a>

N/A
