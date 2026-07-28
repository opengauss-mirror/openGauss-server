# shark

## shark概述

openGauss提供shark Extension（版本为shark-1.0.0）。shark Extension是openGauss的D兼容性数据库（dbcompatibility='D'）扩展，旨在兼容D库语法。shark插件继承内核原有SQL语法，在[shark语法介绍](shark_keywords.md)中，将主要介绍对于内核语法有新增、修改的内容，和内核保持一致的语法等将不再额外写出。

## shark限制

- shark插件只能在D兼容性数据库下创建。
- 通常情况下不支持删除shark插件，但当参数support_extended_features打开，且没有依赖时，允许删除插件。
- 已经创建了shark插件，重启或升级时需要将shark配置到guc参数shared_preload_libraries中，否则无法连接D兼容性数据库或升级失败。
- shark中所有新增/修改的语法不支持在gsql客户端通过```\h```查看帮助说明，不支持在gsql客户端自动补齐。
- shark当前支持使用UTF8, SQL_ASCII字符集创建的数据库下使用。

## shark安装

shark插件随内核一同编译，需要手动创建该插件，步骤如下：
    
### 编译安装

1. [编译安装openGauss](https://gitcode.com/opengauss/openGauss-server#%E7%BC%96%E8%AF%91)。

2. 创建D库并执行创建。

```
openGauss=# create database db_name dbcompatibility 'D';
CREATE DATABASE

openGauss=# \c db_name

db_name=# create extension shark ;
CREATE EXTENSION
```
