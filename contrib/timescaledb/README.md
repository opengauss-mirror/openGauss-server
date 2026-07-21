目录

[Toc]

# **1.** 概述

## **1.1.** 目的

本文旨在指导如何安装、使用TimescaleDB。

## **1.2.** TimescaleDB介绍

TimescaleDB是一个开源的时间序列数据库，专门用于高性能和可扩展的时间序列数据存储和分析。它结合了关系型数据库的功能和优势，以及时间序列数据库的特性，提供了一套强大的功能来处理大规模时间序列数据。基于以上描述，TimescaleDB 在以下场景中非常适用：

1. 物联网（IoT）应用：物联网应用通常会产生大量的时间序列数据，例如传感器数据、设备监控数据等。TimescaleDB 的高性能和数据分区功能可以有效地处理这些数据，并支持快速的实时查询和分析。
2. 金融和交易数据：金融行业需要对交易数据进行高效的存储和分析。TimescaleDB 的连续聚合和数据保留策略功能可以方便地计算和维护聚合数据，同时自动删除过期的数据。
3. 日志和监控数据：在日志和监控系统中，需要对大量的事件和指标数据进行存储和分析。TimescaleDB 的数据连续性和数据压缩功能可以满足高并发的写入需求，并减少存储空间的使用。
4. 时间序列分析：对于需要进行时间序列分析的场景，TimescaleDB 提供了 SQL 接口和丰富的时间序列函数，可以方便地进行复杂的查询和分析操作。

TimescaleDB能够以插件化的形式，很方便的处理时序数据，随着openGauss的发展，对时序数据的处理能力也成为了开发组重点考虑的功能，而且openGauss基于PostgreSQL 9.2.4版本优化，所以从PostgreSQL数据库将TimescaleDB扩展迁移过来是一项满足经济性和科学性的决定。

## **1.3.** 注意事项

### 1.3.1. 一般性限制

- 不支持非编译安装版本
- TimescaleDB插件有些功能只能在pg兼容性数据库下使用，建议在pg兼容性数据库下创建与使用
- 目前TimescaleDB安装之后，不支持删除TimescaleDB插件
- TimescaleDB插件依赖于public schema，因此不支持使用drop schema的方式删除public schema
- Timescaledb 1.7.4 是适配 PostgreSQL 9.6 的插件。PostgreSQL 9.6 中只支持 ON CONFLICT 语法，直到 PostgreSQL 15 才支持 MERGE INTO 语法。而 openGauss 6.0 不支持 ON CONFLICT 语法，**因此该版本的 Timescaledb不支持openGauss的`MERGE INTO`语法**。
由于 PostgreSQL 9.6 的 ON CONFLICT 语法用于在插入数据时处理冲突情况，例如更新已有记录或忽略冲突，而 MERGE INTO 语法则在 PostgreSQL 15 及以上版本中提供了更为强大的功能，允许在单个语句中执行插入、更新或删除操作。openGauss 6.0 不支持 ON CONFLICT 语法，这意味着 Timescaledb 1.7.4 无法利用此语法处理插入冲突，从而在 openGauss 上无法实现相应的功能。
因此，Timescaledb 1.7.4 由于语法支持的限制，无法在 openGauss 6.0 上实现与 PostgreSQL 9.6+ 的相同功能。


### 1.3.2. 软件依赖要求


以下表格列举了编译TimescaleDB的特有软件要求，其它软件依赖请参考[openGauss](https://gitee.com/opengauss/openGauss-server#%E6%93%8D%E4%BD%9C%E7%B3%BB%E7%BB%9F%E5%92%8C%E8%BD%AF%E4%BB%B6%E4%BE%9D%E8%B5%96%E8%A6%81%E6%B1%82)。


软件依赖要求如下：
软件    | 推荐版本
--------|-----------
cmake   | 3.4及以上版本

# **2.** TimescaleDB安装方法


## **2.1.** 源码安装


从Plugin仓下载好TimescaleDB源码，解压完成后，放入openGauss-server/contrib目录下，在脚本所在目录执行 `./bootstrap -DUSE_OPENSSL=0 -DREGRESS_CHECKS=OFF`

```
cd contrib/timescaledb
./bootstrap -DUSE_OPENSSL=0 -DREGRESS_CHECKS=OFF 
```

进入`./build`文件夹中，执行`make && make install`


在对应数据库配置文件（比如data/postgresql.conf）中的最后一行写入`shared_preload_libraries = '$libdir/timescaledb'`

启动数据库，进入到sql命令行界面，执行`create extension timescaledb;`，若出现以下结果，则说明安装成功

```sql
openguass=# create extension timescaledb;
WELCOME TO
 _____ _                               _     ____________  
|_   _(_)                             | |    |  _  \ ___ \ 
  | |  _ _ __ ___   ___  ___  ___ __ _| | ___| | | | |_/ / 
  | | | |  _ ` _ \ / _ \/ __|/ __/ _` | |/ _ \ | | | ___ \ 
  | | | | | | | | |  __/\__ \ (_| (_| | |  __/ |/ /| |_/ /
  |_| |_|_| |_| |_|\___||___/\___\__,_|_|\___|___/ \____/
               Running version 1.7.4
For more information on TimescaleDB, please visit the following links:

 1. Getting started: https://docs.timescale.com/getting-started
 2. API reference documentation: https://docs.timescale.com/api
 3. How TimescaleDB is designed: https://docs.timescale.com/introduction/architecture

Note: TimescaleDB collects anonymous reports to better understand and assist our users.
For more information and how to disable, please see our docs https://docs.timescaledb.com/using-timescaledb/telemetry.
CREATE EXTENSION
```
## **2.2.** 流水线build.sh安装
将timescaledb源码 放到openGauss-server的源码 contrib 目录下，运行build.sh脚本，timescaledb会被自动编译安装

## **2.3.** 创建超表 

```sql
-- Do not forget to create timescaledb extension
-- CREATE EXTENSION timescaledb;

-- We start by creating a regular SQL table
CREATE TABLE conditions (
  time        TIMESTAMPTZ       NOT NULL,
  location    TEXT              NOT NULL,
  temperature DOUBLE PRECISION  NULL,
  humidity    DOUBLE PRECISION  NULL
);

-- Then we convert it into a hypertable that is partitioned by time
SELECT create_hypertable('conditions', 'time');
```


## **2.3.** 插入和查询数据

Inserting data into the hypertable is done via normal SQL commands:

```sql
INSERT INTO conditions(time, location, temperature, humidity)
  VALUES (NOW(), 'office', 70.0, 50.0);

SELECT * FROM conditions ORDER BY time DESC LIMIT 100;

SELECT time_bucket('15 minutes', time) AS fifteen_min,
    location, COUNT(*),
    MAX(temperature) AS max_temp,
    MAX(humidity) AS max_hum
  FROM conditions
  WHERE time > NOW() - interval '3 hours'
  GROUP BY fifteen_min, location
  ORDER BY fifteen_min DESC, max_temp DESC;
```

## **2.4.** 将test文件夹中的函数编译到.so中 (可选)

**条件：** 需要在编译timescaledb的时候设置debug编译选项 `-DCMAKE_BUILD_TYPE="Debug"`，之后test文件夹中的函数会自动编译到.so中


# **3.** TimescaleDB可用接口
| 序号 | 接口名称                                                     | 说明                                                         |
| ---- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1    | chunk_relation_size                                          | 获取超表块的关系大小                                         |
| 2    | chunk_relation_size_pretty                                   | 获取超表块的关系大小                                         |
| 3    | drop_chunks                                                  | 删除时间范围完全在指定时间之前（或之后）的数据区块，跨所有超级表或针对特定超级表运行。 |
| 4    | hypertable_relation_size                                     | 获取超级表的关系大小                                         |
| 5    | hypertable_relation_size_pretty                              | 获取超级表的关系大小                                         |
| 6    | indexes_relation_size                                        | 获取超表上的索引大小                                         |
| 7    | indexes_relation_size_pretty                                 | 获取超表上的索引大小                                         |
| 8    | set_number_partitions                                        | 设置超表上空间维度的分区（片）数                             |
| 9    | show_chunks                                                  | 获取与超表关联的区块列表                                     |
| 10   | add_dimension()空间分区                                      | 向超表添加额外的分区维度。选择作为维度的列可以使用间隔分区或哈希分区。 |
| 11   | attach_tablespace（）将表空间附加到超表                      | 将表空间附加到超表并使用它来存储块                           |
| 12   | create_hypertable（）创建超表                                | 创建超表                                                     |
| 13   | detach_tablespace（）从一个或多个超级表中分离表空间。        | 从一个或多个超级表中分离表空间                               |
| 14   | detach_tablespaces（）从超表中分离所有表空间。               | 从超表中分离所有表空间                                       |
| 15   | set_chunk_time_interval（）设置超表上的chunk_time_interval。 | 设置超表上的区块时间间隔                                     |
| 16   | set_integer_now_func（）设置整数超表当前时间函数             | 只适用于整数类超表，它设置一个函数，该函数以时间列的单位返回now（）值（当前时间） |
| 17   | time_bucket()函数                                            | time_bucket用于分析任意时间间隔的数据                        |
| 18   | timescaledb_information.hypertable获取超表信息               | 获取超表的相关信息或者查看一个表是否为超表                   |
| 19   | timescaledb_information.license获取许可信息                  | 获取有关当前许可证的信息                                     |
| 20   | show_tablespaces（）将显示附加到超表的表空间。               | 将显示附加到超表的表空间。                                   |