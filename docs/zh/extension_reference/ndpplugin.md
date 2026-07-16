# NDPPlugin

## 概述

openGauss提供NDPPlugin Extension（版本为ndpplugin-1.0.0）。NDPPlugin Extension是openGauss资源池化场景下算子卸载扩展。共享存储虽然带来弹性，可靠性的好处，但是和本地盘单机比较性能会下降较多，主要是网络IO和分布式存储自身带来的延迟，尤其对于大规模查询buffer pool无法缓存的场景，大量的数据需要从存储节点搬运到计算节点，这些批量数据经过滤后大部分场景有效数据内容占比非常少，耗费大量的无用网络IO时间，性能较差。通过算子卸载将数据过滤卸载到存储侧执行，去除不需要的数据，从而减少网络通信数据量，提升端到端性能。

## 安装

openGauss-5.1.0版本已经默认编译安装NDPPlugin插件，使用步骤如下：

1.获取LibSmartScan_5.1.0_openEuler_aarch64.tar.gz并解压。

```
tar -zxvf LibSmartScan_5.1.0_openEuler_aarch64.tar.gz
```

2.添加如下环境变量：

```
export LD_LIBRARY_PATH=/path/to/LibSmartScan_5.1.0_openEuler_aarch64/LibSmartScan_ThirdParty/ceph/openEuler_2003_armlib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/path/to/LibSmartScan_5.1.0_openEuler_aarch64/LibSmartScan_ThirdParty/rpc/openEuler_2003_armlib:$LD_LIBRARY_PATH
```

3.postgresql.conf添加guc参数：

```
shared_preload_libraries = 'ndpplugin'
synchronize_seqscans = off
```

4.启动libsmartscan服务，见 **[libsmartscan安装](libsmartscan.md)**。

5.创建数据库并连接数据库开始使用。

```
openGauss=# create extension ndpplugin;
CREATE EXTENSION
```

## 限制

- 暂时仅支持shared_preload_libraries方式加载插件
- 暂不支持Toast表场景
- 暂不支持Ustore场景
- 暂不支持synchronize_seqscans

## 系统视图

pushdown_statics视图显示下推查询基础统计信息。

|名称|类型|描述|
| ------------ | ------------ | ------------ |
|query|unsigned long|下推查询数|
|total_pushdown_page|unsigned long|下推页面数|
|back_to_gauss|unsigned long|返回原生处理页面数|
|received_scan|unsigned long|接收到的scan算子数据过滤后的页面数|
|received_agg|unsigned long|接收到的agg算子数据聚合后的页面数|
|failed_backend_handle|unsigned long|存储侧libsmartscan处理失败页面数|
|failed_sendback|unsigned long|发送失败页面数|

## 查看视图

NDPPlugin视图用于查看查询语句下推详细统计信息，帮助用户判断语句下推情况。

```
openGauss=# select * from pushdown_statics();
 query | total_pushdown_page | back_to_gauss | received_scan | received_agg | failed_backend_handle | failed_sendback 
-------+---------------------+---------------+---------------+--------------+-----------------------+-----------------
     0 |                   0 |             0 |             0 |            0 |                     0 |               0
(1 row)
```

## GUC 参数说明

### ndpplugin.enable_ndp

**参数说明**：参数值为布尔类型，该参数用于使能插件。

**取值范围**：布尔型

- on表示开启算子卸载特性。
- off表示关闭算子卸载特性。

**默认值**：off

### ndpplugin.pushdown_min_blocks

**参数说明**：参数值为整数，该参数限制下推页面数阈值，页面数小于阈值的表即使满足下推条件也不会走下推流程。

**取值范围**：[0, INT_MAX / 1000]

**默认值**：0

### ndpplugin.ndp_port

**参数说明**：参数值为整数，该参数指定存储集群libsmartscan进程监听的端口号，用于和libsmartscan进程通信，发送任务。

**取值范围**：字符串

**默认值**：./

### ndpplugin.crl_path

**参数说明**：参数为字符串，该参数仅在开启SSL时有效，指定CRL证书路径。

**取值范围**：字符串

**默认值**：./
