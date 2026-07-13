
# 因磁盘权限过高导致鉴权失败CM不可用的问题

## 问题现象

在安装数据库时，磁盘权限过高，数据库安装成功，导致鉴权失败出现 CM 不可用的现象，会提示如下信息：

```shell
LOG: open gaussdb state file "/.../.../dn1/gaussdb.state" failed, could not get the build information: Permission denied.
```

## 定位方法

进入`$GAUSSLOG/cm/cm_agent`目录下，寻找最近时间节点的`cm_agent`日志，发现如下报错信息：

```shell
2024-10-09 11:07:58.966 tid=313678  ERROR: [get_connection: 1526]: fail to read pid file (/.../.../dn1/postmaster.pid).
2024-10-09 11:07:58.966 tid=313678  ERROR: failed to connect to datanode:/.../.../dn1
```

可以看到，是`CM`目录权限不足。

## 问题根因

对于磁盘设备文件，如果权限设置过高（过于严格），可能会导致操作系统、其他应用无法正常访问磁盘，从而引发各种错误。
环境安装完成，目录权限过于严格，导致`CM`没有访问磁盘的权限。

## 解决方案

使用命令`chmod`命令，修改磁盘的权限为正常即可。

```shell
chmod -R 775 /dir
```
