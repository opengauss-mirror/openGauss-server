# 因环境变量GAUSS_ENV=2导致的安装已存在问题

## 一、问题现象

执行`gs_install`命令过程中报错如下。

```shell
[GAUSS-51806] : The cluster has been installed.
```

## 二、定位方法

首先通过`ps ux`命令检查当前环境是否存在数据库进程如`gaussdb`、`dssserver`、`cm_agent`、`cm_server`、`om_monitor`，如果存在则需要通过`gs_uninstall`命令或是`kill -9`卸载数据库；如果不存在进一步检查环境变量`GAUSS_ENV`的值是否为2。

## 三、问题根因

环境变量`GAUSS_ENV=1`表示预安装成功，`GAUSS_ENV=2`表示安装成功；如果没有正常卸载安装过的数据库，GAUSS_ENV=2仍存在，再次执行安装时就会产生冲突。

## 四、解决方法

使用以下命令清理环境变量，然后重新执行预安装和安装命令。

```shell
unset GAUSS_ENV
```
