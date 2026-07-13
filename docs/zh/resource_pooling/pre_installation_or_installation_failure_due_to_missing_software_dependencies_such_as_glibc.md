# 因缺少glibc等软件依赖导致预安装或安装失败问题

## 一、问题现象

因缺少某个软件导致预安装/安装失败。例如，当缺少libreadline.so.7时，出现以下报错信息。

```shell
error while loading shared libraries: libreadline.so.7: cannot open shared object file: No such file or directory
```

## 二、定位方法

根据报错信息查看系统是否已经安装对应软件。

## 三、问题根因

操作系统缺少数据库安装必须的软件。

## 四、解决方法

安装缺少的软件。为确保数据库集群安装成功，操作系统上需要安装glibc、libaio-devel、libnsl（openEuler+x86环境中）、libreadline.so.7。
