
# 因磁盘属主异常导致安装失败的问题

## 问题现象

数据库安装失败，出现如下报错信息：

```shell
[GAUSS-50315]: The user xxx is not matched with the owner of /dev/sda.
```

## 定位方法

使用`ls`命令查看磁盘属主是否为当前用户，若不是则使用`chown`命令修改属主为当前用户。

```shell
ls -l /dev/sda
brw-rw---- 1 root disk 8, 1 Oct  5 10:27 /dev/sda
```

## 问题根因

数据库一般不会修改磁盘属主，可能是管理员或其他用户误操作、其他程序修改导致的。

## 解决方案

使用`chown`命令，修改磁盘属主为当前用户即可。例如，要将磁盘`/dev/sda`的属主修改为用户`xxx`，可以使用以下命令：

```shell
chown xxx /dev/sda
```
