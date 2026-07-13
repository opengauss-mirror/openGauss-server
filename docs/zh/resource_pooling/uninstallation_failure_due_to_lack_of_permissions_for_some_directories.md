# 因部分目录无权限导致卸载失败问题

## 一、问题现象

数据库卸载时，出现报错信息如下。

```shell
[GAUSS-50315] : The user xxx is not matched with the owner of /usr1/xxx/install/app.
```

## 二、定位方法

根据报错信息，检查目录权限是否为当前用户。

## 三、原因分析

目录权限不是当前用户。

## 四、解决方法

使用chown命令将目录权限恢复为当前用户，再次执行卸载。
