# 因处于回放状态导致停库时间过长的问题

## 一、问题现象

使用`cm_ctl stop`命令之后，停库时间过长。

## 二、定位方法

1.进入`$GAUSSLOG/cm/cm_agent`日志，搜索关键字`dms_contrl.sh -clean`，查看有没有正常调用dms_contrl.sh脚本；

2.进入`$GAUSSLOG/pg_log`日志，搜索 `Gaussdb exit(0)`字样，如果未出现则认为数据库并非正常退出；

3.重新启动数据库以后，观察 `GAUSSLOG/pg_log`日志是否存在 `Recovery` 字样的日志，如果有则认为刷脏页时间过长，导致脚本无法正常通过`gs_ctl stop`优雅关闭，而是gaussdb进程被直接kill掉，导致重新启动以后进入Recovery模式。

## 三、问题根因

正常执行`cm_ctl stop`时，CM会调用 `dms_contrl.sh` 脚本去停库，但是由于当前数据库刷脏页时间过长， `gs_ctl stop`在规定时间内未能正常停库，`dms_contrl.sh`会强制kill掉数据库。
