# 因预安装使用默认参数导致安装失败问题

## 一、问题现象

1. 不设置dataPortBase、cmServerPortBase，端口被占用时，安装完成后启动数据库失败。以下报错信息为cm默认端口被占用的启动失败信息与cm_agent日志。

启动失败信息。

```shell
Failed to start cluster  in (300)s.
It will continue to start in the background.
If you want to see the cluster status, please try command gs_om -t status.
If you want to stop the cluster, please try command gs_om -t stop.
```

cm_agent报错信息。

```
ERROR: 345: connect to cm_server failed, host=20.20.20.135 port=5000 localhost=20.20.20.135 connect_timeout=1 node_id=1 node_name=openGauss135 remote_type=7. could not connect to server:
        TCP/IP connections on port 5000?
```

2. 不设置cmDir，安装时出现以下报错信息。

```
[GAUSS-50201] : The cm data directory [/cm_server] does not exist.
```

## 二、定位方法

预安装阶段根据xml文件设置端口、数据库安装目录。如果在安装完成后出现数据库启动失败现象，先查看启动日志`$PGDATA/DBStart.log`和cm日志`$GAUSSLOG/cm/cm_agent`，对于端口被占用情况，使用命令`netstat -nap | grep xxxxx`或`lsof -i:xxxxx`检查dataPortBase或cmServerPortBase端口号是否被占用；如果出现cmDir不存在，根据报错信息，修改xml文件即可。

## 三、问题根因

1. 使用默认端口时，存在默认端口已被占用的情况，当端口占用时安装完成后数据库启动失败。
2. 初始化cm实例时会对cmDir进行检测，目录不存在会报错，终止安装。

## 四、解决方法

1. 将dataPortBase、cmServerPortBase修改为未被占用的端口号，然后重新进行预安装与安装。
2. xml文件中配置cmDir路径，然后重新安装数据库。
