
# 因DSS套接字文件丢失导致连接失败的问题

## 问题现象

已有连接正常运行，新连接出现连接失败报错，报错中提示socket套接字相关内容。

```shell
[carrotom@openGauss111 cluster]$ gsql -r
gsql: FATAL:  Could not connect dssserver, vgname: "+data", socketpath: "UDS:/usr3/carrotom/cluster/dss_home/.dss_unix_d_socket"
HINT:  Check vgname and socketpath and restart later.
```

## 定位方法

通过报错可以看出是套接字文件异常，查看对应的文件，发现套接字丢失。

```shell
[carrotom@openGauss111 carrotom_mppdb]$ ll /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
```

## 问题根因

套接字异常的常见原因有：

- 系统错误

- 磁盘空间不足

- 存放socket文件的目录无访问权限

- 数据库管理员误操作

- 恶意病毒软件或黑客破坏

- 其他异常问题

## 解决方案

若套接字文件不存在，重启dss节点或重启集群即可修复。

无其他影响。
