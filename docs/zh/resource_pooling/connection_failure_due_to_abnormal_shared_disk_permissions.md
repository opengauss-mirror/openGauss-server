# 因共享盘权限异常导致连接失败的问题

## 问题现象

连接数据库失败。

```shell
[carrotom@openGauss111 ~]$ gsql -r
gsql: FATAL:  Could not connect dssserver, vgname: "+data", socketpath: "UDS:/usr3/carrotom/cluster/dss_home/.dss_unix_d_socket"
HINT:  Check vgname and socketpath and restart later.
```

## 定位方法

1. 查看socket文件是否有问题。

    ```shell
    [root@openGauss111 script]# ll /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
    srw-------. 1 carrotom carrotom 0 Sep 27 15:18 /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
    ```

2. 套接字文件正常，则查看共享盘是否有问题。

    ```shell
    brw-rw----. 1 root        root         66,  16 Sep 27 16:40 sdah
    brw-rw----. 1 carrotom    carrotom     66,  32 Sep 27 16:40 sdai
    brw-rw----. 1 carrotom    carrotom     66,  48 Sep 27 16:40 sdaj
    brw-rw----. 1 carrotom    carrotom     66,  64 Sep 27 16:40 sdak
    ```
    
    可以发现资源池化的共享盘用户发生异常，变成了root。

## 问题根因

数据库一般不会去修改共享盘的权限，可能是由于管理员误操作、其他程序修改等原因导致。

## 解决方案

使用chmod或chown等，修改共享盘权限为正常即可恢复。
