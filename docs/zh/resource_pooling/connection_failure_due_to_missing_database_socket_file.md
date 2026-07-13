
# 因数据库套接字文件丢失导致连接失败的问题

## 问题现象

已有连接正常运行，新连接出现连接失败报错。

```shell
[carrotom@openGauss111 carrotom_mppdb]$ gsql -r
failed to connect /usr3/carrotom/cluster/om/carrotom_mppdb:15555.
```

## 定位方法

1. 报错中指明了路径与端口`/usr3/carrotom/cluster/om/carrotom_mppdb:15555`。

2. 查看数据库参数配置文件，找到数据库运行端口与套接字存放位置，正是报错中的路径与端口。

    ```shell
    [carrotom@openGauss111 dn1]$ cat postgresql.conf | grep -e port -e unix_socket_directory
    port = 15555
    unix_socket_directory = '/usr3/carrotom/cluster/om/carrotom_mppdb'
    ```

    注意：此处不一样也不要紧，参数配置文件允许在任意时刻修改，但这两个参数只有数据库重启才生效。以报错为准。

3. 查看文件夹内套接字文件数量。

    ```shell
    [carrotom@openGauss111 cluster]$ ll /usr3/carrotom/cluster/om/carrotom_mppdb -a
    total 12K
    drwx------. 2 carrotom carrotom  152 Sep 27 15:00 .
    drwx------. 7 carrotom carrotom 4.0K Sep 27 11:42 ..
    srwx------. 1 carrotom carrotom    0 Sep 27 15:00 .gaussUDF.socket
    -rw-------. 1 carrotom carrotom   93 Sep 27 15:00 .s.PGSQL.15555.lock
    srwx------. 1 carrotom carrotom    0 Sep 27 15:00 .s.PGSQL.15556
    -rw-------. 1 carrotom carrotom   93 Sep 27 15:00 .s.PGSQL.15556.lock
    ```
    
    可以发现15555端口的套接字文件丢失。

## 问题根因

套接字文件权限异常或丢失的原因有很多，常见的原因有：

- 系统错误

- 磁盘空间不足

- 存放socket文件的目录无访问权限

- 数据库管理员误操作

- 恶意病毒软件或黑客破坏

- 其他异常问题

## 解决方案

重启节点或集群即可修复。

无其他影响。

另外，数据库除了port配置端口之外，还有一个+1端口，如图所示为15556，是运维预留端口，套接字存在的话可以进行连接。
