# 因CheckOs检测项不通过导致预安装失败问题

## 一、问题现象

预安装执行报错，报错信息如下。

```shell
[GAUSS-52400] : Installation environment does not meet the desired result.
Please get more details by "gs_checkos -i A -h openGaussxxx,openGaussxxx -X /home/dbuser/1s1p.xml --detail".
```

## 二、定位方法

使用`gs_checkos`命令查看异常（Abnormal）的检查项。

```shell
gs_checkos -i A -h openGaussxxx,openGaussxxx -X /home/dbuser/1s1p.xml --detail
```

## 三、问题根因

`gs_checkos`检测出现异常，基本是集群所在操作系统版本、网络、磁盘、内存等异常，或者是各主机时区、时间、字符集等不一致导致的。

## 四、解决方法

1. 如果操作系统版本（A1）检查项检查结果为Abnormal，需要将不属于混编范围的操作系统版本替换为混编范围内的操作系统版本。<br>
2. 如果内核版本（A2）检查项检查结果为Warning，则表示openGauss集群内操作系统平台的内核版本不一致。<br>
3. 如果Unicode状态（A3）检查项检查结果为Abnormal，需要将各主机的字符集设置为相同的字符集，可以在/etc/profile文件中添加“export LANG=XXX”（XXX为Unicode编码）。<br>
4. 如果时区状态（A4）检查项检查结果为Abnormal，需要将各主机的时区设置为相同时区，可以将/usr/share/zoneinfo/目录下的时区文件拷贝为`/etc/localtime`文件。<br>

    ```shell
    cp /usr/share/zoneinfo/$主时区/$次时区 /etc/localtime
    ```

5. 如果交换内存状态（A5）检查项检查结果为Abnormal，可能是因为Swap空间大于Mem空间，可减小Swap解决或者增大Mem空间解决。<br>
6. 如果系统控制参数（A6）检查项检查结果为Abnormal，可以使用以下两种方法进行设置。<br>

    a. 可以使用gs_checkos进行设置。

    ```shell
    gs_checkos -i B1
    ```

    b. 根据错误提示信息，在/etc/sysctl.conf文件中进行设置。然后执行`sysctl -p`使其生效。
7. 如果文件系统配置状态（A7）检查项检查结果为Abnormal，可以使用如下命令进行设置。<br>

    ```shell
    gs_checkos -i B2
    ```

8. 如果磁盘配置状态（A8）检查项检查结果为Abnormal，需修改磁盘挂载格式为：`rw,noatime,inode64,allocsize=16m`，有以下两种方法。<br>

    a. 使用linux的`man mount`命令挂载XFS选项。

    ```shell
    rw,noatime,inode64,allocsize=16m
    ```

    b. 在/etc/fstab文件中设定XFS选项。

    ```shell
    /dev/data /data xfs rw,noatime,inode64,allocsize=16m 0 0
    ```

9. 如果预读块大小（A9）检查项检查结果为Abnormal，可以使用如下命令进行设置。<br>

    ```shell
    gs_checkos -i B3
    ```

10. 如果IO调度状态（A10）检查项检查结果为Abnormal，可以使用如下命令进行设置。<br>

    ```shell
    gs_checkos -i B4
    ```

11. 如果网卡配置状态（A11）检查项检查结果为Warning，可以使用如下命令进行设置。<br>

    ```shell
    gs_checkos -i B5
    ```

12. 如果时间一致性（A12）检查项检查结果为Abnormal，资源池化集群各个主机系统时间差超过6s，非池化环境超过60s，需检查是否安装ntp服务，以及ntp服务是否启动，并与ntp时钟源同步。<br>
13. 如果防火墙状态（A13）检查项检查结果为Abnormal，需关闭防火墙服务。使用如下命令进行设置。

    a. SuSE

    ```shell
    SuSEfirewall2 stop
    ```

    b. RedHat7

    ```shell
    systemctl disable firewalld
    systemctl stop firewalld
    ```

    c. RedHat6

    ```shell
    service iptables stop
    ```

14. 如果THP服务（A14）检查项检查结果为Abnormal，可以使用如下命令进行设置。<br>

    ```shell
    gs_checkos -i B6
    ```
