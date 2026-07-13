# 出现“Error:No space left on device”提示

## 问题现象<a name="section668912633713"></a>

在数据库使用过程中，出现如下错误提示。

```
Error:No space left on device
```

## 原因分析<a name="section1232620121376"></a>

磁盘空间不足造成此提示信息。

## 处理办法<a name="section1643310207377"></a>

- 使用如下命令查看磁盘占用情况。显示信息如下，其中Avail列表示各磁盘可用的空间，Use%列表示已使用的磁盘空间百分比。

```
[root@openeuler123 mnt]# df -h
Filesystem                  Size  Used Avail Use% Mounted on
devtmpfs                    255G     0  255G   0% /dev
tmpfs                       255G   35M  255G   1% /dev/shm
tmpfs                       255G   57M  255G   1% /run
tmpfs                       255G     0  255G   0% /sys/fs/cgroup
/dev/mapper/openeuler-root  196G  8.8G  178G   5% /
tmpfs                       255G  1.0M  255G   1% /tmp
/dev/sda2                   9.8G  144M  9.2G   2% /boot
/dev/sda1                    10G  5.8M   10G   1% /boot/efi
```

    由于业务数据的增长情况不同，对剩余磁盘空间的要求不同。建议如下：
    
    -   持续观察磁盘空间增长情况，确保剩余空间满足一年以上的增长要求。
    
    -   数据目录所在磁盘已使用空间\>60%则进行空间清理或者扩容。

- 使用如下命令查看数据目录大小。

    ```
     du --max-depth=1 -h /mnt/ 
    ```

    显示如下信息，其中第一列表示目录或文件的大小，第二列是“/mnt/”目录下的所有子目录或者文件。

    ```
    [root@openGauss36 mnt]# du --max-depth=1 -h /mnt
    83G /mnt/data3
    71G /mnt/data2
    365G /mnt/data1
    518G /mnt
    ```

- 清理磁盘空间。建议定期将审计日志备份到其他存储设备，推荐的日志保留时长为一个月。pg\_log存放数据库各进程的运行日志，运行日志可以帮助数据库管理员定位数据库的问题。如果每日查看错误日志并及时处理错误，则可以删除这些日志。

- 清理无用的数据。通过先备份使用频率较低或者一定时间以前的数据至更低成本的存储介质中，然后清理这些已备份的数据来获取更多的磁盘空间。

- 如果以上方法无法清理出足够的空间，请对磁盘空间进行扩容。
