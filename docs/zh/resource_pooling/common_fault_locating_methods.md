# 常见故障定位手段

## 操作系统故障定位手段<a name="section1115263119119"></a>

查询状态时，显示一个节点上所有实例都不正常时，可能是操作系统发生了故障。

可以通过如下方法确定操作系统是否存在问题：

- 通过SSH或者其它远程登录工具登录该节点。如果连接失败，请尝试通过ping发包检查网络状态。

    - 如果ping操作没有回复，则表明这台机器可能存在网络连接故障、处于宕机状态或者正处于重启状态。

        如果操作系统内核发生panic引起系统崩溃，系统重新启动时间较慢，需经过较长时间（大约20分钟）才能重启。建议每5分钟尝试连接一次，20分钟后不能连接成功，则表明这台机器已宕机或网络连接有问题，需要管理员到现场进行检查处理。

    - 如果网络可以ping通，但在SSH登入时卡住或登入后不能执行任何命令，通常是由系资源不足（如CPU或IO资源过载）引起的机器不响应外部连接。建议重试几次。如果5分钟内仍不能成功，需要管理员到现场进行检查处理。

- 可以远程登录节点，但在执行操作时，响应缓慢，需要检查系统运行情况后，进行进一步处理。例如，收集系统信息、确定系统版本、硬件、参数设置及登录用户情况。下面列出一些常用命令供参考。

    - “who”命令查看当前在线用户。

```
[root@openGauss36 ~]# who
root     pts/0        2020-11-07 16:32 (10.70.223.238)
wyc      pts/1        2020-11-10 09:54 (10.70.223.222)
root     pts/2        2020-10-10 14:20 (10.70.223.238)
root     pts/4        2020-10-09 10:14 (10.70.223.233)
root     pts/5        2020-10-09 10:14 (10.70.223.233)
root     pts/7        2020-10-31 17:03 (10.70.223.222)
root     pts/9        2020-10-20 10:03 (10.70.220.85)
```

    -   “cat /etc/openEuler-release”和“uname -a”命令检查系统的版本和内核信息。

```
[root@openGauss36 ~]# cat /etc/openEuler-release 
openEuler release 20.03 (LTS)
[root@openGauss36 ~]# uname -a
Linux openGauss36 4.19.90-2003.4.0.0036.oe1.aarch64 #1 SMP Mon Mar 23 19:06:43 UTC 2020 aarch64 aarch64 aarch64 GNU/Linux
[root@openGauss36 ~]#
```

    -   “sysctl -a”命令（需要root用户执行）和“cat /etc/sysctl.conf”命令获得系统参数信息。
    -   “cat /proc/cpuinfo”和“cat /proc/meminfo”获得CPU和内存信息。
    
        ```
        [root@openGauss36 ~]# cat /proc/cpuinfo
        processor : 0
        BogoMIPS : 200.00
        Features : fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp cpuid asimdrdm jscvt fcma dcpop asimddp asimdfhm
        CPU implementer : 0x48
        CPU architecture: 8
        CPU variant : 0x1
        CPU part : 0xd01
        CPU revision : 0
        [root@openGauss36 ~]# cat /proc/meminfo
        MemTotal:       534622272 kB
        MemFree:        253322816 kB
        MemAvailable:   369537344 kB
        Buffers:         2429504 kB
        Cached:         253063168 kB
        SwapCached:            0 kB
        Active:         88570624 kB
        Inactive:       171801920 kB
        Active(anon):    4914880 kB
        Inactive(anon): 67011456 kB
        Active(file):   83655744 kB
        Inactive(file): 104790464 kB
        ```
    
    -   “top -H”命令查看CPU的使用情况，确定是否因为某个进程导致CPU使用率过高。如果存在这种情况，通过gdb或gstack打印该程序堆栈，观察是否该程序处于死循环逻辑。
    -   “iostat -x 1 3”命令查看IO的使用情况，确定是否当前磁盘的IO处于饱和状态。查看当前运行的执行作业情况，决定是否对占用较多IO的执行作业进行处理。
    -   “vmstat 1 3”命令查看当前系统中内存的消耗情况，结合“top”命令获得消耗内存较多的进程，处于超出预期的状态。
    -   以root用户查看操作系统日志信息（/var/log/messages）或dmseg信息，检查操作系统是否发生过异常错误。
    -   操作系统的watchdog是为了保证OS系统正常运行，或者从死循环，死锁等状态退出的一种机制，如果watchdog超时（一般默认值为60s），系统将会复位。

## 网络故障定位手段<a name="section1321411501272"></a>

在数据库正常工作的情况下，网络层对上层用户是透明的，但数据库在长期运行时，可能会由于各种原因导致出现网络异常或错误。常见的因网络故障引发的异常有：

- 数据库启动失败，报网络错误。
- 状态异常，如：节点上所有的实例都是UnKnown或者所有主机都切换为备机。
- 网络连接建立失败。
- 对数据库执行SQL操作时，报网络异常中断的错误。
- 连接数据库或执行查询时发生进程停止响应。数据库出现了网络故障后，主要通过使用Linux系统提供的网络相关命令工具（ping、ifconfig、netstat、lsof等），进程堆栈查看工具（gdb、gstack），结合数据库的日志信息，进行分析定位。本节通过举例介绍常见的网络问题，并进行基本的分析定位。

常见故障问题如下：

- 启动失败，报网络错误

    **问题现象1**：日志中存在如下错误信息。可能是端口被其他进程侦听。

    ```
    LOG: could not bind socket at the 10 time, is another postmaster already running on port 54000?
    ```

    **处理办法**：执行如下命令查看侦听该端口的进程。端口号请根据实际端口号替换。

    ```
    [root@openGauss36 ~]# netstat -anop | grep 15970
    tcp        0      0 127.0.0.1:15970         0.0.0.0:*               LISTEN      3920251/gaussdb      off (0.00/0/0)
    tcp6       0      0 ::1:15970               :::*                    LISTEN      3920251/gaussdb      off (0.00/0/0)
    unix  2      [ ACC ]     STREAM     LISTENING     197399441 3920251/gaussdb      /tmp/.s.PGSQL.15970
    unix  3      [ ]         STREAM     CONNECTED     197461142 3920251/gaussdb      /tmp/.s.PGSQL.15970
    
    ```

    根据查询结果，强行停止正在占用端口的进程或者更改数据库侦听端口。

    **问题现象2**：使用gs\_om -t status --detail 查询状态，如果显示主备间连接未建立。

    **处理办法**：在openEuler操作系统下，使用systemctl status firewalld.service命令，查看节点上是否开启了防火墙。如果开启，使用systemctl stop firewalld.service关闭防火墙。

    ```
    [root@openGauss36 mnt]# systemctl status firewalld.service
    ●firewalld.service - firewalld - dynamic firewall daemon
       Loaded: loaded (/usr/lib/systemd/system/firewalld.service; disabled; vendor preset: enabled)
       Active: inactive (dead)
         Docs: man:firewalld(1)
    ```

    操作系统不同，命令可能不同，使用对应操作系统命令查看修改。

- 数据库状态异常

    **问题现象**：某一节点上出现如下问题：

    - 所有实例都是UnKnown。
    - 所有主实例都切换成了备实例。
    - 查询中出现大量Connection reset by peer，Connection timed out等报错信息。

    **处理办法**

    - 如果ssh不能连接故障机器，在其他机器上使用Ping命令向该机器发数据包。如果可以Ping通，说明可能是该机器上的资源（内存、CPU、磁盘）耗尽导致不能建立连接。
    - 如果ssh可以连接该机器，尝试执行查询，并每隔1s执行/sbin/ifconfig eth?（?代表数字，表示第几个网卡）命令，查看如下信息中的dropped及errors值的变化情况，如果增长迅速，可能是网卡或网卡驱动故障。

        ```
        [root@openGauss36 ~]# ifconfig enp125s0f0
        enp125s0f0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
                inet 10.90.56.36  netmask 255.255.255.0  broadcast 10.90.56.255
                inet6 fe80::7be7:8038:f3dc:f916  prefixlen 64  scopeid 0x20<link>
                ether 44:67:47:7d:e6:84  txqueuelen 1000  (Ethernet)
                RX packets 129344246  bytes 228050833914 (212.3 GiB)
                RX errors 0  dropped 647228  overruns 0  frame 0
                TX packets 96689431  bytes 97279775245 (90.5 GiB)
                TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
        ```

    - 检查如下参数设置是否正确。

        ```
        net.ipv4.tcp_retries1 = 3
        net.ipv4.tcp_retries2 = 15
        ```

- 网络连接建立失败

    **问题现象1**：节点连接其他节点失败，日志中报出“ Connection refused ”错误。

    **处理办法**：

    - 查看端口是否配置错误，导致连接时使用的端口并非对方侦听的端口。查看报错节点配置文件postgresql.conf记录端口号与对方侦听的端口就号是否一致。
    - 查看对方端口侦听是否正常（可以使用“netstat –anp”命令）。
    - 查看对方进程是否存在。

    **问题现象2**：对数据库执行SQL操作时，获取连接描述符失败，报错如下。

    ```
    WARNING:  29483313: incomplete message from client:4905,9 
    WARNING:  29483313: failed to receive connDefs at the time:1. 
    ERROR:  29483313: failed to get pooled connections 
    ```

    在日志中，找到上面的错误，并向上查看一段日志内容，可以看到详细的错误信息，常见的错误如下所示，主要是由于主备信息不正确导致。

    ```
    FATAL:  dn_6001_6002: can not accept connection in pending mode. 
    FATAL:  dn_6001_6002: the database system is starting up 
    FATAL:  dn_6009_6010: can not accept connection in standby mode.
    ```

    **处理办法：**

    - 使用gs\_om -t status --detail查询状态，确认是否发生过主备切换。重置实例状态。
    - 此外，需要查看连接失败的节点是否发生了core或者重启。通过om日志可以查看到是否发生了重启。

- 对数据库执行 SQL 操作时，报网络异常中断的错误

    **问题现象1**：查询执行失败，报错信息如下。

    ```
    ERROR:  dn_6065_6066: Failed to read response from Datanodes. Detail: Connection reset by peer. Local: dn_6065_6066 Remote: dn_6023_6024
    ERROR: Failed to read response from Datanodes Detail: Remote close socket unexpectedly
    ERROR: dn_6155_6156: dn_6151_6152: Failed to read vector response from Datanodes
    ```

    连接建立失败，报错信息如下。

    ```
    ERROR:  Distribute Query unable to connect 10.145.120.79:14600 [Detail:stream connect connect() fail: Connection timed out
    ERROR: Distribute Query unable to connect 10.144.192.214:12600 [Detail:receive accept response fail: Connection timed out 
    ```

    **处理办法**：

    1. 使用gs\_check检查网络配置是否符合标准。详细参考《工具与命令参考》中“服务端工具 \> gs\_check”章节中对network的检查。

    2. 查看是否有进程发生core或重启，以及主备切换。

    3. 如果不存在上述问题，可以联系网络技术人员做具体分析。

## 磁盘故障定位手段<a name="section1548511952213"></a>

常见的磁盘故障是磁盘空间不足、磁盘出现坏块、磁盘未挂载等。部分磁盘故障会导致文件系统损坏，例如磁盘未挂载，数据库管理自动定期执行磁盘检测时会识别故障并将实例停止，查看数据库状态时对应实例状态异常；部分磁盘故障不会导致文件系统损坏，例如磁盘空间不足，数据库管理无法检测到，服务进程访问到故障磁盘会异常退出，例如数据库无法启动、checksum校验不对、页面读写失败、页面校验错误等。

- 对于会导致文件系统损坏的故障，查看状态会显示对应实例状态持续为Unknown，定位方法如下：

    - 查看日志，日志中会有类似 “data path disc writable test failed”异常，说明文件系统已损坏。
    - 文件系统损坏可能是磁盘未挂载，通过ls –l可以看到该磁盘对应的目录权限异常，如下。
    - 也可能是磁盘出现坏块，然后操作系统将文件系统保护起来，拒绝读写，可以使用磁盘坏块检查工具如badblocks检查磁盘是否有坏块，如下。

        ```
        [root@openeuler123 mnt]# badblocks /dev/sdb1 -s -v
        Checking blocks 0 to 2147482623
        Checking for bad blocks (read-only test): done                                                 
        Pass completed, 0 bad blocks found. (0/0/0 errors)
        ```

- 对于不会导致文件系统损坏的故障，服务进程访问到故障磁盘会异常退出，定位方法如下。

    查看日志。日志中会有文件读写错误，例如“No space left on device”、“ invalid page header n block 122838 of relation base/16385/152715”。 文件读写错误可能是磁盘空间不足，通过df -h可以看到磁盘空间已达100%，如下。

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
    /dev/mapper/openeuler-home  1.5T   69G  1.4T   5% /home
    tmpfs                        51G     0   51G   0% /run/user/0
    tmpfs                        51G     0   51G   0% /run/user/1004
    /dev/sdb1                   2.0T  169G  1.9T   9% /data
    ```

## 数据库故障定位手段<a name="section174283862218"></a>

- 日志。数据库日志记录了数据库服务端启动、运行或停止时出现的问题，当数据库在启动、运行或停止的过程中出现问题时，数据库用户可以通过运行日志快速分析问题的产生原因，并根据不同的原因采取相应的处理方法，尽可能地解决问题。

- 视图。数据库提供了许多视图，用于展示数据库的内部状态，在定位故障时，经常使用的视图如下：

    - pg\_stat\_activity，用于查询当前实例上各个session的状态。

    - pg\_thread\_wait\_status，用于查询该实例上各个线程的等待事件。

    - pg\_locks，用于查询当前实例上的锁状态。

- CORE文件。数据库相关进程在运行过程中可能会因为各种意外情况导致数据库崩溃（Coredump），而崩溃时产生的core文件对于迅速定位程序崩溃的原因及位置非常重要。如果进程运行时出现Coredump现象，建议立即收集core文件便于分析、定位故障。

    - 对性能有一定的影响，尤其是进程频繁异常时对性能的影响更大。

    - core文件会占用磁盘空间。因此，当检查到core文件产生后，应及时解决以避免对操作系统带来更严重的影响。操作系统自带core dump机制。开启后，系统中所有出现Coredump问题时都会生成core文件，对操作系统带来性能和磁盘占用的影响。

    - 设置core文件生成路径。修改/proc/sys/kernel/core\_pattern内容。

```
[root@openeuler123 mnt]# cat /proc/sys/kernel/core_pattern
/data/jenkins/workspace/openGaussInstall/dbinstall/cluster/corefile/core-%e-%p-%t
```
