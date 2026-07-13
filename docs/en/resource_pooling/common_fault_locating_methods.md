# Common Fault Locating Methods<a name="EN-US_TOPIC_0291553838"></a>

## **Locating OS Faults**<a name="section1115263119119"></a>

If all instances on a node are abnormal, an OS fault may have occurred.

Use one of the following methods to check whether any OS fault occurs:

- Log in to the node using SSH or other remote login tools. If the login fails, run the  **ping**  command to check the network status.

    - If no response is returned, the server is down or being restarted, or its network connection is abnormal.

        The restart takes a long time \(about 20 minutes\) if the system crashes due to an OS kernel panic. Try to connect the host every 5 minutes. If the connection failed 20 minutes later, the server is down or the network connection is abnormal. In this case, contact the administrator to locate the fault on site.

    - If ping operations succeed but SSH login fails or commands cannot be executed, the server does not respond to external connections possibly because system resources are insufficient \(for example, CPU or I/O resources are overloaded\). In this case, try again. If the fault persists within 5 minutes, contact the administrator for further fault locating on site.

- If login is successful but responses are slow, check the system running status, such as collecting system information as well as checking system version, hardware, parameter setting, and login users. The following are common commands for reference:

    - Use the  **who**  command to check online users.

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

    -   Use the  **cat /etc/openEuler-release**  and  **uname -a**  commands to check the system version and kernel information.

```
[root@openGauss36 ~]# cat /etc/openEuler-release 
openEuler release 20.03 (LTS)
[root@openGauss36 ~]# uname -a
Linux openGauss36 4.19.90-2003.4.0.0036.oe1.aarch64 #1 SMP Mon Mar 23 19:06:43 UTC 2020 aarch64 aarch64 aarch64 GNU/Linux
[root@openGauss36 ~]#
```

    -   Use the  **sysctl -a**  \(run this command as user  **root**\) and  **cat /etc/sysctl.conf**  commands to obtain system parameter information.
    -   Use the  **cat /proc/cpuinfo**  and  **cat /proc/meminfo**  commands to obtain CPU and memory information.

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

    -   Use the  **top -H**  command to query the CPU usage and check whether the CPU usage is high due to a specific process. If it is, use the  **gdb**  or  **gstack**  command to print the stack trace of this process and check whether this process is in an infinite loop.
    -   Use the  **iostat -x 1 3**  command to query the I/O usage and check whether the I/O usage of the current disk is full. View the ongoing jobs to determine whether to handle the jobs with high I/O usage.
    -   Use the  **vmstat 1 3**  command to query the memory usage in the current system and use the  **top**  command to obtain the processes with unexpectedly high memory usage.
    -   View the OS logs \(**/var/log/messages**\) or dmseg information as user  **root**  to check whether errors have occurred in the OS.
    -   The watchdog of an OS is a mechanism to ensure that the OS runs properly or exits from the infinite loop or deadlock state. If the watchdog times out \(the default value is 60s\), the system resets.

## **Locating Network Faults**<a name="section1321411501272"></a>

When the database runs normally, the network layer is transparent to upper-layer users. However, during the long-term operation of a database cluster, network exceptions or errors may occur. Common exceptions caused by network faults are as follows: 

- Network error reported due to database startup failure.
- Abnormal status, for example, all instances on a host are in the  **UnKnown**  state, or all services are switched over to standby instances.
- Network connection failure.
- Network disconnection reported during database sql query.
- Process response failures during database connection or query execution. When a network fault occurs in a database, locate and analyze the fault by using network-related Linux command tools \(such as  **ping**,  **ifconfig**,  **netstat**, and  **lsof**\) and process stack viewers \(such as  **gdb**  and  **gstack**\) based on database log information. This section lists common network faults and describes how to analyze and locate faults.

Common faults are as follows:

- Network error reported due to a startup failure

    **Symptom 1**: The log contains the following error information. The port may be listened on by another process.

    ```
    LOG: could not bind socket at the 10 time, is another postmaster already running on port 54000?
    ```

    **Solution**: Run the following command to check the process that listens on the port. Replace the port number with the actual one.

    ```
    [root@openGauss36 ~]# netstat -anop | grep 15970
    tcp        0      0 127.0.0.1:15970         0.0.0.0:*               LISTEN      3920251/gaussdb      off (0.00/0/0)
    tcp6       0      0 ::1:15970               :::*                    LISTEN      3920251/gaussdb      off (0.00/0/0)
    unix  2      [ ACC ]     STREAM     LISTENING     197399441 3920251/gaussdb      /tmp/.s.PGSQL.15970
    unix  3      [ ]         STREAM     CONNECTED     197461142 3920251/gaussdb      /tmp/.s.PGSQL.15970
    
    ```

    Forcibly stop the process that is occupying the port or change the listening port of the database based on the query result.

    **Symptom 2**: When the  **gs\_om -t status --detail**  command is used to query status, the command output shows that the connection between the primary and standby nodes is not established.

    **Solution**: In openEuler, run the  **systemctl status firewalld.service**  command to check whether the firewall is enabled on this node. If it is enabled, run the  **systemctl stop firewalld.service**  command to disable it.

    ```
    [root@openGauss36 mnt]# systemctl status firewalld.service
    ●firewalld.service - firewalld - dynamic firewall daemon
       Loaded: loaded (/usr/lib/systemd/system/firewalld.service; disabled; vendor preset: enabled)
       Active: inactive (dead)
         Docs: man:firewalld(1)
    ```

    The command varies according to the operating system. You can run the corresponding command to view and modify the configuration.

- The database is abnormal.

    **Symptom**: The following problems occur on a node:

    - All instances are in the  **Unknown**  state.
    - All primary instances are switched to standby instances.
    - Errors "Connection reset by peer" and "Connection timed out" are frequently displayed.

    **Solution**

    - If you cannot connect to the faulty server through SSH, run the  **ping**  command on other servers to send data packages to the faulty server. If the ping operation succeeds, connection fails because resources such as memory, CPUs, and disks, on the faulty server are used up.
    - Connect to the faulty server through through SSH and run the  **/sbin/ifconfig eth**_?_  command every other second \(replace the question mark \(?\) with the number indicating the position of the NIC\). Check value changes of  **dropped**  and  **errors**. If they increase rapidly, the NIC or NIC driver may be faulty.

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

    - Check whether the following parameters are correctly configured:

        ```
        net.ipv4.tcp_retries1 = 3
        net.ipv4.tcp_retries2 = 15
        ```

- Network connection failure.

    **Symptom 1**: A node fails to connect to other nodes, and the "Connection refused" error is reported in the log.

    **Solution**

    - Check whether the port is incorrectly configured, resulting in that the port used for connection is not the listening port of the peer end. Check whether the port number recorded in the  **postgresql.conf**  configuration file of the faulty node is the same as the listening port number of the peer end.
    - Check whether the peer listening port is normal \(for example, by running the  **netstat –anp**  command\).
    - Check whether the peer process exists.

    **Symptom 2**: When SQL operations are performed on the database, the connection descriptor fails to be obtained. The following error information is displayed:

    ```
    WARNING:  29483313: incomplete message from client:4905,9 
    WARNING:  29483313: failed to receive connDefs at the time:1. 
    ERROR:  29483313: failed to get pooled connections 
    ```

    In logs, locate and view the log content before the preceding error messages, which are generated due to incorrect active and standby information. Error details are displayed as follows.

    ```
    FATAL:  dn_6001_6002: can not accept connection in pending mode. 
    FATAL:  dn_6001_6002: the database system is starting up 
    FATAL:  dn_6009_6010: can not accept connection in standby mode.
    ```

    **Solution**

    - Run the  **gs\_om -t status --detail**  command to query the status and check whether an primary/standby switchover has occurred. Reset the instance status.
    - In addition, check whether a core dump or restart occurs on the node that fails to be connected. In the om log, check whether restart occurs.

- Network disconnection reported during database sql query.

    **Symptom 1**: The query fails, and the following error information is displayed:

    ```
    ERROR:  dn_6065_6066: Failed to read response from Datanodes. Detail: Connection reset by peer. Local: dn_6065_6066 Remote: dn_6023_6024
    ERROR: Failed to read response from Datanodes Detail: Remote close socket unexpectedly
    ERROR: dn_6155_6156: dn_6151_6152: Failed to read vector response from Datanodes
    ```

    If the connection fails, the error information may be as follows:

    ```
    ERROR:  Distribute Query unable to connect 10.145.120.79:14600 [Detail:stream connect connect() fail: Connection timed out
    ERROR: Distribute Query unable to connect 10.144.192.214:12600 [Detail:receive accept response fail: Connection timed out 
    ```

    **Solution**

    1. Use  **gs\_check**  to check whether the network configuration meets requirements. For network check, see "Server Tools \> gs\_check" in the  _Tool Reference_.

    2. Check whether a process core dump, restart, or switchover occurs.

    3. If problems still exist, contact network technical engineers.

## **Locating Disk Faults**<a name="section1548511952213"></a>

Common disk faults include insufficient disk space, bad blocks of disks, and unmounted disks. Disk faults such as unmount of disks damage the file system. The cluster management mechanism identifies this kind of faults and stops the instance, and the instance status is  **Unknown**. However, disk faults such as insufficient disk space do not damage the file system. The cluster management mechanism cannot identify this kind of faults and service processes exit abnormally when accessing a faulty disk. Failures cover database startup, checksum verification, page read and write operation, and page verification.

- For faults that result in file system damages, the instance status is  **Unknown**  when you view the host status. Perform the following operations to locate the disk fault:

    - Check the logs. If the logs contain information similar to "data path disc writable test failed", the file system is damaged.
    - The possible cause of file system damage may be unmounted disks. Run the  **ls –l**  command and you can view that the disk directory permission is abnormal, as shown in the following:
    - Another possible cause is that the disk has bad blocks. In this case, the OS rejects read and write operations to protect the file system. You can use a bad block check tool, for example,  **badblocks**, to check whether bad blocks exist.

        ```
        [root@openeuler123 mnt]# badblocks /dev/sdb1 -s -v
        Checking blocks 0 to 2147482623
        Checking for bad blocks (read-only test): done                                                 
        Pass completed, 0 bad blocks found. (0/0/0 errors)
        ```

- For faults that do not damage the file system, the service process will report an exception and exit when it accesses the faulty disk. Perform the following operations to locate the disk fault:

    View logs. The log contains read and write errors, such as "No space left on device" and "invalid page header n block 122838 of relation base/16385/152715". Run the  **df -h**  command to check the disk space. If the disk usage is 100% as shown below, the read and write errors are caused by insufficient disk space:

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

## **Locating Database Faults**<a name="section174283862218"></a>

- Logs. Database logs record the operations \(starting, running, and stopping\) on servers. Database users can view logs to quickly locate fault causes and rectify the faults accordingly.

- View. A database provides different views to display its internal status. When locating a fault, you can use:

    - **pg\_stat\_activity**: shows the status of each session on the current instance.

    - **pg\_thread\_wait\_status**: shows the wait events of each thread on the current instance.

    - **pg\_locks**: shows the status of locks on the current instance.

- Core files. Abnormal termination of a database process will trigger a core dump. A core dump file helps locate faults and determine fault causes. Once a core dump occurs during process running, collect the core file immediately for further analyzing and locating the fault.

    - The OS performance is affected, especially when errors occur frequently.

    - The OS disk space will be occupied by core files. Therefore, after core files are discovered, locate and rectify the errors as soon as possible. The OS is delivered with a core dump mechanism. If this mechanism is enabled, core files are generated for each core dump, which has an impact on the OS performance and disk space.

    - Set the path for generating core files. Modify the  **/proc/sys/kernel/core\_pattern**  file.

```
[root@openeuler123 mnt]# cat /proc/sys/kernel/core_pattern
/data/jenkins/workspace/openGaussInstall/dbinstall/cluster/corefile/core-%e-%p-%t
```
