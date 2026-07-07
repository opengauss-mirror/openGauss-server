# 网络

通过sar或ifconfig命令查看openGauss内节点网络使用情况，分析是否存在由于网络导致的性能瓶颈。

## 查看网络状况<a name="zh-cn_topic_0237121489_zh-cn_topic_0073253549_zh-cn_topic_0040046507_section31965288163424"></a>

查询服务器网络状况的方法主要有以下两种方式：

- 使用root用户身份登录服务器，执行如下命令查看服务器网络连接。

    ```
    SIA1000056771:~ # ifconfig
    eth0      Link encap:Ethernet  HWaddr 28:6E:D4:86:7D:D5  
              inet addr:10.180.123.163  Bcast:10.180.123.255  Mask:255.255.254.0
              inet6 addr: fe80::2a6e:d4ff:fe86:7dd5/64 Scope:Link
              UP BROADCAST RUNNING MULTICAST  MTU:1500  Metric:1
              RX packets:5669314 errors:0 dropped:0 overruns:0 frame:0
              TX packets:4955927 errors:0 dropped:0 overruns:0 carrier:0
              collisions:0 txqueuelen:1000 
              RX bytes:508077795 (484.5 Mb)  TX bytes:818004366 (780.1 Mb)
    
    lo        Link encap:Local Loopback  
              inet addr:127.0.0.1  Mask:255.0.0.0
              inet6 addr: ::1/128 Scope:Host
              UP LOOPBACK RUNNING  MTU:16436  Metric:1
              RX packets:711938 errors:0 dropped:0 overruns:0 frame:0
              TX packets:711938 errors:0 dropped:0 overruns:0 carrier:0
              collisions:0 txqueuelen:0 
              RX bytes:164158862 (156.5 Mb)  TX bytes:164158862 (156.5 Mb)
    ```

    - “errors”表示收包错误的总数量。
    - “dropped”表示数据包已经进入了Ring Buffer，但是由于内存不够等系统原因，导致在拷贝到内存的过程中被丢弃的总数量。
    - “overruns”表示Ring Buffer队列中被丢弃的报文数目，由于Ring Buffer\(aka Driver Queue\)传输的IO大于openGauss能够处理的IO导致。

    分析时，如果发现上述三个值持续增长，则表示网络负载过大或者存在网卡、内存等硬件故障。

- 使用sar命令查看服务器网络连接。

    ```
    sar -n DEV 1  //1为间隔时间
    Average: IFACE  rxpck/s  txpck/s    rxkB/s    txkB/s rxcmp/s txcmp/s rxmcst/s %ifutil
    Average:    lo  1926.94  1926.94  25573.92  25573.92    0.00    0.00     0.00    0.00
    Average:  A1-0     0.00     0.00      0.00      0.00    0.00    0.00     0.00    0.00
    Average:  A1-1     0.00     0.00      0.00      0.00    0.00    0.00     0.00    0.00
    Average:  NIC0     5.17     1.48      0.44      0.92    0.00    0.00     0.00    0.00
    Average:  NIC1     0.00     0.00      0.00      0.00    0.00    0.00     0.00    0.00
    Average:  A0-0  8173.06 92420.66  97102.22 133305.09    0.00    0.00     0.00    0.00
    Average:  A0-1 11431.37  9373.06 156950.45    494.40    0.00    0.00     0.00    0.00
    Average:  B3-0     0.00     0.00      0.00      0.00    0.00    0.00     0.00    0.00
    Average:  B3-1     0.00     0.00      0.00      0.00    0.00    0.00     0.00    0.00
    ```

    “rxkB/s”为每秒接收的kB数，“txkB/s”为每秒发送的kB数。

    分析时，请主要关注每个网卡的传输量和是否达到传输上限。

    检查完后，按“Ctrl+Z”键退出查看。
