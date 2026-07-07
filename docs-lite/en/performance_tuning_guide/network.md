# Network<a name="EN-US_TOPIC_0289900966"></a>

You can run the  **sar**  or  **ifconfig**  command to check the network status on each node in openGauss and analyze whether performance bottlenecks caused by network faults occur.

## Checking Network Status<a name="en-us_topic_0283137150_en-us_topic_0237121489_en-us_topic_0073253549_en-us_topic_0040046507_section31965288163424"></a>

Check the server network status using either of the following two methods:

- Log in to the server as user  **root**  and run the following commands to check the network connection:

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

    - **errors**  indicates the total number of error packets received.
    - **dropped**  indicates the number of packets that have reached the ring buffer but are discarded before being copied to the memory due to system faults, for example, insufficient memory.
    - **overruns**  indicates the number of packets that have been discarded from the ring buffer. They are discarded because the kernel is incapable of processing ring buffer \(a.k.a. Driver Queue\) transmission.

    In the command output, if the values of the three parameters keep increasing, the network is overloaded or hardware \(such as NICs and memory\) faults exist.

- Run the  **sar**  command to check the network connection.

    ```
    sar -n DEV 1  // 1 indicates the interval.
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

    **rxkB/s**  indicates the number of kilobytes of data received per second and  **txkB/s**  indicates the number of those sent per second.

    In the command output, check whether the amount of data received and sent by any NIC has reached the upper limit.

    After the check, press  **Ctrl+Z**  to exit.
