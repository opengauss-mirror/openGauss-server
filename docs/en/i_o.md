# I/O<a name="EN-US_TOPIC_0245374523"></a>

You can run the  **iostat**  or  **pidstat**  command, or use openGauss heath check tools to check the I/O usage and throughput on each node in openGauss and analyze whether performance bottleneck caused by I/O exists.

## Checking I/O Usage<a name="en-us_topic_0237121488_en-us_topic_0073253548_en-us_topic_0040046485_section49799026113827"></a>

Use one of the following methods to check the server I/O:

- Run the  **iostat**  command to check the I/O usage. This command focuses on the I/O usage and the amount of data read and written on a single hard disk per second.

    ```
    iostat -xm 1  // 1 indicates the interval.
    Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
    sdc               0.01   519.62    2.35   44.10     0.31     2.17   109.66     0.68   14.62    2.80   15.25   0.31   1.42
    sdb               0.01   515.95    5.84   44.78     0.89     2.16   123.51     0.72   14.19    1.55   15.84   0.31   1.55
    sdd               0.02   519.93    2.36   43.91     0.32     2.17   110.16     0.65   14.12    2.58   14.74   0.30   1.38
    sde               0.02   520.26    2.34   45.17     0.31     2.18   107.46     0.80   16.86    2.92   17.58   0.34   1.63
    sda              12.07    15.72    3.97    5.01     0.07     0.08    34.11     0.28   30.64   10.11   46.92   0.98   0.88
    ```

    **rMB/s**  indicates the number of megabytes of data read per second,  **wMB/s**  indicates that of data written per second, and  **%util**  indicates the disk usage.

- Run the  **pidstat**  command to check the I/O usage. This command focuses on the amount of data read and written on a single process per second.

    ```
    pidstat -d 1 10  // 1 indicates the interval and 10 indicates that the top 10 processes with the busiest I/O will be displayed.
    03:17:12 PM   UID       PID   kB_rd/s   kB_wr/s kB_ccwr/s  Command
    03:17:13 PM  1006     36134      0.00  59436.00      0.00  gaussdb
    
    ```

    **kB\_rd/s**  indicates the number of kilobytes of data read per second, and  **kB\_wr/s**  indicates that of data written per second.

- Run the  **gs\_checkperf**  command as user  **omm**  to check the I/O usage in openGauss.

    ```
    gs_checkperf
    Cluster statistics information:
        Host CPU busy time ratio                     :    .69        %
        MPPDB CPU time % in busy time                :    .35        %
        Shared Buffer Hit ratio                      :    99.92      %
        In-memory sort ratio                         :    100.00     %
        Physical Reads                               :    8581
        Physical Writes                              :    2603
        DB size                                      :    281        MB
        Total Physical writes                        :    1944
        Active SQL count                             :    3
        Session count                                :    11
    ```

    The I/O usage, number of reads and writes, and time when data is read and written are displayed.

    You can also run the  **gs\_checkperf --detail**  command to query performance details of each node.

## Analyzing Performance Parameters<a name="en-us_topic_0237121488_en-us_topic_0073253548_en-us_topic_0040046485_section401001449238"></a>

1. Check whether the disk usage exceeds 60%. Disk usage exceeding 60% is called high.

    ```
    df -T
    ```

2. Perform the following operations to reduce I/O usage if the I/O usage keeps high:
    - Reduce the number of concurrent tasks.
    - Do  **VACUUM FULL**  for related tables.

        ```
        vacuum full tablename;
        ```

        >[!NOTE]NOTE   
        >You are advised to do  **VACUUM FULL**  during the system idle time because this operation will cause heavy I/O load in a short period.  **VACUUM FULL**  during the system busy time does not facilitate the I/O decrease.  
