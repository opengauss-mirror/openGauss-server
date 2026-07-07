# Memory<a name="EN-US_TOPIC_0289900635"></a>

Run the  **top**  command to check the memory usage of each node in openGauss and analyze whether a performance bottleneck occurs due to high memory usage.

## Checking Memory Usage<a name="en-us_topic_0283137367_en-us_topic_0237121487_en-us_topic_0073253547_en-us_topic_0040046523_section31350235191024"></a>

You can query the memory usage of the server in the following ways:

Run the  **top**  command to check the memory usage. Then, press  **Shift+M**  to sort memory on each node by memory size.

```
top - 11:38:26 up 2 days, 17:59, 10 users,  load average: 0.01, 0.05, 0.15
Tasks: 685 total,   1 running, 684 sleeping,   0 stopped,   0 zombie
%Cpu(s):  0.2 us,  0.2 sy,  0.0 ni, 99.7 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st
KiB Mem : 19740646+total, 23503420 free, 15947100 used, 15795595+buff/cache
KiB Swap:  8242172 total,  8242172 free,        0 used. 13366219+avail Mem

  PID USER PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND
29838 omm  20   0 1373104 456904 175248 S   3.6  0.2  98:53.16 gaussdb
27789 omm  20   0  150732   4136   3216 S   0.0  0.0   0:00.00 gsql
45659 omm  20   0  117164   4052   1860 S   0.0  0.0   0:00.24 bash
 8087 omm  20   0  117164   4000   1848 S   0.0  0.0   0:00.05 bash
27459 omm  20   0  117160   4000   1848 S   0.0  0.0   0:00.04 bash
33619 omm  20   0  117120   3852   1740 S   0.0  0.0   0:00.04 bash
27282 omm  20   0  117120   3840   1728 S   0.0  0.0   0:00.03 bash
 9923 omm  20   0  158064   2932   1612 R   0.3  0.0   0:00.04 top
```

In the command output, focus on the memory usage \(**%MEM**\) occupied by the processes and the free memory of the system.

The parameters in the command output are described as follows:

- **total**: total physical memory
- **used**: used physical memory
- **free**: free memory
- **buffers**: memory used for buffers.
- **%MEM**: usage of the memory used by a process
- **VIRT**: virtual memory that a process has applied for.
- **SWAP**: swap partitions used by a process.
- **RES**: physical memory used by a process.
- **SHR**: size of the shared memory

## Analyzing Performance Parameters<a name="en-us_topic_0283137367_en-us_topic_0237121487_en-us_topic_0073253547_en-us_topic_0040046523_section4615314285845"></a>

1. Run the following command as user  **root**  to check the cache usage:

    ```
    free
    ```

    The command output is similar to the following:

    ```
                 total       used       free     shared    buffers     cached
    Mem:       8038844    6336184    1702660          0     375896    2880912
    -/+ buffers/cache:    3079376    4959468
    Swap:      4192924          0    4192924
    ```

2. If the cache usage is too high, run the following command to clear cache:

    ```
    /sbin/sysctl -w vm.drop_caches=3
    ```

3. You need to check the execution plan if the memory usage occupied is too high. Focus on the following items:
    - Whether improper  **JOIN**  sequences exist. For example, if  **JOIN**  is performed for multiple tables, the intermediate result set of the two tables associated preferentially is large during the execution plan execution, resulting in that the final execution cost is high.
