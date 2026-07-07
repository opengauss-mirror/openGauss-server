# 内存<a name="ZH-CN_TOPIC_0289900635"></a>

通过top命令查看openGauss节点内存使用情况，分析是否存在由于内存占用率过高导致的性能瓶颈。

## 查看内存状况<a name="zh-cn_topic_0283137367_zh-cn_topic_0237121487_zh-cn_topic_0073253547_zh-cn_topic_0040046523_section31350235191024"></a>

查询服务器内存的使用情况主要通过以下方式：

执行**top**命令，查看内存占用情况。执行该命令后，按“Shift+M”键，可按照内存大小排序。

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

分析时，请主要关注gaussdb进程占用的内存百分比（%MEM）、整系统的剩余内存。

显示信息中的主要属性解释如下：

- total：物理内存总量。
- used：已使用的物理内存总量。
- free：空闲内存总量。
- buffers：缓存的内存量。
- %MEM：进程占用的内存百分比。
- VIRT：进程申请的虚拟内存总量。
- SWAP：进程使用的交换分区的总量。
- RES：进程使用的物理内存总量。
- SHR：共享内存大小。

## 性能参数分析<a name="zh-cn_topic_0283137367_zh-cn_topic_0237121487_zh-cn_topic_0073253547_zh-cn_topic_0040046523_section4615314285845"></a>

1. 以root用户执行“free”命令查看cache的占用情况。

    ```
    free
    ```

    查询结果如下所示：

    ```
                 total       used       free     shared    buffers     cached
    Mem:       8038844    6336184    1702660          0     375896    2880912
    -/+ buffers/cache:    3079376    4959468
    Swap:      4192924          0    4192924
    ```

2. 若“cache”占用过高，请导入环境变量后执行如下命令清除缓存。

    ```
    /sbin/sysctl -w vm.drop_caches=3
    ```

3. 若用户内存占用过高，需查看执行计划，重点分析以下内容。
    - 是否有不合理的join顺序。例如，多表关联时，执行计划中优先关联的两表的中间结果集比较大，导致最终执行代价比较大。
