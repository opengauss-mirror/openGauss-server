# 操作系统配置<a name="ZH-CN_TOPIC_0289900544"></a>

1. 安装openEuler操作系统，具体请参见[《openEuler 安装指南》](https://openeuler.org/zh/docs/20.03_LTS/docs/Installation/installation.html)。
2. 修改操作系统内核PAGESIZE为64KB。
    1. 查看操作系统内核PAGESIZE的值。

        以root用户登录操作系统，执行如下命令查看PAGESIZE的值。

        ```
        getconf PAGESIZE
        ```

        如[图1](#zh-cn_topic_0283137217_zh-cn_topic_0263913267_fig1277156123020)所示，表示操作系统内核PAGESIZE为64KB。

        **图 1**  查看操作系统内核PAGESIZE的值<a name="zh-cn_topic_0283137217_zh-cn_topic_0263913267_fig1277156123020"></a>  
        ![](figures/查看操作系统内核PAGESIZE的值.png)

    2. 如果PAGESIZE不为64KB，需要修改参数并重新编译内核，详情请参考[https://bbs.huaweicloud.com/blogs/154313](https://bbs.huaweicloud.com/blogs/154313)。

3. 关闭CPU中断的服务irqbalance。irqbalance负责均衡CPU中断，避免单CPU处理中断负载过重。

    以root用户登录操作系统，执行如下命令。

    ```
    service irqbalance stop   # 关闭Irq balance。
    echo 0 > /proc/sys/kernel/numa_balancing    
    echo 'never' > /sys/kernel/mm/transparent_hugepage/enabled 
    echo 'never' > /sys/kernel/mm/transparent_hugepage/defrag 
    echo none > /sys/block/nvme*n*/queue/scheduler  # 针对nvme磁盘io队列调度机制设置。
    ```
