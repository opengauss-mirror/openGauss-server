# 因系统资源不足导致gs_probackup中全量备份core的问题

## 一、问题现象

gs_probackup全量备份，高并发10000000，core dumped。  

1. 新建备份目录并初始化：  
`gs_probackup init -B /home/zcz_ct/f_aler_rowcompress_0059` 　　
2. 在备份路径内初始化一个新的备份实例：  
`gs_probackup add-instance -B /home/zcz_ct/f_aler_rowcompress_0059 -D /usr1/zczct_install/omm/openGauss/dn1 --instance=pro1`
3. 执行全量备份：  
`gs_probackup backup -B /home/zcz_ct/f_aler_rowcompress_0059 --instance -b full -d postgres -j 10000000 --instance-id 1`  
4. 实际输出会出现core dumped：  

    ```shell
    [2024-05-16 12:24:18]: keepalive message is received
    [2024-05-16 12:24:23]: keepalive message is received
    [2024-05-16 12:24:28]: keepalive message is received
    [2024-05-16 12:24:33]: keepalive message is received
    [2024-05-16 12:24:38]: keepalive message is received
    Segmentation fault (core dumped)
    ```

## 二、定位方法

1. 通过查看全量备份的错误打印可以发现全量备份的过程被突然终止。

    ```shell
    gs probackup backup -B /home/lyog/backup --instance backup
    b full -d postgres -j 1000000000 --instance-id 1
    INFO: Backup start, gs probackup version: 2.4.2, instance: backup, backup ID:
    SDK7WK, backup mode: FULL, wal mode: STREAM, remote: false, compress algorit
    hm: none, compress-level: 1
    LOG: Backup destination is initialized
    LOG: This openGauss instance was initialized with data block checksums. Data
    block corruption will be detected
    LOG: Database backup start
    LOG: started streaming WAL at 0/80000000 (timeline 1)
    ```

2. 通过代码走读分析发现资源池化场景下限制了`gs_probackup`的线程数量并检测CPU可用核心数量，如果指定的gs_probackup线程数量超过该值的10倍，则直接报错。

## 三、问题根因

全量备份发现通过-j执行多线程，每个线程都要占用一定的资源，如栈空间、cpu等，如果-j参数指定过大，会因为系统资源不足而core掉导致全量备份被终止。所以当指定的线程数量过多，备份的效率与稳定性都会降低。

## 四、解决方案

在main函数中加入`check_threads_num_option`，限制`gs_probackup`线程数量，检测CPU可用核心数量，如果指定的`gs_probackup`线程数量超过该值的10倍，直接报错。
