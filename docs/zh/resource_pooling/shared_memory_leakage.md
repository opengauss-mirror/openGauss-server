# 共享内存泄露问题

## 问题现象

日志里出现如下错误：

```
This error usually means that PostgreSQL's request for a shared  memory segment 
exceeded available memory or swap space,  or exceeded your kernel's SHMALL parameter.  
You can either  reduce the request size or reconfigure the kernel with larger SHMALL. 
```

## 原因分析

使用`free`命令查看内存使用情况，发现`shared`内存的确占用了很大一部分。

```
# free -g
              total        used        free      shared  buff/cache   available
Mem:             31           1           2          23         27         2
Swap:             3           3           0
```

使用`ipcs`命令进一步查看共享内存的使用情况，发现存在大量不再被进程使用但未回收的共享内存，即`nattch`为0的部分。

```
[root@pekpeuler00671 script]# ipcs -m

------ Shared Memory Segments --------
key        shmid      owner      perms      bytes      nattch     status
0x00000000 65536      gnome-init 777        16384      1          dest
0x00000000 131073     gnome-init 777        16384      1          dest
0x00000000 163842     gnome-init 777        3145728    2          dest
0x00000000 393219     gnome-init 600        524288     2          dest
0x00000000 425988     gnome-init 600        4194304    2          dest
0x00000000 458757     gnome-init 777        3145728    2          dest
0x00f42401 3604486    1001       600        4455342080 0
0x00f42402 14123015   1003       600        4457177088 0
0x00f42403 23592968   1005       600        4457177088 0
0x00f42404 33062921   1007       600        4457177088 0
0x00f42405 42532874   1009       600        4457177088 0
0x00f42406 52002827   1011       600        4457177088 0
0x00f42407 61472780   1013       600        4457177088 0
0x00f42408 70942733   1015       600        4457177088 0
0x00f42409 80412686   1017       600        4457177088 0
0x00f4240a 89882639   1019       600        4457177088 0
0x00f4240b 99352592   1021       600        4457177088 0
0x00f4240c 108822545  1023       600        4457177088 0
0x00f4240d 118292498  1025       600        4457177088 0
0x00f4240e 127762451  1027       600        4457177088 0
0x00f4240f 136904724  1029       600        4455342080 0
0x00f42410 146374677  1031       600        4457177088 0
0x00f42411 155844630  1033       600        4457177088 0
0x00f42412 165314583  1035       600        4457177088 0
0x00f42413 174784536  1037       600        4457177088 0
```

经过定位，这部分内存是由于使用`kill -9`命令来退出数据库进程，导致没有调用`IpcMemoryDelete`函数来清理共享内存，造成了内存泄漏。

## 处理方法

使用`ipcrm`释放无属主的共享内存，例如，释放`shmid`为`3604486`的共享内存，命令如下所示。

```
ipcrm -m 3604486
```
