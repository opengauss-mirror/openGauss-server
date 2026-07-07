# CPU<a name="ZH-CN_TOPIC_0289900497"></a>

通过top命令查看openGauss内节点CPU使用情况，分析是否存在由于CPU负载过高导致的性能瓶颈。
top命令经常用来监控linux的系统状况，是常用的性能分析工具，能够实时显示系统中各个进程的资源占用情况。
参数解释：
-d：number代表秒数，表示top命令显示的页面更新一次的间隔。默认是5秒。 -b：以批次的方式执行top。 -n：与-b配合使用，表示需要进行几次top命令的输出结果。 -p：指定特定的pid进程号进行观察。

## 查看CPU状况<a name="zh-cn_topic_0283136982_zh-cn_topic_0237121486_zh-cn_topic_0073253546_zh-cn_topic_0040046498_section5417561019132"></a>

查询服务器CPU的使用情况主要通过以下方式：

在所有存储节点，逐一执行**top**命令，查看CPU占用情况。执行该命令后，按“1”键，可查看每个CPU核的使用率。

```
top - 17:05:04 up 32 days, 20:34,  5 users,  load average: 0.02, 0.02, 0.00
Tasks: 124 total,   1 running, 123 sleeping,   0 stopped,   0 zombie
Cpu0  :  0.0%us,  0.3%sy,  0.0%ni, 69.7%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Cpu1  :  0.3%us,  0.3%sy,  0.0%ni, 69.3%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Cpu2  :  0.3%us,  0.3%sy,  0.0%ni, 69.3%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Cpu3  :  0.3%us,  0.3%sy,  0.0%ni, 69.3%id,  0.0%wa,  0.0%hi,  0.0%si,  0.0%st
Mem:   8038844k total,  7165272k used,   873572k free,   530444k buffers
Swap:  4192924k total,     4920k used,  4188004k free,  4742904k cached

   PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND                                                                                                                                                                      
                                                                                                                                                                   
 35184 omm  20   0  822m 421m 128m S    0  5.4   5:28.15 gaussdb                                                                                                                                                                       
     1 root      20   0 13592  820  784 S    0  0.0   1:16.62 init            
```

分析时，请主要关注进程占用的CPU利用率。

其中，统计信息中“us”表示用户空间占用CPU百分比，“sy”表示内核空间占用CPU百分比，“id”表示空闲CPU百分比。如果“id”低于10%，即表明CPU负载较高，可尝试通过降低本节点任务量等手段降低CPU负载。

## 性能参数分析<a name="zh-cn_topic_0283136982_zh-cn_topic_0237121486_zh-cn_topic_0073253546_zh-cn_topic_0040046498_section1965795485640"></a>

1. 使用“top -H”命令查看CPU，显示内容如下所示。

    ```
        14 root      20   0     0    0    0 S    0  0.0   0:16.41 events/3                  
    top - 14:22:49 up 5 days, 21:51,  2 users,  load average: 0.08, 0.08, 0.06
    Tasks: 312 total,   1 running, 311 sleeping,   0 stopped,   0 zombie
    Cpu(s):  1.3%us,  0.7%sy,  0.0%ni, 95.0%id,  2.4%wa,  0.5%hi,  0.2%si,  0.0%st
    Mem:   8038844k total,  5317668k used,  2721176k free,   180268k buffers
    Swap:  4192924k total,        0k used,  4192924k free,  2886860k cached
    
       PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND                  
                      
      3105 root      20   0 50492  11m 2708 S    3  0.1  22:22.56 acc-snf                   
                    
      4015 gdm       20   0  232m  23m  11m S    0  0.3  11:34.70 gdm-simple-gree           
     51001 omm  20   0 12140 1484  948 R    0  0.0   0:00.94 top                       
                    
     54885 omm  20   0  615m 396m 116m S    0  5.1   0:09.44 gaussdb                   
                      
                  
         1 root      20   0 13592  944  792 S    0  0.0   0:08.54 init          
    ```

2. 根据查询结果中“Cpu\(s\)”分析是系统CPU（sy）还是用户CPU（us）占用过高。
    - 如果是系统CPU占用过高，需要查找异常系统进程进行处理。
    - 如果是“USER”为omm的openGauss进程CPU占用过高，请根据目前运行的业务查询内容，对业务SQL进行优化。请根据以下步骤，并结合当前正在运行的业务特征进行分析，是否该程序处于死循环逻辑。
        1. 使用“top -H  -p pid”查找进程内占用的CPU百分比较高的线程，进行分析。

            ```
            top -H -p 54952
            ```

            查询结果如下所示，top中可以看到占用CPU很高的线程，下面以线程54775为主，分析其为何占用CPU过高。

            ```
            top - 14:23:27 up 5 days, 21:52,  2 users,  load average: 0.04, 0.07, 0.05
            Tasks:  13 total,   0 running,  13 sleeping,   0 stopped,   0 zombie
            Cpu(s):  0.9%us,  0.4%sy,  0.0%ni, 97.3%id,  1.1%wa,  0.2%hi,  0.1%si,  0.0%st
            Mem:   8038844k total,  5322180k used,  2716664k free,   180316k buffers
            Swap:  4192924k total,        0k used,  4192924k free,  2889860k cached
            
               PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND                  
             54775 omm  20   0  684m 424m 131m S    0  5.4   0:00.32 gaussdb                   
             54951 omm  20   0  684m 424m 131m S    0  5.4   0:00.84 gaussdb                   
             54732 omm  20   0  684m 424m 131m S    0  5.4   0:00.24 gaussdb                   
             54758 omm  20   0  684m 424m 131m S    0  5.4   0:00.00 gaussdb                   
             54759 omm  20   0  684m 424m 131m S    0  5.4   0:00.02 gaussdb                   
             54773 omm  20   0  684m 424m 131m S    0  5.4   0:02.79 gaussdb                   
             54780 omm  20   0  684m 424m 131m S    0  5.4   0:00.04 gaussdb                   
             54781 omm  20   0  684m 424m 131m S    0  5.4   0:00.21 gaussdb                   
             54782 omm  20   0  684m 424m 131m S    0  5.4   0:00.02 gaussdb                   
             54798 omm  20   0  684m 424m 131m S    0  5.4   0:16.70 gaussdb                   
             54952 omm  20   0  684m 424m 131m S    0  5.4   0:07.51 gaussdb                   
             54953 omm  20   0  684m 424m 131m S    0  5.4   0:00.81 gaussdb                   
             54954 omm  20   0  684m 424m 131m S    0  5.4   0:06.54 gaussdb                   
            ```

        2. 使用“gstack ”查看进程内各线程的函数调用栈。查找上一步骤中占用CPU较高的线程ID对应的线程号。操作系统中若默认无gstack命令，使用时可自行安装。

            ```
            gstack  54954
            ```

            查询结果如下所示，其中线程ID54775对应线程号是10。

            ```
            192.168.0.11:~ # gstack 54954
            Thread 10 (Thread 0x7f95a5fff710 (LWP 54775)):
            #0  0x00007f95c41d63c6 in poll () from /lib64/libc.so.6
            #1  0x0000000000d3d2d3 in WaitLatchOrSocket(Latch volatile*, int, int, long) ()
            #2  0x000000000095ed25 in XLogPageRead(XLogRecPtr*, int, bool, bool) ()
            #3  0x000000000095f6dd in ReadRecord(XLogRecPtr*, int, bool) ()
            #4  0x000000000096aef0 in StartupXLOG() ()
            #5  0x0000000000d5607a in StartupProcessMain() ()
            #6  0x00000000009e19f9 in AuxiliaryProcessMain(int, char**) ()
            #7  0x0000000000d50135 in SubPostmasterMain(int, char**) ()
            #8  0x0000000000d504ec in MainStarterThreadFunc(void*) ()
            #9  0x00007f95c79b85f0 in start_thread () from /lib64/libpthread.so.0
            #10 0x00007f95c41df84d in clone () from /lib64/libc.so.6
            #11 0x0000000000000000 in ?? ()
            ```
