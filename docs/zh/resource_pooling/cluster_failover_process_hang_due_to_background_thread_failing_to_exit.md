# 因后台线程退出失败导致集群Failover流程卡住的问题

## 一、问题现象  

主备环境同时跑TPCC业务，业务量不限。测试RTO过程中，执行主机注入故障并kill掉主节点进程 ，发现测试rto时间偶现超过30秒，耗时60s  

1. 观察集群状态等待备机成功升主，进入数据库日志：`cd $GAUSSLOG/pg_log/dn_6001/`全局搜索failover trigger;   
2. 通过数据库日志发现升主节点存在两次failover记录,整个rto时间长达2分钟。如下所示：    

    ```shell
    2024-07-25 09:34:50.892 [unknown] [unknown] localhost 281453068005328 0[0:0#0] 0[DMS] LOG: [SS failover] failover trigger.
    2024-07-25 09:35:21.964 [unknown][unknown] localhost 281453068005328 0[0:0#0]0 [DMS] WARNING: [SS failover] failover failed, backends can not exit
    2024-07-25 09:35:26.336 [unknown] [unknown] localhost 281450146672592 0[0:0#0]0 [DMS] LOG: [SS failover] failover trigger.
    2024-07-25 09:35:26.352 [unknown] [unknown] localhost 281450146672592 0[0:0#0]0 [DMS] LOG: [SS failover] do failover when DB restart.
    2024-07-25 09:37:17.4 [postmaster][reaper][281459746799632] LOG: database system is ready to accept connections
    ```

## 二、定位方法

1. 进入数据库日志：可以发现发生failover trigger后首先组织了failover reform，在第一次打印failover trigger到failover failed中间打印了大量的could not fork new process for connection due to PMstate PM WAIT_BACKENDS。如下所示：    

    ```shell
    3013 2024-07-25 15:19:59.991 [unknown] [unknown] localhost 281446460463264 0[0:0#0] o [DMS] LOG: [ss failover] failover trigger.a
    3014 2024-07-25 15:19:59.991 [unknown] [unknown] localhost 281446460463264 0[0:0#0] o [DMS] LOG: [SS reform] dms reform start, role:1, reform type:failover reform
    3015 2024-07-25 15:20:00.563 65040562.5175 postgres 281445888989344 cm_agent 0 dn_6001_6002 42809 4222124650660924 [BACKEND] ERROR: failed to request snapshot as current node is in reform!
    3016 2024-07-25 15:20:00.563 65040562.5175 postgres 281445888989344 cm_agent 0 dn_60016002 42809 4222124650660924 [BACKEND] STATEMENT: select local role,static_connections,db_state,detail information
    from pg_stat _get stream replications();
    3017 2024-07-25 15:20:01.59865040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3018 2024-07-25 15:20:02.613 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_60016002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT_BACKENDS
    3019 apons dweqreqeas qpssneb/up/e:ep/lle1suT/ssneßuado/Jaqsewbo/awoy/, alTJ ouks) :qpssne6 :907 [ON3NOV8] 0 00000 Z009 T009 up 0 [uMouxun] 8ZT88T9ZO/SVT8Z [UMouyun] T ZESOVOS9 £VZ.80:07:ST ST-60-87OZ
    ss
    3020 2024-07-25 15:20:03.243 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001 6002 00000 0 [DMS] LOG: [SS failover] kill backends begin.
    3021 2024-07-25 15:20:03.243 6504055b.5080 postgres 281449418496160 JobSchedulerO dn_6001_6002 00000 0 [BACKEND] LOG: job scheduler is shutting down
    3022 2024-07-25 15:20:03.626 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 600201000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3023 2024-07-25 15:20:04.639 65040532.1 [unknown] 281457026138128 [unknown] ● dn_60016002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3024 2024-07-25 15:20:05.651 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT_BACKENDS
    3025 2024-07-25 15:20:06.664 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3026 2024-07-25 15:20:07.676 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 01000 0 [BACKEND] WARNING: could not fork new processa for connection due to PMstate PM_WAIT_BACKENDS
    3027 2024-07-25 15:20:08.688 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3028 2024-07-25 15:20:09.701 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new processca for connection due to PMstate PM WAIT_BACKENDS
    3029 2024-07-25 15:20:10.714 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new processaa for connection due to PMstate PM WAIT BACKENDS
    3030 2024-07-25 15:20:11.727 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3031 2024-07-25 15:20:12.740 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3032 2024-07-25 15:20:13.753 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new processaa for connection due to PMstate PM WAIT_BACKENDS
    3033 2024-07-25 15:20:14.767 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3034 2024-07-25 15:20:15.780 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3036 2024-07-25 15:20:17.806 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001_6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3037 2024-07-25 15:20:18.820 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND ] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3038 2024-07-25 15:20:19.833 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3039 2024-07-25 15:20:20.846 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3040 2024-07-25 15:20:21.859 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3041 2024-07-25 15:20:22.872 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3042 2024-07-25 15:20:23.885 65040532.1 [unknown] 281457026138128 [unknown] 0 dn_6001_6002 01000 0 [BACKEND] WARNING: could not fork new processar for connection due to PMstate PM_WAIT_BACKENDS
    3043 2024-07-25 15:20:24.898 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3044 2024-07-25 15:20:25.911 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001_6002 01000 0 [BACKEND] WARNING: could not fork new processaa for connection due to PMstate PM WAIT_BACKENDS
    3045 2024-07-25 15:20:26.924 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new processa for connection due to PMstate PM WAIT BACKENDS
    3046 2024-07-25 15:20:27.936 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3047 2024-07-25 15:20:28.947 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3048 2024-07-25 15:20:29.959 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT_BACKENDS
    3049 2024-07-25 15:20:30.970 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM_WAIT_BACKENDS
    3050 2024-07-25 15:20:31.982 65040532.1 [unknown] 281457026138128 : [unknown] 0 dn 6001 6002 01000 0 [BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT_BACKENDS
    3051 2024-07-25 15:20:32.993 65040532.1 [unknown] 281457026138128 [unknown] 0 dn 6001 6002 01000 0[BACKEND] WARNING: could not fork new process for connection due to PMstate PM WAIT BACKENDS
    3052 2024-07-25 15:20:33.410 [unknown] [unknown] localhost 281446460463264 0[0:0#0]o [DMS] WARNING: [SS failover] failover failed, backends can not exit
    ```

2. 这代表着由于PM_WAIT_BACKENDS阻碍了failover进程和组织reform流程；  
3. 从第一次failover trigger到数据库重新启动，历时2分半左右，出现了两次failover；并且第一次failover的原因为backends can not exit。  

## 三、问题根因

1. 备机在跑业务，进行failover升主过程中；  
业务线程迟迟无法退出导致的第一次failover失败，此式cm会自动决策下一轮重启failover，保证升主成功。  
2. 第一轮在线failover因为业务线程无法退出，造成30s的耗时；  
第二轮重启failover，因为重启造成有额外耗时；  

## 四、解决方案

对于该问题有两种方案解决：  

1. 集群reform场景中增加后端请求的超时标志位，超过一定时间主动从后端退出所有业务线程，暴力退出机制；  
2. 从底层业务线程向上退出，优雅退出机制；  

预期目标：保证第一次failover成功升主，且整个过程时间不超过30S；  
