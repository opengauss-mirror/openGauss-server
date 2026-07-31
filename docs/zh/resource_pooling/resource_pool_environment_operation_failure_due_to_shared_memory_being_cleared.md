# 因共享内存被清理导致资源池化环境运行失败的问题

## 一、问题现象

在openGauss资源池化集群使用过程中，出现主机业务中断，检查到主机gaussdb进程消失，备机升主。

```shell
[ctt_ltt@openGauss169 ~]$ ps ux
USER        PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
ctt_ltt    9085  0.0  0.0  24768 14976 ?        Ss   Oct17   0:52 /usr/lib/systemd/systemd --user
ctt_ltt    9087  0.0  0.0  48640 26752 ?        S    Oct17   0:00 (sd-pam)
ctt_ltt   66813  1.6  0.0  16960 12352 ?        S    Oct17  23:50 /data/ctt_ltt/omm/openGauss/gauss/app/bin/om_monitor -L /data/ctt_ltt/omm/log/gaussdb/ctt_ltt/cm/om_monitor
ctt_ltt   67764  0.0  0.0   8256  3520 ?        Ss   Oct17   0:37 /usr/bin/ssh-agent -a /home/ctt_ltt/gaussdb_tmp/gauss_socket_tmp
ctt_ltt  307636  0.0  0.0  23552 11264 ?        S    11:50   0:00 sshd: ctt_ltt@pts/2
ctt_ltt  307643  0.0  0.0 216512  6208 pts/2    Ss   11:50   0:00 -bash
ctt_ltt  307723  0.0  0.0  23168 11264 ?        S    11:50   0:00 sshd: ctt_ltt@notty
ctt_ltt  307889  0.0  0.0   9984  5120 ?        Ss   11:50   0:00 /usr/libexec/openssh/sftp-server -l INFO -f AUTH
ctt_ltt  308316  0.0  0.0 235456 11968 pts/2    S+   11:50   0:00 gsql -d postgres -p 3500 -r
ctt_ltt  326980  0.0  0.0 217856  5504 pts/0    R+   11:51   0:00 ps ux
ctt_ltt  605404  0.0  0.0  23552 11264 ?        S    11:24   0:00 sshd: ctt_ltt@pts/0
ctt_ltt  605407  0.0  0.0 216704  6464 pts/0    Ss   11:24   0:00 -bash
ctt_ltt  605485  0.0  0.0  23168 11264 ?        S    11:24   0:00 sshd: ctt_ltt@notty
ctt_ltt  605486  0.0  0.0   9984  5120 ?        Ss   11:24   0:00 /usr/libexec/openssh/sftp-server -l INFO -f AUTH
ctt_ltt  606461 11.0  0.0 1999168 31232 ?       Sl   11:24   2:58 /data/ctt_ltt/omm/openGauss/gauss/app/bin/cm_agent
ctt_ltt  606633  0.0  0.0 1412864 73024 ?       Sl   11:24   0:00 gaussdb fenced UDF master process
ctt_ltt  606717  0.1  0.0 4795264 289792 ?      Sl   11:24   0:01 /data/ctt_ltt/omm/openGauss/gauss/app/bin/cm_server
ctt_ltt  606935 14.6  0.0 2838400 121920 ?      Sl   11:24   3:55 /data/ctt_ltt/omm/openGauss/gauss/app/bin/dssserver -D /data/ctt_ltt/omm/openGauss/dss_home
ctt_ltt  607308  1.2  0.0   5952  1728 ?        Sl   11:25   0:20 perctrl 36 39
```

```shell
[ctt_ltt@openGauss169 ctt_ltt]$ cm_ctl query -Cvipd
[  CMServer State   ]

node            node_ip         instance                                      state
-------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  1    /data/ctt_ltt/omm/openGauss/cm/cm_server Standby
2  openGauss171 xx.xx.xx.xx  2    /data/ctt_ltt/omm/openGauss/cm/cm_server Primary


[ Defined Resource State ]

node            node_ip         res_name instance  state
----------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  dms_res  6001      Deleted
2  openGauss171 xx.xx.xx.xx  dms_res  6002      OnLine
1  openGauss169 xx.xx.xx.xx  dss      20001     Deleted
2  openGauss171 xx.xx.xx.xx  dss      20002     OnLine

[   Cluster State   ]

cluster_state   : Unavailable
redistributing  : No
balanced        : No
current_az      : AZ_ALL

[  Datanode State   ]

node            node_ip         instance                                    state            | node            node_ip         instance                                    state
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  6001 3500   /data/ctt_ltt/omm/openGauss/dn1 P Down    Manually stopped | 2  openGauss171 xx.xx.xx.xx  6002 3500   /data/ctt_ltt/omm/openGauss/dn1 S Standby Promoting
```

经过短暂业务中断后，备机升主完成，继续对外提供服务。

```shell
[ctt_ltt@openGauss169 ctt_ltt]$ cm_ctl query -Cvipd
[  CMServer State   ]

node            node_ip         instance                                      state
-------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  1    /data/ctt_ltt/omm/openGauss/cm/cm_server Standby
2  openGauss171 xx.xx.xx.xx  2    /data/ctt_ltt/omm/openGauss/cm/cm_server Primary


[ Defined Resource State ]

node            node_ip         res_name instance  state
----------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  dms_res  6001      Deleted
2  openGauss171 xx.xx.xx.xx  dms_res  6002      OnLine
1  openGauss169 xx.xx.xx.xx  dss      20001     Deleted
2  openGauss171 xx.xx.xx.xx  dss      20002     OnLine

[   Cluster State   ]

cluster_state   : Degraded
redistributing  : No
balanced        : No
current_az      : AZ_ALL

[  Datanode State   ]

node            node_ip         instance                                    state            | node            node_ip         instance                                    state
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  openGauss169 xx.xx.xx.xx  6001 3500   /data/ctt_ltt/omm/openGauss/dn1 P Down    Manually stopped | 2  openGauss171 xx.xx.xx.xx  6002 3500   /data/ctt_ltt/omm/openGauss/dn1 S Primary Normal
```

## 二、定位方法

检查故障节点pg_log日志，目录为`$GAUSSLOG/pg_log/dn_xxxx`，检查到如下错误打印：

```shell
2024-10-18 11:51:45.374 6711d94e.5535 [unknown] 281059349802912 dn_6001 0 dn_6001_6002 42809  0 [BACKEND] FATAL:  semop(id=36012398) failed: Identifier removed
2024-10-18 11:51:45.374 6711d50c.1 [unknown] 281473410400272 [unknown] 0 dn_6001_6002 00000  0 [BACKEND] LOG:  WAL file creator process (ThreadId 281059349802912) was terminated by signal 1: Hangup
2024-10-18 11:51:45.375  [postmaster][reaper][281473410400272] LOG: terminating any other active server processes
2024-10-18 11:51:45.375  [postmaster][reaper][281473410400272] LOG: WAL file creator process (ThreadId 281059349802912) exited with exit code 0
2024-10-18 11:51:45.375  [postmaster][reaper][281473410400272] LOG: the server process exits
```

## 三、问题根因

出现该错误的原因是信号量被移除，导致数据库内部线程运行出现错误，数据库自动退出。

## 四、解决方案

数据库运行环境上，应当避免清理共享内存，同时对于RemoveIPC参数也应当设置为false，详见[关闭RemoveIPC](https://docs.opengauss.org/zh/docs/latest/installation_guide/preparing_the_software_and_hardware_installation_environment_enterprise.html#%E5%85%B3%E9%97%ADremoveipc)。
