# 因redo日志过多导致的failover恢复时间过长问题

## 一、问题现象

failover恢复后，集群较长时间处于以下状态。

```shell
[  Datanode State   ]

node            node_ip         instance                              state
-----------------------------------------------------------------------------------------
1  openGaussxxx xxx.xxx.xxx.xxx    6001 25400  /.../install/data/dn P Standby Starting
2  openGaussxxx xxx.xxx.xxx.xxx    6002 25400  /.../install/data/dn S Down    Unknown
3  openGaussxxx xxx.xxx.xxx.xxx    6003 25400  /.../install/data/dn S Standby Promoting
```

## 二、定位分析

查看`$GAUSSLOG/pg_log/dn_xxx`日志，升主节点出现以下信息。

```shell
2024-10-12 09:17:16.123 [unknown] [unknown] localhost 281450988154784 0[0:0#0]  0 [BACKEND] LOG:  CheckpointInProgress: ckpt_done=1, ckpt_started=1
2024-10-12 09:17:16.123 [unknown] [unknown] localhost 281450988154784 0[0:0#0]  0 [DMS] LOG:  [SS reform][SS failover] backends exit successfully
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451140722592 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:9, buf_if start from:74304 to:82559, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451240927136 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:0, buf_if start from:0 to:8255, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451122962336 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:10, buf_if start from:82560 to:90815, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451019153312 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:15, buf_if start from:123840 to:132095, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451209469856 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:4, buf_if start from:33024 to:41279, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451093077920 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:12, buf_if start from:99072 to:107327, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451186401184 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:6, buf_if start from:49536 to:57791, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451064766368 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:13, buf_if start from:107328 to:115583, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451041697696 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:14, buf_if start from:115584 to:123839, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451238764448 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:1, buf_if start from:8256 to:16511, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451219431328 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:3, buf_if start from:24768 to:33023, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451232538528 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:2, buf_if start from:16512 to:24767, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451108806560 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:11, buf_if start from:90816 to:99071, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451196886944 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:5, buf_if start from:41280 to:49535, max_buf_id:132095.
2024-10-12 09:17:16.127 [unknown] [unknown] localhost 281451164381088 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:7, buf_if start from:57792 to:66047, max_buf_id:132095.
2024-10-12 09:17:16.128 [unknown] [unknown] localhost 281451149701024 0[0:0#0]  0 [DMS] LOG:  [SS reform] rebuild page: success.rebuild buf thread_index:8, buf_if start from:66048 to:74303, max_buf_id:132095.
2024-10-12 09:17:16.137 [unknown] [unknown] localhost 281450988154784 0[0:0#0]  0 [DMS] LOG:  [SS reform][SS failover] SSFailoverPromoteNotify:send signal to PM to initialize startup thread when DB alive
2024-10-12 09:17:16.138 [unknown] [unknown] localhost 281450988154784 0[0:0#0]  0 [DMS] LOG:  [SS reform][SS failover] wait startup thread start.
...
```

等待一段时间（具体时间由数据库回放速度决定）后，备节点dn日志出现以下信息，且数据库其他节点状态恢复为Normal。

```
[SS reform][SS failover] backends exit successfully
```

## 三、问题根因

出现这一问题现象是由于redo日志过多，进而造成集群恢复时间过长。

## 四、解决方法

等待一段时间后，查看`$GAUSSLOG/pg_log/dn_xxx`日志，日志回放完成，此时集群状态恢复正常。
