# 因按需回放redo阶段执行部分DDL报错的问题

## 问题现象

按需回放redo阶段，执行部分DDL报错，报错信息如下：

```shell
openGauss=# create database tpcc1;
ERROR:  only support INSERT/UPDATE/DELETE/SELECT/SET/SHOW/CALL, and ALTER/CREATE/DROP on Table/Index/View/Procedure/Schema, and ALTER on Database during SS on-demand recovery, command 752
```

## 定位方法

查询`ondemand_recovery_status`视图，确认当前节点处于按需回放状态，对应`in_ondemand_recovery`字段查询为`true`，`ondemand_recovery_status`字段状态为`ONDEMAND_RECOVERY_REDO`。

```shell
openGauss=# select * from ondemand_recovery_status();
-[ RECORD 1 ]---------------+-----------------------
primary_checkpoint_redo_lsn | 0/0
realtime_build_replayed_lsn | B/24C39FA0
hashmap_used_blocks         | 9928223
hashmap_total_blocks        | 70640909
trxn_queue_blocks           | 0
seg_queue_blocks            | 0
in_ondemand_recovery        | t
ondemand_recovery_status    | ONDEMAND_RECOVERY_REDO
realtime_build_status       | DISABLED
recovery_pause_status       | NOT PAUSE
record_item_num             | 57001
record_item_mbytes          | 648
```

## 问题根因

主节点当前处于按需回放状态，支持有限的DDL（详见[特性增强](ultimate_rto_on_demand_playback.md#特性增强)），此现象符合预期。

## 解决方案

等待按需回放redo阶段结束，之后执行SQL无限制。按需回放redo阶段进度，可以通过查询`ondemand_recovery_status`视图中的`hashmap_used_blocks`字段判断。当`hashmap_used_blocks`的值为0时，会进行全量checkpoint，之后退出按需回放，`in_ondemand_recovery`字段为`false`，`ondemand_recovery_status`字段为`normal`。
