# 重建索引失败

## 问题现象<a name="section094511327226"></a>

当Desc表的索引出现损坏时，无法进行一系列操作，可能的报错信息如下。

```
index \"%s\" contains corrupted page at block
 %u" ,RelationGetRelationName(rel),BufferGetBlockNumber(buf), please reindex it.
```

## 原因分析<a name="section461110379220"></a>

在实际操作中，索引会由于软件或者硬件问题引起崩溃。例如，当索引分裂完发生磁盘空间不足、出现页面损坏等问题时，会导致索引损坏。

## 处理办法<a name="section8841134114226"></a>

如果此表是以pg\_cudesc\_xxxxx\_index进行命名则为列存表，则说明desc表的索引表损坏。通过desc表的索引表表名，找到对应主表的oid和表，执行如下语句重建表的索引。

```
REINDEX INTERNAL TABLE name;
```
