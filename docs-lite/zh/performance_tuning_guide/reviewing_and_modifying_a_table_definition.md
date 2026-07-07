# 审视和修改表定义<a name="ZH-CN_TOPIC_0289900185"></a>

## 审视和修改表定义概述<a name="ZH-CN_TOPIC_0289900372"></a>

好的表定义至少需要达到以下几个目标：

1. **减少扫描数据数据量**。通过分区的剪枝机制可以实现该点。
2. **尽量极少随机IO**。通过聚簇/局部聚簇可以实现该点。

表定义在数据库设计阶段创建，在SQL调优过程中进行审视和修改。

## 选择存储模型<a name="ZH-CN_TOPIC_0289900051"></a>

进行数据库设计时，表设计上的一些关键项将严重影响后续整库的查询性能。表设计对数据存储也有影响：好的表设计能够减少I/O操作及最小化内存使用，进而提升查询性能。

表的存储模型选择是表定义的第一步。客户业务属性是表的存储模型的决定性因素，依据下面表格选择适合当前业务的存储模型。

<a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_table39547486"></a>
<table><thead align="left"><tr id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_row59078165"><th class="cellrowborder" valign="top" width="15.65%" id="mcps1.1.3.1.1"><p id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p20602051"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p20602051"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p20602051"></a><strong id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_b5354996163216"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_b5354996163216"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_b5354996163216"></a>存储模型</strong></p>
</th>
<th class="cellrowborder" valign="top" width="84.35000000000001%" id="mcps1.1.3.1.2"><p id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p53618895"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p53618895"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p53618895"></a><strong id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_b12808013"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_b12808013"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_b12808013"></a>适用场景</strong></p>
</th>
</tr>
</thead>
<tbody><tr id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_row30816121"><td class="cellrowborder" valign="top" width="15.65%" headers="mcps1.1.3.1.1 "><p id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p13077833"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p13077833"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p13077833"></a>行存</p>
</td>
<td class="cellrowborder" valign="top" width="84.35000000000001%" headers="mcps1.1.3.1.2 "><p id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p52671525"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p52671525"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p52671525"></a>点查询(返回记录少，基于索引的简单查询)。</p>
<p id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p4281684"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p4281684"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p4281684"></a>增删改比较多的场景。</p>
</td>
</tr>
<tr id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_row38535158"><td class="cellrowborder" valign="top" width="15.65%" headers="mcps1.1.3.1.1 "><p id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p34340132"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p34340132"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p34340132"></a>列存</p>
</td>
<td class="cellrowborder" valign="top" width="84.35000000000001%" headers="mcps1.1.3.1.2 "><p id="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p30087318"><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p30087318"></a><a name="zh-cn_topic_0283136984_zh-cn_topic_0237121516_zh-cn_topic_0076211991_zh-cn_topic_0071158045_p30087318"></a>统计分析类查询 (group , join多的场景)。</p>
</td>
</tr>
</tbody>
</table>

## 使用局部聚簇<a name="ZH-CN_TOPIC_0289900213"></a>

局部聚簇（Partial Cluster Key）是列存下的一种技术。这种技术可以通过min/max稀疏索引较快的实现基表扫描的filter过滤。Partial Cluster Key可以指定多列，但是一般不建议超过2列。Partial Cluster Key的选取原则：

1. 受基表中的简单表达式约束。这种约束一般形如col op const，其中col为列名，op为操作符 =、\>、\>=、<=、<，const为常量值。
2. 尽量采用选择度比较高\(过滤掉更多数据\)的简单表达式中的列。
3. 尽量把选择度比较低的约束col放在Partial Cluster Key中的前面。
4. 尽量把枚举类型的列放在Partial Cluster Key中的前面。

## 使用分区表<a name="ZH-CN_TOPIC_0000001119972376"></a>

分区表是把逻辑上的一张表根据某种方案分成几张物理块进行存储。这张逻辑上的表称之为分区表，物理块称之为分区。分区表是一张逻辑表，不存储数据，数据实际是存储在分区上的。分区表和普通表相比具有以下优点：

1. 改善查询性能：对分区对象的查询可以仅搜索自己关心的分区，提高检索效率。
2. 增强可用性：如果分区表的某个分区出现故障，表在其他分区的数据仍然可用。
3. 方便维护：如果分区表的某个分区出现故障，需要修复数据，只修复该分区即可。

    openGauss数据库支持的分区表为一级分区表和二级分区表，其中一级分区表包括范围分区表、间隔分区表、列表分区表、哈希分区表四种，二级分区表包括范围分区、列表分区、哈希分区两两组合的九种。

    - 范围分区表：将数据基于范围映射到每一个分区，这个范围是由创建分区表时指定的分区键决定的。这种分区方式是最为常用的，并且分区键经常采用日期，例如将销售数据按照月份进行分区。
    - 间隔分区表：是一种特殊的范围分区表，相比范围分区表，新增间隔值定义，当插入记录找不到匹配的分区时，可以根据间隔值自动创建分区。
    - 列表分区表：将数据中包含的键值分别存储再不同的分区中，依次将数据映射到每一个分区，分区中包含的键值由创建分区表时指定。
    - 哈希分区表：将数据根据内部哈希算法依次映射到每一个分区中，包含的分区个数由创建分区表时指定。
    - 二级分区表：由范围分区、列表分区、哈希分区任意组合得到的分区表，其一级分区和二级分区均可以使用前面三种定义方式。

## 选择数据类型<a name="ZH-CN_TOPIC_0000001166772183"></a>

高效数据类型，主要包括以下三方面：

1. **尽量使用执行效率比较高的数据类型**

    一般来说整型数据运算\(包括=、＞、＜、≧、≦、≠等常规的比较运算，以及group by\)的效率比字符串、浮点数要高。比如某客户场景中对列存表进行点查询，filter条件在一个numeric列上，执行时间为10+s；修改numeric为int类型之后，执行时间缩短为1.8s左右。

2. **尽量使用短字段的数据类型**

    长度较短的数据类型不仅可以减小数据文件的大小，提升IO性能；同时也可以减小相关计算时的内存消耗，提升计算性能。比如对于整型数据，如果可以用smallint就尽量不用int，如果可以用int就尽量不用bigint。

3. **使用一致的数据类型**

    表关联列尽量使用相同的数据类型。如果表关联列数据类型不同，数据库必须动态地转化为相同的数据类型进行比较，这种转换会带来一定的性能开销。
