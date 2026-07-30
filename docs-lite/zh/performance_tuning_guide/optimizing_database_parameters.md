# 数据库系统参数调优<a name="ZH-CN_TOPIC_0289900754"></a>

为了保证数据库尽可能高性能地运行，建议依据硬件资源情况和业务实际进行数据库系统参数——GUC参数的设置。这里主要介绍GUC参数对性能的影响，关于参数的详细设置方法请参见"数据库管理指南"。

## 数据库内存参数调优<a name="ZH-CN_TOPIC_0289900947"></a>

数据库的复杂查询语句性能非常强的依赖于数据库系统内存的配置参数。数据库系统内存的配置参数主要包括逻辑内存管理的控制参数和执行算子是否下盘的参数。

### 逻辑内存管理参数<a name="zh-cn_topic_0283136881_zh-cn_topic_0237121495_zh-cn_topic_0073253552_zh-cn_topic_0062863366_section6641095815423"></a>

逻辑内存管理参数为max\_process\_memory，主要功能是控制数据库节点上可用内存的最大峰值，该参数的数值设置公式参考[max\_process\_memory](https://docs.opengauss.org/zh/docs/latest-lite/database_reference/memory.html#zh-cn_topic_0283136786_zh-cn_topic_0237124699_zh-cn_topic_0059777577_sbebcee7acf2042dc8824982f22a2b4a8)。

执行作业最终可用的内存为：

max\_process\_memory – shared memory \( 包括shared\_buffers \) – cstore\_buffers

所以影响执行作业可用内存参数的主要两个参数为shared\_buffers及cstore\_buffers。

逻辑内存管理有专门的视图查询数据库节点中各大块内存区域已使用内存及峰值信息。可连接到单个数据库节点，通过“pg\_total\_memory\_detail”查询该节点上内存区域信息；

参数work\_mem依据查询特点和并发来确定，一旦work\_mem限定的物理内存不够，算子运算数据将写入临时表空间，带来5-10倍的性能下降，查询响应时间从秒级下降到分钟级。

- 对于串行无并发的复杂查询场景，平均每个查询有5-10关联操作，建议work\_mem=50%内存/10。
- 对于串行无并发的简单查询场景，平均每个查询有2-5个关联操作，建议work\_mem=50%内存/5。
- 对于并发场景，建议work\_mem=串行下的work\_mem/物理并发数。

### 执行算子是否下盘的参数<a name="zh-cn_topic_0283136881_zh-cn_topic_0237121495_zh-cn_topic_0073253552_zh-cn_topic_0062863366_section14594953151011"></a>

参数work\_mem可以判断执行作业可下盘算子是否已使用内存量触发下盘点。当前可下盘算子有六类（向量化及非向量化共10种）：Hash\(VecHashJoin\)，Agg\(VecAgg\)，Sort\(VecSort\)，Material\(VecMaterial\)，SetOp\(VecSetOp\)，WindowAgg\(VecWindowAgg\)。该参数设置通常是一个权衡，即要保证并发的吞吐量，又要保证单查询作业的性能，故需要根据实际执行情况（结合Explain Performance输出）进行调优。

### Jemalloc内存管理<a name="zh-cn_topic_0283136881_zh-cn_topic_0237121495_zh-cn_topic_0073253552_zh-cn_topic_0062863366_section14594953151022"></a>
 
 openGauss使用Jemalloc提供高效的内存分配和管理，以此减少内存碎片和锁竞争。它具有以下几个技术优势：
 
 1. 高效的内存分配和管理：jemalloc使用许多优化技术，如本地缓存、分配器缓存、延迟分配等，以提高内存分配和管理的效率。
 2. 减少内存碎片：jemalloc使用分配器缓存和延迟分配技术，减少内存碎片产生，从而提高内存利用率。
 3. 支持多线程：jemalloc支持多线程环境下的内存分配和管理，通过分层缓存管理，可以有效减少锁竞争，提高并发性能。
 4. 可扩展性：jemalloc支持动态调整内存分配器参数，可以根据实际需求调整内存分配器的行为，以适应不同的工作负载。
 
 - Jemalloc关键参数说明：
 
 | 参数名称               | 默认值       | 参数含义             | 场景说明                                                                                 |
 | ------------------ | --------- | ---------------- | ----------------------------------------------------------------------------------------------- |
 | background\_thread | true      | 内存回收后台线程开关        | 内存敏感型负载建议设置为true；cpu敏感型负载建议设置为false；平衡值配置建议设置为true                              |
 | tcache             | true      | 线程缓存开关               | 内存敏感型负载建议设置为true；cpu敏感型负载建议设置为false；平衡值配置建议设置为true                              |
 | tcache\_max        | 4096      | 单线程缓存上限             | 内存敏感型负载建议设置为1024；极限内存型可设置为0；cpu敏感型负载建议保持默认值4096                                 |
 | dirty\_delay\_ms   | 10000     | 脏页回收延迟时间（单位:ms）  | 内存敏感型负载设置较低值如1000-5000，极限可设置为0；CPU敏感型负载设置较高值如20000+；平衡值配置建议设置为5000-10000  |
 | muzz\_delay\_ms    | 10000     | 模糊页回收延迟时间（单位:ms） | 内存敏感型负载设置较低值如1000-5000，极限可设置为0；CPU敏感型负载设置较高值如20000+；平衡值配置建议设置为5000-10000  |
 | narenas            | cpu核心数\*4 | 内存分配器数量            | 低并发场景/内存敏感型负载设置为cpu核心数或更低；高并发场景保持默认值cpu核心数\*4；单线程场景可设置1                   |
 
 - Jemalloc参数推荐配置：
 
 | 场景说明 | 参考配置                                                                                       |
 | ---------- | ------------------------------------------------------------------------------------------ |
 | 内存充足追求更高性能 | background\_thread:true,dirty\_delay\_ms:10000,muzz\_delay\_ms:10000 |
 | 内存和性能平衡配置  | background\_thread:true,dirty\_delay\_ms:50000,muzz\_delay\_ms:50000,narenas:4 |
 | 内存占用较为敏感   | background\_thread:true,dirty\_delay\_ms:1000,muzz\_delay\_ms:1000,narenas:1,tcache\_max:1024 |
 | 内存占用极为敏感   | background\_thread:true,dirty\_delay\_ms:0,muzz\_delay\_ms:0,narenas:1,tcache:false |
 
 - openGauss Jemalloc核心参数配置如下：
 
 1. openGauss三方库Jemalloc核心参数默认配置为background\_thread:true,dirty\_delay\_ms:4000,muzz\_delay\_ms:4000
 2. 若内存配置较低，追求资源占用更低，可增加环境变量动态配置参数：export MALLOC\_CONF="dirty\_delay\_ms:0,muzz\_delay\_ms:0"

## 数据库I/O参数调优<a name="ZH-CN_TOPIC_0000001149231237"></a>

### I/O相关参数<a name="section181599115402"></a>

- pagewriter\_sleep：增量checkpoint模式，控制后端写线程pagewriter刷页频率，当脏页占据shared\_buffers的比例达到dirty\_page\_percent\_max时，每批脏页数量以设定的max\_io\_capacity计算出的值刷页。pagewriter线程刷页用于推进recovery点，设置时间较长时，会导致recovery点推进慢，宕机启动时间长和xlog日志堆积问题。

    为降低RTO，减少日志膨胀，需要将pagewriter\_sleep适当调小，加快页面刷盘，推进recovery点，促进日志回收。

- bgwriter\_delay：增量checkpoint模式下，控制后端写线程bgwriter刷页频率，当前空闲缓冲页面个数占据shared\_buffers的比例低于candidate\_buf\_percent\_target时，每批脏页数量以设定的max\_io\_capacity计算出的值刷页。bgwriter线程将可以淘汰的页面刷盘，加速业务执行时，页面占用槽位的速度，时间过长会影响性能。

    为了提升业务性能时，适当调小bgwriter\_delay的时间。

- max\_io\_capacity：设置后端写线程（pagewriter线程和bgwriter线程）批量刷页每秒的IO上限，需要根据具体业务场景和机器磁盘IO能力进行设置。要求RTO很短时间或者数据量比共享内存大多倍的情况，业务访问数据量又是随机访问时，该值不宜过小。该参数设置较小会减小后端写线程刷页个数，如果业务触发页面淘汰多时，该值设置小会影响业务。

    max\_io\_capacity需依据随机写IO能力最优设置。
