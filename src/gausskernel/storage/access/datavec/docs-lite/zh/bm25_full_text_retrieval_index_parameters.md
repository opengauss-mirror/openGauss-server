# BM25全文检索索引参数

## enable_bm25_taat<a name="section14941640131"></a>

**参数说明**： 使用BM25索引扫描时，参数开启使用 TAAT 方法按搜索词遍历，对包含搜索词所有文档进行打分、汇总、排序，最终返回 limit n 个分数最高的文档，速度较慢，召回率更高。关闭时，通过 DAAT MaxScore 方法对文档进行剪枝，速度更快，召回率可能会有一定损失。

该参数属于USERSET类型参数，请参考[表1](../database_administration_guide/reset_parameters.md#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846)中对应设置方法进行设置。

**取值范围**： 布尔型

- on表示开启 TAAT 扫描功能。
- off表示开启 DAAT MaxScore 扫描功能。

**设置建议**：当希望检索搜索词相关的所有文档的排序结果时，建议开启该参数。否则，建议该参数保持关闭状态。

**默认值**： off

**设置语法**：

```
set enable_bm25_taat = on;
```

## bm25_topk<a name="section14941640131"></a>

**参数说明**： 使用BM25索引扫描时的动态top-k候选集大小。

该参数属于USERSET类型参数，请参考[表1](../database_administration_guide/reset_parameters.md#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846)中对应方法三进行设置。

**取值范围**： 整型，5~200

**设置建议**： 建议设置值大于等于`Limit`。如果没有获取足够`Limit`大小的数据会自动扩大bm25_topk继续扫描，直到获取足够的数据或者没有更多满足条件的数据为止。

**默认值**： 5

**设置语法**：

```
set bm25_topk = 10;
```

>[!NOTE]说明
>
> 设置值小于`Limit`时可能会导致多轮扩大bm25_topk继续扫描，查询效率会变低。<br>
> 由于 TAAT 方法会默认给包含搜索词的所有候选文档进行打分，因此，此参数仅在 enable_bm25_taat 关闭生效。

## bm25_k1<a name="section14941640131"></a>

**参数说明**： BM25算法参数，影响词频对文档得分。该参数值越大，词频得分就越大。一般保持默认值 1.2 即可。

该参数属于USERSET类型参数，请参考[表1](../database_administration_guide/reset_parameters.md#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846)中对应方法三进行设置。

**取值范围**： 浮点型，0.0\~3.0

**默认值**： 1.2

**设置语法**：

```
set bm25_k1 = 1.5;
```

## bm25_b<a name="section14941640131"></a>

**参数说明**： BM25算法参数，用于调整文档长度对文档评分影响。bm25_b 越大，对文档长度的惩罚力度就越大。bm25_b = 0时，长短文档词频一样，则得分一样，bm25_b 越大，长文档分数越低，短文档分数越高。一般保持默认值 0.75 即可。

该参数属于USERSET类型参数，请参考[表1](../database_administration_guide/reset_parameters.md#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846)中对应方法三进行设置。

**取值范围**： 浮点型，0.0\~1.0

**默认值**： 0.75

**设置语法**：

```
set bm25_b = 0.5;
```

## max_score_ratio<a name="section14941640131"></a>

**参数说明**： 使用BM25索引 DAAT MaxScore 扫描时，用于控制搜索词 MaxScore 的缩放比例。

该参数属于USERSET类型参数，请参考[表1](../database_administration_guide/reset_parameters.md#zh-cn_topic_0283137176_zh-cn_topic_0237121562_zh-cn_topic_0059777490_t91a6f212010f4503b24d7943aed6d846)中对应设置方法进行设置。

**取值范围**： 浮点型，0.5~1.3

**默认值**： 1.05

**设置语法**：

```
set max_score_ratio = 0.9;
```

>[!NOTE]说明
>
>在新增文档时，文档的平均长度是不断变化的。因此，文档词汇的 MaxScore 是一个估计值，新增该参数实现对MaxScore进行调整，建议大于 1.0。<br>
>在文档扫描时，该参数小于 1.0 时，MaxScore 缩小，能够实现更加激进的剪枝，扫描文档数量变少，查询速度更快，召回率会有一定损失。相反，该参数大于0，剪枝比较宽松，查询速度会慢一点，召回率会更高。
>bm25_topk 和 max_score_ratio 设置不合理的情况下，由于 Limit n > bm25_topk，内部多轮扩展查询，导致查询结果可能会出现分数不是完全按降序排序。
