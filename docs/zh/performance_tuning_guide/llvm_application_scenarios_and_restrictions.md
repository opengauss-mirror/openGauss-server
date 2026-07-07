# LLVM适用场景与限制

## 适用场景<a name="zh-cn_topic_0237121504_zh-cn_topic_0066033419_section279430195545"></a>

- 支持LLVM的表达式

    查询语句中存在以下的表达式支持LLVM优化：

    1. Case…when… 表达式
    2. In表达式
    3. Bool表达式（And/Or/Not）
    4. BooleanTest表达式（IS\_NOT\_KNOWN/IS\_UNKNOWN/IS\_TRUE/IS\_NOT\_TRUE/IS\_FALSE/IS\_NOT\_FALSE）
    5. NullTest表达式（IS\_NOT\_NULL/IS\_NULL）
    6. Operator表达式
    7. Function表达式
    8. Nullif表达式

    表达式计算支持的数据类型包括bool、tinyint、smallint、int、bigint、float4、float8、numeric、date、time、timetz、timestamp、timestamptz、interval、bpchar、varchar、text、oid。

    仅当表达式出现在向量化执行引擎中Scan节点的filter，Hash Join节点中的complicate hash condition、hash join filter、hash join target，Nested Loop节点中的filter、join filter，Merge Join节点的merge join filter、merge join target，Group节点中的filter表达式时，才会考虑是否使用LLVM动态编译优化。

- 支持LLVM的算子：

    1. Join：HashJoin
    2. Agg：HashAgg
    3. Sort

    其中HashJoin算子仅支持Hash Inner Join，对应的hash cond仅支持int4、bigint、bpchar类型的比较；HashAgg算子仅支持针对bigint、numeric类型的sum及avg操作，且group by语句仅支持int4、bigint、bpchar、text、varchar、timestamp类型操作，同时支持count\(\*\)聚集操作。Sort算子仅支持对int4、bigint、numeric、bpchar、text、varchar数据类型的比较操作。除此之外，无法使用LLVM动态编译优化，具体可通过explain performance工具进行显示。

## 非适用场景<a name="zh-cn_topic_0237121504_zh-cn_topic_0066033419_section316931181001"></a>

- 不支持小数据量表使用LLVM动态编译优化。
- 不支持生成非向量化执行路径的查询作业。
