# 介绍<a name="ZH-CN_TOPIC_0289900675"></a>

GIN（Generalized Inverted Index）通用倒排索引。设计为处理索引项为组合值的情况，查询时需要通过索引搜索出出现在组合值中的特定元素值。例如，文档是由多个单词组成，需要查询出文档中包含的特定单词。

使用item表示索引的组合值，key表示一个元素值。GIN用来存储和搜索key，而不是item。

GIN索引存储一系列（key，posting list）键值对，这里的posting list是一组出现key的行ID。由于每个item都可能包含多个key，同一个行ID可能会出现在多个posting list中，而每个key值只被存储一次，所以在相同的key在item中出现多次的情况下，GIN索引是非常简洁的。

因为GIN索引的访问方式不需要了解他的运行方式，所以GIN索引是通用的。GIN索引使用为特殊数据类型定义的策略。策略定义了如何从索引选项和查询条件中抽出key，以及如何确定在查询中包含某些key值的行是否实际满足查询条件。
