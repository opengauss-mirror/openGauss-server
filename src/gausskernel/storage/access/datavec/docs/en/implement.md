# Implementation

Internally, a GIN index contains a B-tree index constructed over keys, where each key is an element of one or more indexed items (for example, a member of an array). In addition, each tuple on a page contains a pointer to a B-tree of heap pointers (a posting tree), or a simple list of heap pointers (a posting list) when the list is small enough to be stored together with the key value in an index tuple.

A multicolumn GIN index is implemented by building a single B-tree over composite values (column number, key value). Key values of different columns can have different types.

## GIN Fast Update Technique<a name="zh-cn_topic_0283137368_zh-cn_topic_0237122201_zh-cn_topic_0059778495_s0257d3dc71434d4c8e7d1395a49035d8"></a>

Due to the intrinsic nature of inverted indexes, updating a GIN index can be slow. Inserting or updating a heap row can cause many insertions into the index. After VACUUM is performed on the table, or if the list of pending entries becomes too large (larger than work_mem), these entries are moved to the main GIN data structure using the same bulk insert method used during initial index creation. Even accounting for the additional VACUUM overhead, this greatly improves the speed of GIN index updates. Moreover, this additional overhead work can be handled by background processes rather than frontend queries.

The main disadvantage of this approach is that searches must scan the list of pending entries in addition to the regular index. Therefore, a large list of pending entries can significantly slow down searches. Another disadvantage is that, although most updates are fast, an update that causes the pending list to become "too large" will trigger an immediate cleanup and will therefore be much slower than other updates. Proper use of autovacuum can mitigate both of these problems.

If consistent response time (the response time of cleaning up entries and the response time of updates) is more important than update speed, pending entries can be disabled by setting the GIN index storage parameter FASTUPDATE to off. For details, see [CREATE INDEX](https://docs.opengauss.org/en/docs/latest/sql_reference/create_index.html).

## Partial Match Algorithm<a name="zh-cn_topic_0283137368_zh-cn_topic_0237122201_zh-cn_topic_0059778495_s9dc41ea95b9144c38d709b0b9a43fe9e"></a>

GIN can support "partial match" queries. That is, the query does not determine an exact match for a single key or multiple keys; instead, the possible matches fall within a reasonably narrow range of key values (according to the key value sort order determined by the compare support function). In this case, the extractQuery method does not return a key value for exact matching; instead, it returns the lower bound of the key value range to be searched, and sets pmatch to true. Then, the comparePartial method is used to scan this key value range. comparePartial must return 0 for a matching index key, return a value less than 0 if it does not match but is still within the searched range, and return a value greater than 0 for an index key that exceeds the matchable range.
