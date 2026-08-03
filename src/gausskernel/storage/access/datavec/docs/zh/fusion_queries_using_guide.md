# 融合查询使用指南

本章节主要介绍openGauss中DataVec向量引擎的融合查询使用指导。

## 1. 安装部署

使用Docker实现openGauss搭载DataVec的容器化部署，简化DevOps用户的安装、配置和环境设置，参考 [容器镜像安装](https://docs.opengauss.org/zh/docs/latest/installation_guide/installing_the_container_image.html)。

## 2. 融合查询

数据源类型通常是多样化的，包含结构化数据（如文本、数字等）和非结构化数据（如视频、图片、音频等）。为了有效存储和检索不同类型的数据，融合查询结合了结构化过滤和非结构化检索的技术，使用户在同一查询中使用不同的数据类型和查询方法，以获取更为精确的非结构化数据。openGauss集成了结构化数据和非结构化数据融合查询的能力。

openGauss将向量引擎DataVec深度集成到数据库内核中，使用户在检索向量时使用ANN相关索引进行融合查询。

openGauss融合查询具有以下特点：

- `支持相似性搜索与关系型数据的JOIN`：在融合查询中，既能进行基于向量的相似性检索，也能将结果与关系型数据关联。
- `全面支持结构化数据类型`：兼容所有类型的结构化数据，实现无缝融合。
- `兼容所有SQL`：包括复杂的运算和功能，如窗口分析函数（Windows analytic functions）, 存储过程（stored procedures）, 以及聚合操作（aggregations）。

**案例说明：**

假设有一个旅游平台需要实现查找离用户本地距离在10到20千米，相似性最高的旅游地区的功能。输入为景点图片、本地名称、距离范围。

### 2.1 创建表格

```sql
openGauss=# CREATE TABLE gist_info (
    id int,
    src_location varchar(64),
    dist_location varchar(64),
    update_time timestamp,
    distance float,
    feature vector(6)
);

--设置向量索引
openGauss=# CREATE INDEX ON gist_info USING HNSW(feature vector_l2_ops);
```

### 2.2 融合查询

```sql
openGauss=# SELECT dist_location 
FROM gist_info 
WHERE src_location = 'zhejiang'
AND distance > 10 AND distance < 20
ORDER BY feature <-> '[0,1,2,3,4,5]';
```

融合查询主要有两类执行方法，对应的执行计划如下：

**第一类：精确检索**

精确检索是一种通过遍历整个表的数据查找满足查询条件的数据的检索方式。它适用于小数据量或需要处理大量数据而无法有效利用索引的场景。精确检索的执行计划包含以下步骤：首先根据向量距离进行排序，然后扫描全表获取符合条件的数据。

精确检索虽然能够保持查询的准确性和全面性，但是在处理大量数据时，全文扫描通常会导致显著的性能瓶颈。

```
                                                               QUERY PLAN                                                                
-----------------------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=13.83..13.83 rows=1 width=178)
   Sort Key: ((feature <-> '[0,1,2,3,4,5]'::vector))
   ->  Seq Scan on gist_info  (cost=0.00..13.82 rows=1 width=178)
         Filter: ((distance > 10::double precision) AND (distance < 20::double precision) AND ((src_location)::text = 'zhejiang'::text))
(4 rows)
```

**第二类：近似检索**

根据索引查询是一种利用索引结构来快速定位目标数据的方法。索引的作用类似书籍的目录，通过索引可以直接找到所需的数据，而无需对整个表进行逐行扫描。索引查询的执行计划包含以下步骤：首先通过向量索引查询与输入的景点图片最接近的`k`行，然后根据结构化条件过滤出符合距离范围的数据。

`Ann Index Scan`（近似最近邻索引扫描）通过高效的索引结构显著减少查询所需的时间和计算资源。相较于精确检索，基于向量索引扫描的查询方法能够在处理高维数据时表现出极大的性能优势。

```
                                                            QUERY PLAN                                                             
-----------------------------------------------------------------------------------------------------------------------------------
 Ann Index Scan using gist_info_feature_idx on gist_info  (cost=4.61..48.43 rows=1 width=178)
   Order By: (feature <-> '[0,1,2,3,4,5]'::vector)
   Filter: ((distance > 10::double precision) AND (distance < 20::double precision) AND ((src_location)::text = 'zhejiang'::text))
(3 rows)
```

## 3. 全文检索

[全文检索](../sql_reference/full_text_retrieval.md)（Full-Text Search, FTS）是一项能够解析自然语言中的单词和词语，并基于关键词在数据库中查找和检索文本数据，最终按文档相关性对结果进行排序的技术。openGauss提供了完整的全文检索功能，包含特定的数据类型及排序函数。

以下通过一个案例展示全文检索的基本流程。假设有一个存储原始文档数据的表格`chunks_table_test`，主键字段为`chunk_id`，文本字段为`chunk_content`，需要根据输入的文本查询表格中关联的所有文档。实现全文检索功能需遵循以下主要步骤。

### 3.1 文本搜索配置

文本搜索配置（Text Search Configuration）定义了将文档转换成`tsvector`所需的组件。

首先，创建一个名称为`testchcfg`的文本搜索配置，`chparser`适用于中英文搜索场景；若为全英文搜索场景，推荐使用`pg_catalog.english`。

```sql
-- 加载中文分词插件
openGauss=# CREATE EXTENSION chparser;
-- 创建用于处理中文和英文的文本搜索配置
openGauss=# CREATE TEXT SEARCH CONFIGURATION testchcfg (PARSER = chparser);
-- 创建用于处理全英文场景的文本搜索配置
openGauss=# CREATE TEXT SEARCH CONFIGURATION ts_conf ( COPY = pg_catalog.english );
-- 将文本搜索配置修改为使用simple字典处理名词、形容词、介词、代词和特殊类别词
openGauss=# ALTER TEXT SEARCH CONFIGURATION testchcfg ADD MAPPING FOR n,v,a,i,e,l WITH simple;
-- 查看文本搜索配置
openGauss=# \dF
```

### 3.2 创建用于全文检索的表格

```sql
--创建文档表格
openGauss=# CREATE TABLE chunks_table_test (chunk_id SERIAL PRIMARY KEY, chunk_content TEXT);
--创建索引
openGauss=# CREATE INDEX idx_chunks_table ON chunks_table_test USING GIN(to_tsvector('testchcfg', chunk_content));
--插入数据
openGauss=# INSERT INTO chunks_table_test VALUES(1, '北京市（Beijing），简称“京”，古称燕京、北平，是中华人民共和国首都、直辖市、国家中心城市、超大城市， [185]国务院批复确定的中国政治中心、文化中心、国际交往中心、科技创新中心， [1]中国历史文化名城和古都之一，世界一线城市');
```

### 3.3 执行全文检索

**分析文本**

```sql
openGauss=> SELECT to_tsvector('testchcfg', '中国的首都是北京');
            to_tsvector            
-----------------------------------
 '中国':1 '北京':4 '是':3 '首都':2
(1 row)
```

`to_tsvector`将文本文档解析为token，并将token简化词素，返回一个`tsvector`。其中`tsvector`中列出了词素及它们在文档中的位置。此外，停用词如上述示例中“的”会被过滤。

**查询文本**

```sql
openGauss=> SELECT to_tsquery('testchcfg', '中国的首都是哪里？');
       to_tsquery       
------------------------
 '中国' & '首都' & '是'
(1 row)
```

```sql
openGauss=> SELECT replace(to_tsquery('testchcfg', '中国的首都是哪里？')::text, '&', '|');
        replace         
------------------------
 '中国' | '首都' | '是'
(1 row)
```

`to_tsquery`可以将查询语句分割成单个token，每个token之间必须由布尔运算符`&`（AND）、`|`(OR)和`!`(NOT)连接，默认指定`&`作为连接符，由于`&`可能会导致某些查询过于严格和复杂，推荐将连接符替换成`|`，以提高查询的灵活性和容错性。

**综合排序结果**

```sql
openGauss=# SELECT chunk_content
FROM chunks_table_test, to_tsquery('testchcfg', '中国的首都是哪里？') query
WHERE to_tsvector('testchcfg', chunk_content) @@ query
ORDER BY ts_rank(to_tsvector('testchcfg', chunk_content), query, 1) DESC
LIMIT 2;
```

在上述SQL查询中：

- `to_tsvector@@to_tsquery`: `@@`是openGauss的全文检索匹配算子，当`tsvector`（docunment）匹配到`tsquery`（query）时返回true。
- `ts_rank(to_tsvector, to_tsquery, integer)`：openGauss提供了两个预置的[排序方法](../sql_reference/ranking_search_results.md)（`ts_rank`， `ts_rank_cd`），可将相关性最高的文档排在前面。同时，通过设置`integer`类型的标准化选项来定义文档长度的影响程度。

综合以上步骤，即可实现高效的全文检索。

## 4. 双路召回

双路召回（Dual Retrival）是一种结合了向量检索与全文检索的多维数据召回策略。

在传统的单一检索方式中，面对查询内容过于复杂或嵌入模型表现不佳的情况，检索结果往往难以另使用者满意。因此，双路召回策略通过结合两种不同类型的检索技术，弥补了单一检索策略的不足，从而实现更全面和灵活的数据召回。

在具体实现过程中，向量检索用于捕捉数据间的相似性，而全文检索则补充了基于关键字的召回能力。

在openGauss数据库中，该策略通过分别执行向量检索与全文检索获取候选数据，随后通过精排和后处理以优化召回结果，确保更高的召回率。

**示例：**

### 4.1 创建表

```sql
openGauss=# CREATE TABLE documents (
    id int NOT NULL,
    user_id int,
    document_id int,
    content TEXT,
    embedding vector(3),
    created_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);
```

### 4.2 创建向量索引

```sql
openGauss=# CREATE INDEX ON documents USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 200);
```

### 4.3 创建GIN索引

```sql
openGauss=# CREATE INDEX ON documents USING GIN(to_tsvector('testchcfg', content));
```

### 4.4 进行双路召回查询

```sql
openGauss=#
WITH combined AS 
(
       (
       SELECT id, user_id, document_id, content, created_time, updated_time,
       embedding <-> '[1, 2, 3]'::vector AS similarity, 1 AS source 
       FROM documents 
       ORDER BY embedding <-> '[1, 2, 3]'::vector 
       LIMIT 10
       )
       UNION ALL
       (
       SELECT id, user_id, document_id, content, created_time, updated_time,
       ts_rank(to_tsvector('testchcfg', content), query) AS similarity, 2 AS source
       FROM documents, to_tsquery('testchcfg', 'openGauss向量数据库新特性是什么？') query
       WHERE to_tsvector('testchcfg', content) @@ query
       ORDER BY ts_rank(to_tsvector('testchcfg', content), query) DESC
       LIMIT 10
       )
)
SELECT id, document_id, content, MAX(similarity) AS similarity, BIT_OR(source)
FROM combined
GROUP BY id, document_id, content
ORDER BY similarity DESC
LIMIT 10;
```

**计划：**

```
                                                                  QUERY PLAN                                                                   
-----------------------------------------------------------------------------------------------------------------------------------------------
 Limit  (cost=25.48..25.50 rows=10 width=64)
   ->  Sort  (cost=25.48..25.51 rows=13 width=64)
         Sort Key: (max(combined.similarity)) DESC
         ->  HashAggregate  (cost=25.10..25.23 rows=13 width=64)
               Group By Key: combined.id, combined.document_id, combined.content
               ->  Subquery Scan on combined  (cost=4.73..24.94 rows=13 width=52)
                     ->  Append  (cost=4.73..24.81 rows=13 width=92)
                           ->  Subquery Scan on "*SELECT* 1"  (cost=4.73..5.55 rows=10 width=92)
                                 ->  Limit  (cost=4.73..5.45 rows=10 width=92)
                                       ->  Ann Index Scan using documents_embedding_idx on documents  (cost=4.73..53.11 rows=670 width=92)
                                             Order By: (embedding <-> '[1,2,3]'::vector)
                           ->  Subquery Scan on "*SELECT* 2"  (cost=19.22..19.26 rows=3 width=92)
                                 ->  Limit  (cost=19.22..19.23 rows=3 width=92)
                                       ->  Sort  (cost=19.22..19.23 rows=3 width=92)
                                             Sort Key: (ts_rank(to_tsvector('testchcfg'::regconfig, hct.documents.content), query.query)) DESC
                                             ->  Nested Loop  (cost=12.03..19.20 rows=3 width=92)
                                                   ->  Function Scan on query  (cost=0.00..0.01 rows=1 width=32)
                                                   ->  Bitmap Heap Scan on documents  (cost=12.03..19.14 rows=3 width=60)
                                                         Recheck Cond: (to_tsvector('testchcfg'::regconfig, content) @@ query.query)
                                                         ->  Bitmap Index Scan on documents_to_tsvector_idx  (cost=0.00..12.03 rows=3 width=0)
                                                               Index Cond: (to_tsvector('testchcfg'::regconfig, content) @@ query.query)
```

以上执行计划通过`UNION ALL`合并向量检索以及全文检索的结果，获取最优的10条结果。
