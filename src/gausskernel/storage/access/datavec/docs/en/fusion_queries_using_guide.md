# Fusion Query Usage Guide

This chapter mainly introduces the fusion query usage guide for the DataVec vector engine in openGauss.

## 1. Installation and Deployment

Use Docker to implement containerized deployment of openGauss with DataVec, simplifying installation, configuration, and environment setup for DevOps users. For details, see [Container Image Installation](https://docs.opengauss.org/en/docs/latest/installation_guide/installation_overview.html).

## 2. Fusion Query

Data sources are typically diverse, including structured data (such as text and numbers) and unstructured data (such as videos, images, and audio). To effectively store and retrieve different types of data, fusion query combines structured filtering and unstructured retrieval techniques, allowing users to use different data types and query methods in the same query to obtain more accurate unstructured data. openGauss integrates the capability of fusing structured and unstructured data queries.

openGauss deeply integrates the DataVec vector engine into the database kernel, enabling users to use ANN-related indexes for fusion queries when retrieving vectors.

openGauss fusion query has the following features:

- `Supports similarity search and JOIN with relational data`: In a fusion query, both vector-based similarity retrieval and association of results with relational data can be performed.
- `Comprehensive support for structured data types`: Compatible with all types of structured data, achieving seamless fusion.
- `Compatible with all SQL`: Including complex operations and functions, such as window analytic functions, stored procedures, and aggregations.

**Case Study:**

Suppose a travel platform needs to implement a function that finds tourist areas with the highest similarity to the user's local location within a distance of 10 to 20 kilometers. The inputs are the scenic spot image, local name, and distance range.

### 2.1 Creating a Table

```sql
openGauss=# CREATE TABLE gist_info (
    id int,
    src_location varchar(64),
    dist_location varchar(64),
    update_time timestamp,
    distance float,
    feature vector(6)
);

--Set the vector index.
openGauss=# CREATE INDEX ON gist_info USING HNSW(feature vector_l2_ops);
```

### 2.2 Fusion Query

```sql
openGauss=# SELECT dist_location 
FROM gist_info 
WHERE src_location = 'zhejiang'
AND distance > 10 AND distance < 20
ORDER BY feature <-> '[0,1,2,3,4,5]';
```

There are two main execution methods for fusion queries, and the corresponding execution plans are as follows:

**Type 1: Exact Search**

Exact search is a retrieval method that finds data satisfying the query conditions by traversing the entire table. It is suitable for scenarios with small data volumes or where a large amount of data must be processed and indexes cannot be effectively utilized. The execution plan of exact search includes the following steps: first sort by vector distance, then scan the entire table to obtain the data that meets the conditions.

Although exact search can maintain the accuracy and completeness of the query, a full-table scan usually causes significant performance bottlenecks when processing large amounts of data.

```
                                                               QUERY PLAN                                                                
-----------------------------------------------------------------------------------------------------------------------------------------
 Sort  (cost=13.83..13.83 rows=1 width=178)
   Sort Key: ((feature <-> '[0,1,2,3,4,5]'::vector))
   ->  Seq Scan on gist_info  (cost=0.00..13.82 rows=1 width=178)
         Filter: ((distance > 10::double precision) AND (distance < 20::double precision) AND ((src_location)::text = 'zhejiang'::text))
(4 rows)
```

**Type 2: Approximate Search**

Index-based query is a method that uses index structures to quickly locate target data. The role of an index is similar to the table of contents of a book: through the index, the required data can be found directly without scanning the entire table row by row. The execution plan of an index query includes the following steps: first query the `k` rows closest to the input scenic-spot image through the vector index, then filter out the data that meets the distance range based on structured conditions.

`Ann Index Scan` (approximate nearest neighbor index scan) significantly reduces the time and computational resources required for queries through an efficient index structure. Compared with exact search, the query method based on vector index scanning can demonstrate a great performance advantage when processing high-dimensional data.

```
                                                            QUERY PLAN                                                             
-----------------------------------------------------------------------------------------------------------------------------------
 Ann Index Scan using gist_info_feature_idx on gist_info  (cost=4.61..48.43 rows=1 width=178)
   Order By: (feature <-> '[0,1,2,3,4,5]'::vector)
   Filter: ((distance > 10::double precision) AND (distance < 20::double precision) AND ((src_location)::text = 'zhejiang'::text))
(3 rows)
```

## 3. Full-Text Search

[Full-text search](https://docs.opengauss.org/en/docs/latest/sql_reference/full_text_retrieval.html) (FTS) is a technology that parses words and phrases in natural language, searches and retrieves text data in the database based on keywords, and finally sorts the results by document relevance. openGauss provides complete full-text search capabilities, including specific data types and ranking functions.

The following case demonstrates the basic process of full-text search. Assume there is a table `chunks_table_test` that stores raw document data, with the primary key field `chunk_id` and the text field `chunk_content`. You need to query all documents associated with the input text in the table. To implement full-text search, follow these main steps.

### 3.1 Text Search Configuration

A text search configuration defines the components required to convert a document into a `tsvector`.

First, create a text search configuration named `testchcfg`. `chparser` is suitable for Chinese and English search scenarios; for English-only search scenarios, `pg_catalog.english` is recommended.

```sql
-- Load the Chinese word segmentation plugin.
openGauss=# CREATE EXTENSION chparser;
-- Create a text search configuration for processing Chinese and English.
openGauss=# CREATE TEXT SEARCH CONFIGURATION testchcfg (PARSER = chparser);
-- Create a text search configuration for English-only scenarios.
openGauss=# CREATE TEXT SEARCH CONFIGURATION ts_conf ( COPY = pg_catalog.english );
-- Modify the text search configuration to use the simple dictionary for processing nouns, adjectives, prepositions, pronouns, and special-category words.
openGauss=# ALTER TEXT SEARCH CONFIGURATION testchcfg ADD MAPPING FOR n,v,a,i,e,l WITH simple;
-- View the text search configuration.
openGauss=# \dF
```

### 3.2 Creating a Table for Full-Text Search

```sql
--Create the document table
openGauss=# CREATE TABLE chunks_table_test (chunk_id SERIAL PRIMARY KEY, chunk_content TEXT);
--Create the index
openGauss=# CREATE INDEX idx_chunks_table ON chunks_table_test USING GIN(to_tsvector('testchcfg', chunk_content));
--Insert data
openGauss=# INSERT INTO chunks_table_test VALUES(1, 'Beijing (Beijing), abbreviated as "Jing", was known in ancient times as Yanjing and Beiping. It is the capital of the People's Republic of China, a municipality directly under the Central Government, a national central city, and a megacity. [185] It is the political center, cultural center, international exchange center, and scientific and technological innovation center of China as approved by the State Council. [1] It is one of China's famous historical and cultural cities and ancient capitals, and a world-class first-tier city.');
```

### 3.3 Performing Full-Text Search

**Analyzing Text**

```sql
openGauss=> SELECT to_tsvector('testchcfg', 'The capital of China is Beijing');
            to_tsvector            
-----------------------------------
 'China':1 'Beijing':4 'is':3 'capital':2
(1 row)
```

`to_tsvector` parses a text document into tokens, reduces the tokens to lexemes, and returns a `tsvector`. The `tsvector` lists the lexemes and their positions in the document. In addition, stop words such as "of" in the example above are filtered out.

**Querying Text**

```sql
openGauss=> SELECT to_tsquery('testchcfg', 'Where is the capital of China?');
       to_tsquery       
------------------------
 'China' & 'capital' & 'is'
(1 row)
```

```sql
openGauss=> SELECT replace(to_tsquery('testchcfg', 'Where is the capital of China?')::text, '&', '|');
        replace         
------------------------
 'China' | 'capital' | 'is'
(1 row)
```

`to_tsquery` can split a query statement into individual tokens. Each token must be connected by the Boolean operators `&` (AND), `|` (OR), and `!` (NOT). By default, `&` is specified as the connector. Since `&` may make some queries overly strict and complex, it is recommended to replace the connector with `|` to improve query flexibility and fault tolerance.

**Comprehensive Ranking Results**

```sql
openGauss=# SELECT chunk_content
FROM chunks_table_test, to_tsquery('testchcfg', 'Where is the capital of China?') query
WHERE to_tsvector('testchcfg', chunk_content) @@ query
ORDER BY ts_rank(to_tsvector('testchcfg', chunk_content), query, 1) DESC
LIMIT 2;
```

In the preceding SQL query:

- `to_tsvector@@to_tsquery`: `@@` is the full-text search matching operator in openGauss. It returns true when the `tsvector` (document) matches the `tsquery` (query).
- `ts_rank(to_tsvector, to_tsquery, integer)`: openGauss provides two preset [ranking methods](https://docs.opengauss.org/en/docs/latest/sql_reference/ranking_search_results.html) (`ts_rank`, `ts_rank_cd`) that can rank the most relevant documents first. In addition, you can set the `integer` normalization option to define the degree to which document length affects the ranking.

By combining the preceding steps, you can implement efficient full-text search.

## 4. Dual Retrieval

Dual Retrieval is a multi-dimensional data recall strategy that combines vector retrieval and full-text search.

In traditional single retrieval methods, when the query content is too complex or the embedding model performs poorly, the retrieval results often fail to satisfy users. Therefore, the dual-channel recall strategy compensates for the shortcomings of a single retrieval strategy by combining two different types of retrieval techniques, thereby achieving more comprehensive and flexible data recall.

In the specific implementation, vector retrieval is used to capture the similarity between data, while full-text search supplements the keyword-based recall capability.

In the openGauss database, this strategy obtains candidate data by executing vector retrieval and full-text search separately, and then optimizes the recall results through fine ranking and post-processing to ensure a higher recall rate.

**Example:**

### 4.1 Creating a Table

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

### 4.2 Creating a Vector Index

```sql
openGauss=# CREATE INDEX ON documents USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 200);
```

### 4.3 Creating a GIN Index

```sql
openGauss=# CREATE INDEX ON documents USING GIN(to_tsvector('testchcfg', content));
```

### 4.4 Performing a Dual-Retrieval Query

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
       FROM documents, to_tsquery('testchcfg', 'What are the new features of the openGauss vector database?') query
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

**Plan:**

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

The execution plan above merges the results of vector search and full-text search through `UNION ALL` to obtain the top 10 optimal results.
