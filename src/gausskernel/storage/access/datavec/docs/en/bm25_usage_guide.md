# BM25 Full-Text Search Indexing Usage Guide

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T12:06:33.527Z pushedAt=2026-07-30T12:23:58.585Z -->

This chapter introduces how to use the BM25 full-text search index in openGauss.

## 1. Installation and Deployment

Use Docker to implement containerized deployment of openGauss. This can simplify installation, configuration, and environment setup for DevOps users. For details, see [Installing the Container Image](../installation_guide/installing_the_container_image.md).

## 2. Syntax Overview

The BM25 full-text search indexing builds full-text indexes on the document column of a regular table, enabling efficient retrieval of documents.

- **Index Creation**

    The BM25 indexing support building full-text indexes on specified document columns, with parallel build support that significantly improves index construction speed for large text datasets. The syntax is as follows:

    ```bash
    -- Set the number of parallel build threads, ranging from 1 to 32. If not set, single-thread build is used by default
    ALTER TABLE {table_name} SET(parallel_workers=32);
    
    -- Build a BM25 index for the specified document column of the specified table (dictionary by default)
    CREATE INDEX {index_name} on {table_name} using bm25({document_column_name});

    -- Build a BM25 index for the specified document column of the specified table (custom dictionary directory)
    CREATE INDEX {index_name} on {table_name} using bm25({document_column_name}) WITH (dict_path='{absolute_path_to_ dictionary_directory}');
    ```

    For constraints related to BM25 index construction, see [BM25 Index Introduction](bm25_full_text_search_index.md)
    >[!NOTE]
    >
    >In the parallel index build scenario, the `Maxscore` parameter computed for a term in a document may deviate slightly from that in the serial build scenario. This may have a small probability of affecting the document pruning strategy of the DAAT Maxscore method during retrieval, causing some fluctuation in recall.
    >There may be some deviation in recall between serial and parallel index builds. Even among multiple parallel builds on the same data, some deviation may occur.

- **BM25 Index Operator**

Since BM25 index scanning requires specifying query terms, a new BM25 index operator `<&>` is introduced, indicating that documents are searched based on query terms. The usage is as follows:

   ```bash
   {document_column_name} <&> {query_term}
   ```

- **Index Scanning**

    - **Basic format**

        The goal of the BM25 index is to search for the top-n documents most relevant to the query terms from the document collection and return them to the user in descending order of relevance. The search syntax follows this fixed format:

       ```bash
       -- If LIMIT is not set, all documents relevant to the query terms are returned
       select * from {table_name} ORDER BY {document_column_name} <&> {query_term} DESC LIMIT n;
       ```

    - **BM25 index scanning via hints**

        ```bash
        -- -- Query the top-n documents with the highest scores
        SELECT /*+ indexscan (table_name index_name)*/ * FROM {table_name} ORDER BY {document_column_name} <&> {query_term} DESC LIMIT n;

        -- -- To view the returned document scores, display the scores as a virtual column.
        SELECT /*+ indexscan (table_name index_name)*/ *, {document_column_name} <&> {query_term} AS score FROM {table_name} ORDER BY {document_column_name} <&> {query_term} DESC LIMIT n;
        ```

    - **BM25 index scanning via GUC setting scanning**

        ```bash
        -- Disable sequential scanning
        set enable_seqscan = off;
        
        -- Enable index scanning
        set enable_indexscan = on;
      
        -- Query the top-n documents with the highest scores
        SELECT * FROM {table_name} ORDER BY {document_column_name} <&> {query_term} DESC LIMIT n;
        ```

    >[!NOTE] Note
    >
    >BM25 index scanning performance can be tuned through relevant GUC parameters. You can set them before executing statements. For details, see [BM25 Parameter Tuning](../database_reference/bm25_full_text_retrieval_index_parameters.md).

- **Index Deletion**

    ```bash
    DROP INDEX {index_name};
    ```

## 3. Custom Dictionary (dict_path)

`dict_path` is used to specify a dictionary directory for a single BM25 index, allowing different businesses to use different tokenization dictionaries.

- Must be an absolute path. The path must be under the `$GAUSSHOME` directory.
- The directory must contain the following files: `jieba.dict.utf8`, `hmm_model.utf8`, `user.dict.utf8`, `idf.utf8`, and `stop_words.utf8`.
- Modifying the `dict_path` of an existing index is not supported. If the custom dictionary is adjusted, the existing index will become invalid and must be dropped and recreated.
- In a primary-standby scenario, the dictionary directory contents on the primary and standby nodes must remain consistent, and the `GAUSSHOME` on both ends must also remain consistent.
- If `dict_path` is not set, the default dictionary is used.

## 4. Index Space Optimization Notes

The `7.0.0 LTS` release introduces BM25 index space optimization capabilities. Through a variable-sized block storage mechanism, space waste can be reduced in scenarios with small documents or small inverted index segments, improving index space utilization.

This optimization is an internal storage enhancement and does not change the SQL usage patterns of BM25.

BM25 indexes created in earlier versions remain usable in newer versions. To achieve lower space occupancy, it is recommended to drop the old index and rebuild it.

## 5. Example

This section demonstrates the index syntax described above through an example.

```bash
-- -- Create an ordinary table with id and document columns, where document stores text data
openGauss=# CREATE TABLE bm25_table
(
  id   INT,
  document TEXT
);

CREATE TABLE

-- -- Insert document data
INSERT INTO bm25_table VALUES(1, 'Bananas are tropical fruits');
INSERT INTO bm25_table VALUES(2, 'Xiao Ming likes eating bananas');

-- -- Create a BM25 index on the document column
openGauss=# CREATE INDEX bm25_index on bm25_table using bm25(document);
ALTER TABLE

-- -- Use the BM25 index to search for documents related to 'banana', view the query plan, and perform BM25 index scanning
openGauss=# EXPLAIN SELECT /*+ indexscan (bm25_table bm25_index)*/ *, document <&> 'banana' AS score FROM bm25_table ORDER BY document <&> 'banana' DESC;
                       QUERY PLAN                        
---------------------------------------------------------
 Index Scan using bm25_index on bm25_table  (cost=0.00..7.11 rows=1238 width=36)
    Order by: (document <&> 'banana'::text)
(2 rows)

-- -- Execute a query to retrieve documents related to 'banana' and display document scores
openGauss=# SELECT /*+ indexscan (bm25_table bm25_index)*/ *, document <&> 'banana' AS score FROM bm25_table ORDER BY document <&> 'banana' DESC;
id  |    document   |   score
1   |  Xiao Ming likes eating bananas  | .182321563363075
2   |  Bananas are tropical fruits  | .182321563363075
(2 rows)

-- -- Use the BM25 index to search for 'What does Xiaoming like to eat'.
openGauss=# SELECT /*+ indexscan (bm25_table bm25_index)*/ *, document <&> 'What does Xiaoming like to eat' AS score FROM bm25_table ORDER BY document <&> 'What does Xiaoming like to eat' DESC;
id  |    document   |   score
1   |  Xiao Ming likes eating bananas  | 1.3862943649292
(1 rows)

---- Drop the index
openGauss=# DROP INDEX bm25_index;
DROP INDEX 
```
