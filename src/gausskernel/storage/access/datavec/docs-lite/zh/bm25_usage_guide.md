# BM25全文检索索引使用指南

本章节主要介绍openGauss中BM25全文检索索引使用指南。

## 1. 安装部署

使用Docker实现openGauss容器化部署，简化DevOps用户的安装、配置和环境设置，参考 [容器镜像安装](https://docs.opengauss.org/zh/docs/latest-lite/installation_guide/installing_the_container_image.html)。

## 2. 语法介绍

BM25全文检索索引对普通表的文档列构建全文索引，实现对文档的高效检索。

- **索引构建**
    
    BM25索引支持对指定文档列构建全文索引，支持并行构建，大幅提升大文本数据集的索引构建速度，语法如下：

    ```
    -- 设置并行构建线程数，设置范围1~32。不设置该参数时，默认单线程构建
    ALTER TABLE {表名称} SET(parallel_workers=32);
    
    -- 给指定表的指定文档列构建BM25索引（默认词典）
    CREATE INDEX {索引名称} on {表名称} using bm25({文档列名称});

    -- 给指定表的指定文档列构建BM25索引（自定义词典目录）
    CREATE INDEX {索引名称} on {表名称} using bm25({文档列名称}) WITH (dict_path='{词典目录绝对路径}');
    ```
    
    BM25索引构建的相关约束见[BM25索引介绍](bm25_full_text_search_index.md)
    >[!NOTE]说明
    >
    >并行构建索引场景下，计算一篇文档中词汇的Maxscore参数会和串行构建场景下存在一定偏差，小概率会影响检索过程中DAAT Maxscore方法文档剪枝策略，导致召回率有一定波动。
    >串行、并行构建索引召回率存在一定偏差。相同数据，多次执行并行构建，也可能存在一定偏差。
- **BM25索引操作符**
    
    由于BM25索引扫描需要指定查询词，因此新增BM25索引操作符：<&>，表示根据查询词来搜索相关的文档。使用方式如下：

   ```
   {文档列名称} <&> {查询词}
   ```

- **索引扫描**

    - **BM25索引扫描基本格式**
        
        BM25索引的目标是在文档数据集集中搜索出与查询词最相关的 n 篇文档，并按相关性由高到低的顺序返回给用户。其搜索语法下述固定格式：

       ```
       -- LIMIT 不设置时返回所有与查询词相关的文档
       select * from {表名称} ORDER BY {文档列名称} <&> {查询词} DESC LIMIT n;
       ```

    - **通过提示词的方式使用BM25索引扫描**

        ```
        -- 查询分数最高的 n 个文档
        SELECT /*+ indexscan (表名称 索引名称)*/ * FROM {表名称} ORDER BY {文档列名称} <&> {查询词} DESC LIMIT n;

        -- 如果想要查看返回的文档分数，可以将分数以虚拟列的方式展示
        SELECT /*+ indexscan (表名称 索引名称)*/ *, {文档列名称} <&> {查询词} AS score FROM {表名称} ORDER BY {文档列名称} <&> {查询词} DESC LIMIT n;
        ```

    - **通过扫描GUC设置使用BM25索引扫描**

        ```
        -- 关闭顺序扫描
        set enable_seqscan = off;
        
        -- 开启索引扫描
        set enable_indexscan = on;
      
        -- 查询分数最高的 n 个文档
        SELECT * FROM {表名称} ORDER BY {文档列名称} <&> {查询词} DESC LIMIT n;
        ```

    >[!NOTE]说明
    >
    >BM25索引扫描性能可以通过相关GUC参数调优，执行语句前可先进行参数设置，详情参考[BM25参数调优](./bm25_full_text_retrieval_index_parameters.md)。

- **索引删除**

    ```
    DROP INDEX {索引名称};
    ```

## 3. 自定义词典（dict_path）

`dict_path` 用于为单个 BM25 索引指定词典目录，便于不同业务使用不同分词词典。

- 必须是绝对路径。路径需在 `$GAUSSHOME` 目录下。
- 目录下需包含以下文件：`jieba.dict.utf8`、`hmm_model.utf8`、`user.dict.utf8`、`idf.utf8`、`stop_words.utf8`。
- 不支持修改已创建索引的 `dict_path`；如调整自定义词典，已有索引会失效，需删除并重建索引。
- 主备场景下，主机与备机的词典目录内容需要保持一致，且两端 `GAUSSHOME` 需要保持一致。
- 未设置 `dict_path` 时，使用默认词典。

## 4. 索引空间优化说明

`7.0.0 LTS` 版本引入 BM25 索引空间优化能力。通过可变大小块存储机制，在小文档或小倒排片段场景下可降低空间浪费，提升索引空间利用率。

该优化为内部存储能力增强，不改变 BM25 的 SQL 使用方式。

旧版本创建的 BM25 索引在新版本中仍可用；如需获得更低空间占用，建议删除旧索引并重建。

## 5. 示例

本节将通过实例对上述索引语法进行演示。

```
-- 创建普通表，包含id、document列，document存储文本数据
openGauss=# CREATE TABLE bm25_table
(
  id   INT,
  document TEXT
);

CREATE TABLE

-- 插入文档数据
INSERT INTO bm25_table VALUES(1, '香蕉是热带水果');
INSERT INTO bm25_table VALUES(2, '小明喜欢吃香蕉');

-- 为document列建立bm25索引
openGauss=# CREATE INDEX bm25_index on bm25_table using bm25(document);
ALTER TABLE

-- 使用bm25索引检索'香蕉'相关的文档，查看查询计划，走BM25索引
openGauss=# EXPLAIN SELECT /*+ indexscan (bm25_table bm25_index)*/ *, document <&> '香蕉' AS score FROM bm25_table ORDER BY document <&> '香蕉' DESC;
                       QUERY PLAN                        
---------------------------------------------------------
 Index Scan using bm25_index on bm25_table  (cost=0.00..7.11 rows=1238 width=36)
    Order by: (document <&> '香蕉'::text)
(2 rows)

-- 执行查询语句，检索'香蕉'相关的文档，并显示文档分数
openGauss=# SELECT /*+ indexscan (bm25_table bm25_index)*/ *, document <&> '香蕉' AS score FROM bm25_table ORDER BY document <&> '香蕉' DESC;
id  |    document   |   score
1   |  小明喜欢吃香蕉  | .182321563363075
2   |  香蕉是热带水果  | .182321563363075
(2 rows)

-- 使用bm25索引检索'小明喜欢吃什么'
openGauss=# SELECT /*+ indexscan (bm25_table bm25_index)*/ *, document <&> '小明喜欢吃什么' AS score FROM bm25_table ORDER BY document <&> '小明喜欢吃什么' DESC;
id  |    document   |   score
1   |  小明喜欢吃香蕉  | 1.3862943649292
(1 rows)

--删除索引
openGauss=# DROP INDEX bm25_index;
DROP INDEX 
```
