# OGAI Usage Guide

This document describes the environment preparation, system tables, system functions, and usage of openGauss AI (OGAI). For details about OGAI features, customer benefits, and constraints, see [OGAI Feature Description](../characteristic_description/ogai.md).

## Environment Preparation

### 1. Creating Encryption Key Files

OGAI uses encryption key files to encrypt and store the API key when you register a model. Before use, generate the key files on each database node:

```bash
gs_guc generate -S XXX -D $GAUSSHOME/bin -o ogai
```

This command generates the `ogai.key.cipher` and `ogai.key.rand` files in the `$GAUSSHOME/bin` directory. The `-S` option specifies the encryption password.

> [!NOTE]
> If you do not create the key files, OGAI cannot encrypt the API key written during model registration. Decryption reports the following error: "Make sure the ogai.key.cipher file exists and is valid."

### 2. Installing the Extension

Connect to the database and run:

```sql
CREATE EXTENSION ogai;
```

After installation, OGAI automatically creates the `ogai` schema and its system tables, functions, and triggers.

### 3. Configuring Asynchronous Vectorization (Optional)

To use asynchronous vectorization mode, enable the parameter in `postgresql.conf` and restart the database:

```ini
enable_async_ogai = on
```

For details about all OGAI Grand Unified Configuration (GUC) parameters, see [OGAI Parameters](../database_reference/ogai_parameters.md).

## System Tables

OGAI uses system tables in the `ogai` schema to manage configurations and tasks. Row-level security (RLS) policies are enabled for all tables, and you can access only the data that you create.

### `ogai.model_sources`

**Function:** Stores information about registered AI models.

**Table structure:**

| Column | Data Type | Constraint | Description |
|------|---------|------|------|
| id | BIGSERIAL | PRIMARY KEY | Auto-increment primary key |
| model_key | TEXT | NOT NULL, UNIQUE (together with `owner_name`) | Model identifier referenced in function calls |
| model_name | TEXT | NOT NULL | Model name, such as `text-embedding-ada-002` |
| model_provider | ogai.model_provider_type | NOT NULL | Model provider type enumeration |
| url | VARCHAR(2048) | NOT NULL | API endpoint or model file path |
| description | TEXT | DEFAULT '' | Model description |
| api_key | TEXT | - | API key, required for cloud models |
| owner_name | TEXT | NOT NULL | User name of the model owner |

**Model provider enumeration (`ogai.model_provider_type`):**

| Enumeration Value | Description | Use Case |
|--------|------|----------|
| openai | OpenAI API-compatible endpoint | Cloud API calls for all OpenAI-compatible services |
| Qwen | Alibaba Cloud Qwen API | Cloud API calls through Model Studio |
| ollama | Local LLM service | Local deployment that supports multiple open-source models |
| onnx | Local model in the ONNX format | High-performance local inference without network access |

**Example:**

```sql
-- View all model configurations for the current user
SELECT model_key, model_name, model_provider, url FROM ogai.model_sources;

-- Register a new model
INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, api_key, owner_name)
VALUES ('my_model', 'text-embedding-v3', 'Qwen',
        'https://dashscope.aliyuncs.com/compatible-mode/v1',
        'sk-xxx', CURRENT_USER);
```

### `ogai.vectorize_tasks`

**Function:** Stores configuration information for automatic vectorization tasks.

**Table structure:**

| Column | Data Type | Constraint | Description |
|------|---------|------|------|
| task_id | BIGSERIAL | PRIMARY KEY | Auto-increment task primary key |
| task_name | TEXT | NOT NULL, UNIQUE (together with `owner_name`) | Task name |
| type | ogai.task_type | NOT NULL | Task type: `sync` or `async` |
| index_type | TEXT | NOT NULL | Distance function for the vector index: `l2`, `ip`, or `cosine` |
| model_key | TEXT | NOT NULL | Identifier of the associated embedding model |
| src_schema | TEXT | NOT NULL | Schema that contains the source table |
| src_table | TEXT | NOT NULL | Source table name |
| src_col | TEXT | NOT NULL | Text column to vectorize |
| primary_key | TEXT | NOT NULL | Primary key column of the source table |
| method | ogai.table_method | NOT NULL | Storage method: `append` or `join` |
| dim | INTEGER | NOT NULL | Vector dimension |
| max_chunk_size | INTEGER | NOT NULL, DEFAULT 0 | Maximum chunk size. `0` disables chunking. |
| max_chunk_overlap | INTEGER | NOT NULL, DEFAULT 0 | Chunk overlap size |
| enable_bm25 | BOOLEAN | NOT NULL, DEFAULT true | Whether to enable a BM25 index |
| owner_name | NAME | NOT NULL, DEFAULT CURRENT_USER | Task owner |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Creation time |

**Task type enumeration (`ogai.task_type`):**

| Enumeration Value | Description |
|--------|------|
| sync | Processes vectorization synchronously and immediately |
| async | Processes vectorization asynchronously through a background worker process |

**Storage method enumeration (`ogai.table_method`):**

| Enumeration Value | Description | Created Objects |
|--------|------|-----------|
| append | Adds the `ogai_embedding` column to the source table | Vector column and index |
| join | Creates a separate vector table | Vector table, view, and index |

**Example:**

```sql
-- View all vectorization tasks for the current user
SELECT task_name, type, src_table, src_col, method, dim, enable_bm25
FROM ogai.vectorize_tasks;
```

### `ogai.vectorize_queue`

**Function:** Stores the processing queue for asynchronous vectorization tasks.

**Table structure:**

| Column | Data Type | Constraint | Description |
|------|---------|------|------|
| msg_id | BIGSERIAL | UNIQUE | Auto-increment message ID |
| task_id | INTEGER | - | Associated task ID |
| pk_value | INTEGER | - | Primary key value of the record to process |
| status | ogai.queue_status | NOT NULL, DEFAULT 'ready' | Processing status |
| vt | TIMESTAMP | NOT NULL | Visibility time for delayed processing |
| fail_reason | TEXT | DEFAULT NULL | Failure reason |
| create_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Creation time |
| update_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Update time |
| retry_count | INTEGER | DEFAULT 0 | Number of retries |
| owner_name | NAME | NOT NULL, DEFAULT CURRENT_USER | Owner |

**Queue status enumeration (`ogai.queue_status`):**

| Enumeration Value | Description |
|--------|------|
| ready | Waiting for processing |
| processing | Processing |
| completed | Completed |
| failed | Processing failed |

**Example:**

```sql
-- View queue status statistics
SELECT status, COUNT(*) FROM ogai.vectorize_queue GROUP BY status;

-- View failed tasks
SELECT msg_id, task_id, pk_value, fail_reason, retry_count
FROM ogai.vectorize_queue WHERE status = 'failed';
```

## System Functions

### Core AI Functions

#### `ogai_embedding`

**Function:** Converts text into a vector representation.

**Parameters:**

| Parameter | Type | Description |
|------|------|------|
| text | TEXT | Text to vectorize |
| model_key | TEXT | Identifier of a registered embedding model |
| dimension | INTEGER | Vector dimension, which must match the model output dimension |

**Syntax:**

```sql
SELECT ogai_embedding('openGauss is an open-source database', 'my_embed_model', 1536);
```

#### `ogai_generate`

**Function:** Calls an LLM to generate a response.

**Parameters:**

| Parameter | Type | Description |
|------|------|------|
| query | TEXT | User question or prompt |
| model_key | TEXT | Identifier of a registered chat model |

**Syntax:**

```sql
SELECT ogai_generate('What is a vector database?', 'qwen_chat');
```

#### `ogai_rerank`

**Function:** Reranks retrieval results by relevance.

**Parameters:**

| Parameter | Type | Description |
|------|------|------|
| query | TEXT | Query text |
| documents | TEXT[] | Array of documents to rerank. The array cannot be empty or contain `NULL`. |
| model_key | TEXT | Identifier of a registered reranking model |

**Return columns:**

| Column | Type | Description |
|------|------|------|
| origin_index | INTEGER | Index of the original document in the array, starting from `0` |
| document | TEXT | Document content |
| rerank_score | FLOAT8 | Reranking score. A higher score indicates greater relevance. |

**Syntax:**

```sql
SELECT * FROM ogai_rerank(
    'Database optimization',
    ARRAY['Index optimization', 'Backup strategy', 'Query plan'],
    'rerank_model'
) ORDER BY rerank_score DESC;
```

#### `ogai_chunk`

**Function:** Splits long text into chunks suitable for processing.

**Parameters:**

| Parameter | Type | Constraint                       | Description |
|------|------|--------------------------|------|
| document | TEXT | NOT NULL                 | Document content to chunk |
| max_chunk_size | INTEGER | > 0                     | Maximum chunk size in characters |
| max_chunk_overlap | INTEGER | >= 0 and < `max_chunk_size` | Overlap size between adjacent chunks |

**Syntax:**

```sql
SELECT * FROM ogai_chunk('Long text...', 500, 100);
```

#### `load_onnx_model`

**Function:** Loads an ONNX model into the memory cache.

**Parameters:**

| Parameter | Type | Description           |
|------|------|--------------|
| model_key | TEXT | Identifier of a registered ONNX model |

**Syntax:**

```sql
SELECT load_onnx_model('local_bge');
```

#### `unload_onnx_model`

**Function:** Unloads an ONNX model from the memory cache.

**Parameters:**

| Parameter | Type | Description           |
|------|------|--------------|
| model_key | TEXT | Identifier of a registered ONNX model |


**Syntax:**

```sql
SELECT unload_onnx_model('local_bge');
```

### Schema System Functions

The following functions are defined in the `ogai` schema. They manage vectorization tasks and perform retrieval operations.

#### `ogai.ai_vectorize`

**Function:** Creates an automatic vectorization task to vectorize table data.

**Syntax:**

```sql
ogai.ai_vectorize(
    p_task_name TEXT,
    p_task_type TEXT,
    p_index_type TEXT,
    p_embed_model TEXT,
    p_src_schema TEXT,
    p_src_table TEXT,
    p_src_col TEXT,
    p_primary_key TEXT,
    p_table_method TEXT,
    p_dim INTEGER,
    p_max_chunk_size INTEGER DEFAULT 1000,
    p_max_chunk_overlap INTEGER DEFAULT 200,
    p_enable_bm25 BOOLEAN DEFAULT true
) RETURNS TABLE(task_id INT, success BOOLEAN, processed_count INT, message TEXT)
```

**Parameters:**

| Parameter | Type | Required | Default | Description |
|------|------|------|--------|------|
| p_task_name | TEXT | Yes | - | Task name, which must be unique for the same user |
| p_task_type | TEXT | Yes | - | `sync`: synchronous processing. `async`: asynchronous background processing. |
| p_index_type | TEXT | Yes | - | Vector index type: `l2`, `ip`, or `cosine` |
| p_embed_model | TEXT | Yes | - | `model_key` of a registered embedding model |
| p_src_schema | TEXT | Yes | - | Schema that contains the source table |
| p_src_table | TEXT | Yes | - | Source table name |
| p_src_col | TEXT | Yes | - | Name of the text column to vectorize |
| p_primary_key | TEXT | Yes | - | Name of the primary key column in the source table |
| p_table_method | TEXT | Yes | - | `append`: adds a vector column to the source table. `join`: creates a separate vector table. |
| p_dim | INTEGER | Yes | - | Vector dimension, which must match the model output |
| p_max_chunk_size | INTEGER | No | 1000 | Maximum chunk size. `0` disables chunking. |
| p_max_chunk_overlap | INTEGER | No | 200 | Chunk overlap size |
| p_enable_bm25 | BOOLEAN | No | true | Whether to create a BM25 full-text index |

**Syntax:**

```sql
-- Synchronous mode with the append method
SELECT * FROM ogai.ai_vectorize(
    'simple_task', 'sync', 'cosine', 'my_embed_model',
    'public', 'articles', 'content', 'id',
    'append', 1536, 0, 0, true
);

-- Asynchronous mode with the join method and chunking enabled
SELECT * FROM ogai.ai_vectorize(
    'chunked_task', 'async', 'cosine', 'my_embed_model',
    'public', 'documents', 'content', 'id',
    'join', 1536, 500, 100, true
);
```

#### `ogai.search`

**Function:** Performs semantic retrieval based on vector similarity.

**Parameters:**

| Parameter | Type | Default | Description             |
|------|------|--------|----------------|
| p_task_name | TEXT | - | Name of the vectorization task (knowledge base) |
| p_query | TEXT | - | Query text, which is automatically vectorized |
| p_return_cols | TEXT | '' | Columns to return, separated by commas. An empty value returns all columns. |
| p_limit | INTEGER | 10 | Number of results to return |
| p_where_clause | TEXT | '' | Additional SQL filter condition |

**Example:**

```sql
-- Basic search
SELECT * FROM ogai.search('my_task', 'Database optimization methods');

-- Specify the columns and number of results to return
SELECT * FROM ogai.search('my_task', 'Database optimization', 'id, title, content', 5, '');
```

#### `ogai.hybrid_search`

**Function:** Performs hybrid retrieval that combines vector similarity with BM25 keyword matching.

**Parameters:**

| Parameter | Type | Default | Description |
|------|------|--------|------|
| p_task_name | TEXT | - | Name of the vectorization task (knowledge base) |
| p_query | TEXT | - | Query text, which is automatically vectorized |
| p_return_cols | TEXT | '' | Columns to return, separated by commas. An empty value returns all columns. |
| p_limit | INTEGER | 10 | Number of results to return |
| p_where_clause | TEXT | '' | Additional SQL filter condition |

**Prerequisites:**

- `p_enable_bm25` must be set to `true` when you create the task.
- The text column must be of the TEXT type.

**Syntax:**

```sql
-- Set the hybrid search weight
SET ogai.hybrid_search_ratio = 0.7;

-- Run a hybrid search
SELECT * FROM ogai.hybrid_search('my_task', 'openGauss database optimization', 'id, title', 10, '');
```

#### `ogai.rag`

**Function:** Performs end-to-end retrieval-augmented generation (RAG) question answering.

**Parameters:**

| Parameter | Type | Default | Description |
|------|------|--------|------|
| p_user_question | TEXT | - | User question |
| p_task_name | TEXT | - | Name of the vectorization task (knowledge base) |
| p_reranker_model | TEXT | - | `model_key` of the reranking model |
| p_chat_model | TEXT | - | `model_key` of the chat model |
| p_rerank_limit | INTEGER | 5 | Number of documents to retain after reranking |
| p_search_limit | INTEGER | 20 | Number of initial vector retrieval results |

**Process:**

1. Use vector search to retrieve `p_search_limit` relevant documents.
2. Use the reranking model to reorder the results.
3. Use the top `p_rerank_limit` documents as context.
4. Call the chat model to generate the final response.

**Syntax:**

```sql
SELECT ogai.rag(
    'What vector index types does openGauss support?',
    'knowledge_base_task',
    'rerank_model',
    'chat_model',
    5,
    20
);
```

#### `ogai.ai_unvectorize`

**Function:** Deletes a vectorization task and its related resources.

**Parameters:**

| Parameter          | Type | Default | Description                   |
|-------------|------|--------|----------------------|
| p_task_name | TEXT | - | Task name (knowledge base), which must be unique for the same user |

**Syntax:**

```sql
SELECT * FROM ogai.ai_unvectorize('my_task');
```

## Usage Guide

After you complete [environment preparation](#environment-preparation), you can start using OGAI.

### 1. Registering Models

```sql
INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, api_key, owner_name)
VALUES ('openai_embed', 'text-embedding-ada-002', 'openai',
        'https://api.openai.com/v1', 'sk-xxx', CURRENT_USER);

INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, api_key, owner_name)
VALUES ('qwen_embed', 'text-embedding-v3', 'Qwen',
        'https://dashscope.aliyuncs.com/compatible-mode/v1', 'sk-xxx', CURRENT_USER);

INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, owner_name)
VALUES ('ollama_embed', 'nomic-embed-text', 'ollama', 'http://localhost:11434', CURRENT_USER);

INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, owner_name)
VALUES ('onnx_bge', 'bge-small-zh', 'onnx', '/data/models/bge-small-zh.onnx', CURRENT_USER);
```

### 2. Complete Example

```sql
-- 1. Register models
INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, api_key, owner_name)
VALUES
    ('embed_model', 'text-embedding-v3', 'Qwen',
     'https://dashscope.aliyuncs.com/compatible-mode/v1', 'your-api-key', CURRENT_USER),
    ('chat_model', 'qwen-turbo', 'Qwen',
     'https://dashscope.aliyuncs.com/compatible-mode/v1', 'your-api-key', CURRENT_USER),
    ('rerank_model', 'gte-rerank', 'Qwen',
     'https://dashscope.aliyuncs.com/compatible-mode/v1', 'your-api-key', CURRENT_USER);

-- 2. Create a knowledge base table
CREATE TABLE knowledge_base (
    id SERIAL PRIMARY KEY,
    title TEXT,
    content TEXT,
    category TEXT
);

-- 3. Insert data
INSERT INTO knowledge_base (title, content, category) VALUES
('Introduction to openGauss', 'openGauss is an open-source relational database...', 'Product'),
('Vector indexes', 'openGauss supports Hierarchical Navigable Small World (HNSW), Inverted File Flat (IVFFlat), and other vector indexes...', 'Technology');

-- 4. Create a vectorization task
SELECT * FROM ogai.ai_vectorize(
    'kb_task', 'sync', 'cosine', 'embed_model',
    'public', 'knowledge_base', 'content', 'id',
    'join', 1024, 500, 100, true
);

-- 5. Perform a vector search
SELECT * FROM ogai.search('kb_task', 'What is openGauss', 'id, title', 5, '');

-- 6. Perform a hybrid search
SET ogai.hybrid_search_ratio = 0.7;
SELECT * FROM ogai.hybrid_search('kb_task', 'openGauss vector indexes', '', 5, '');

-- 7. Perform RAG question answering
SELECT ogai.rag('What indexes does openGauss support?', 'kb_task', 'rerank_model', 'chat_model', 5, 20);

-- 8. Clean up
SELECT * FROM ogai.ai_unvectorize('kb_task');
```
