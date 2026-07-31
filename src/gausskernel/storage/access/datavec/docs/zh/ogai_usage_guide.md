# OGAI 使用指南

本文档介绍 OGAI（openGauss AI）的环境准备、系统表、系统函数及使用方法。关于 OGAI 的特性介绍、客户价值和约束说明，请参考 [OGAI 特性文档](https://docs.opengauss.org/zh/docs/latest/characteristic_description/ogai.html)。

## 环境准备

### 1. 创建加密密钥文件

OGAI 使用加密密钥文件对注册模型时的 API Key 进行加密存储。使用前需要在各数据库节点上生成密钥文件：

```bash
gs_guc generate -S XXX -D $GAUSSHOME/bin -o ogai
```

执行后会在 `$GAUSSHOME/bin` 目录下生成 `ogai.key.cipher` 和 `ogai.key.rand` 两个文件。其中 `-S` 指定加密口令。

> [!NOTE]说明
> 如果未创建密钥文件，注册模型时写入的 API Key 将无法被加密，解密时会报错提示 "Make sure the ogai.key.cipher file exists and is valid."。

### 2. 安装插件

连接数据库后执行：

```sql
CREATE EXTENSION ogai;
```

安装后会自动创建 `ogai` schema 及其下的系统表、函数和触发器。

### 3. 配置异步向量化（可选）

如需使用异步向量化模式，需要在 `postgresql.conf` 中开启参数并重启数据库：

```ini
enable_async_ogai = on
```

OGAI 相关 GUC 参数的完整说明请参考 [OGAI 参数](https://docs.opengauss.org/zh/docs/latest/database_reference/ogai_parameters.html)。

## 系统表

OGAI 使用 `ogai` schema 下的系统表管理配置和任务，所有表都启用了行级安全策略（Row Level Security），用户只能访问自己创建的数据。

### ogai.model_sources

**功能：** 存储注册的 AI 模型信息。

**表结构：**

| 列名 | 数据类型 | 约束 | 说明 |
|------|---------|------|------|
| id | BIGSERIAL | PRIMARY KEY | 自增主键 |
| model_key | TEXT | NOT NULL, UNIQUE(与owner_name联合) | 模型标识符，用于函数调用时引用 |
| model_name | TEXT | NOT NULL | 模型名称，如 `text-embedding-ada-002` |
| model_provider | ogai.model_provider_type | NOT NULL | 模型提供者类型枚举 |
| url | VARCHAR(2048) | NOT NULL | API 地址或模型文件路径 |
| description | TEXT | DEFAULT '' | 模型描述 |
| api_key | TEXT | - | API 密钥（云端模型需要） |
| owner_name | TEXT | NOT NULL | 模型所有者用户名 |

**模型提供者枚举（ogai.model_provider_type）：**

| 枚举值 | 说明 | 适用场景 |
|--------|------|----------|
| openai | OpenAI API 兼容接口 | 云端 API 调用，支持所有 OpenAI 兼容的服务 |
| Qwen | 阿里云通义千问 API | 云端 API 调用，使用百炼平台 |
| ollama | 本地大模型服务 | 本地部署，支持多种开源模型 |
| onnx | ONNX 格式本地模型 | 本地高性能推理，无需网络 |

**示例：**

```sql
-- 查看当前用户的所有模型配置
SELECT model_key, model_name, model_provider, url FROM ogai.model_sources;

-- 注册新模型
INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, api_key, owner_name)
VALUES ('my_model', 'text-embedding-v3', 'Qwen', 
        'https://dashscope.aliyuncs.com/compatible-mode/v1', 
        'sk-xxx', CURRENT_USER);
```

### ogai.vectorize_tasks

**功能：** 存储自动向量化任务的配置信息。

**表结构：**

| 列名 | 数据类型 | 约束 | 说明 |
|------|---------|------|------|
| task_id | BIGSERIAL | PRIMARY KEY | 任务自增主键 |
| task_name | TEXT | NOT NULL, UNIQUE(与owner_name联合) | 任务名称 |
| type | ogai.task_type | NOT NULL | 任务类型：`sync` 或 `async` |
| index_type | TEXT | NOT NULL | 向量索引距离函数：`l2`、`ip`、`cosine` |
| model_key | TEXT | NOT NULL | 关联的嵌入模型标识 |
| src_schema | TEXT | NOT NULL | 源表所在 schema |
| src_table | TEXT | NOT NULL | 源表名称 |
| src_col | TEXT | NOT NULL | 待向量化的文本列 |
| primary_key | TEXT | NOT NULL | 源表主键列 |
| method | ogai.table_method | NOT NULL | 存储方式：`append` 或 `join` |
| dim | INTEGER | NOT NULL | 向量维度 |
| max_chunk_size | INTEGER | NOT NULL, DEFAULT 0 | 最大分块大小（0表示不分块） |
| max_chunk_overlap | INTEGER | NOT NULL, DEFAULT 0 | 分块重叠大小 |
| enable_bm25 | BOOLEAN | NOT NULL, DEFAULT true | 是否启用 BM25 索引 |
| owner_name | NAME | NOT NULL, DEFAULT CURRENT_USER | 任务所有者 |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 创建时间 |

**任务类型枚举（ogai.task_type）：**

| 枚举值 | 说明 |
|--------|------|
| sync | 同步处理，立即执行向量化 |
| async | 异步处理，通过后台工作进程执行 |

**存储方式枚举（ogai.table_method）：**

| 枚举值 | 说明 | 创建的对象 |
|--------|------|-----------|
| append | 在源表添加 `ogai_embedding` 列 | 向量列 + 索引 |
| join | 创建独立向量表 | 向量表 + 视图 + 索引 |

**示例：**

```sql
-- 查看当前用户的所有向量化任务
SELECT task_name, type, src_table, src_col, method, dim, enable_bm25 
FROM ogai.vectorize_tasks;
```

### ogai.vectorize_queue

**功能：** 存储异步向量化任务的处理队列。

**表结构：**

| 列名 | 数据类型 | 约束 | 说明 |
|------|---------|------|------|
| msg_id | BIGSERIAL | UNIQUE | 消息自增ID |
| task_id | INTEGER | - | 关联的任务ID |
| pk_value | INTEGER | - | 待处理记录的主键值 |
| status | ogai.queue_status | NOT NULL, DEFAULT 'ready' | 处理状态 |
| vt | TIMESTAMP | NOT NULL | 可见时间（用于延迟处理） |
| fail_reason | TEXT | DEFAULT NULL | 失败原因 |
| create_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| update_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | 更新时间 |
| retry_count | INTEGER | DEFAULT 0 | 重试次数 |
| owner_name | NAME | NOT NULL, DEFAULT CURRENT_USER | 所有者 |

**队列状态枚举（ogai.queue_status）：**

| 枚举值 | 说明 |
|--------|------|
| ready | 等待处理 |
| processing | 处理中 |
| completed | 已完成 |
| failed | 处理失败 |

**示例：**

```sql
-- 查看队列状态统计
SELECT status, COUNT(*) FROM ogai.vectorize_queue GROUP BY status;

-- 查看失败的任务
SELECT msg_id, task_id, pk_value, fail_reason, retry_count 
FROM ogai.vectorize_queue WHERE status = 'failed';
```

## 系统函数

### 核心 AI 函数

#### ogai_embedding

**功能：** 将文本转换为向量表示。

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| text | TEXT | 待向量化的文本内容 |
| model_key | TEXT | 已注册的嵌入模型标识 |
| dimension | INTEGER | 向量维度（需与模型输出维度匹配） |

**语法**

```sql
SELECT ogai_embedding('openGauss是一款开源数据库', 'my_embed_model', 1536);
```

#### ogai_generate

**功能：** 调用大语言模型生成回答。

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| query | TEXT | 用户问题或提示词 |
| model_key | TEXT | 已注册的对话模型标识 |

**语法：**

```sql
SELECT ogai_generate('什么是向量数据库？', 'qwen_chat');
```

#### ogai_rerank

**功能：** 对检索结果进行相关性重排序。

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| query | TEXT | 查询文本 |
| documents | TEXT[] | 待重排序的文档数组（不能为空，不能包含 NULL） |
| model_key | TEXT | 已注册的重排序模型标识 |

**返回列：**

| 列名 | 类型 | 说明 |
|------|------|------|
| origin_index | INTEGER | 原始文档在数组中的索引（从0开始） |
| document | TEXT | 文档内容 |
| rerank_score | FLOAT8 | 重排序分数（越高越相关） |

**语法：**

```sql
SELECT * FROM ogai_rerank(
    '数据库优化',
    ARRAY['索引优化', '备份策略', '查询计划'],
    'rerank_model'
) ORDER BY rerank_score DESC;
```

#### ogai_chunk

**功能：** 将长文本分割为适合处理的片段。

**参数说明：**

| 参数 | 类型 | 约束                       | 说明 |
|------|------|--------------------------|------|
| document | TEXT | NOT NULL                 | 待分块的文档内容 |
| max_chunk_size | INTEGER | \> 0                     | 最大分块大小（字符数） |
| max_chunk_overlap | INTEGER | \>= 0 且 < max_chunk_size | 相邻分块的重叠大小 |

**语法：**

```sql
SELECT * FROM ogai_chunk('长文本...', 500, 100);
```

#### load_onnx_model

**功能：** 将 ONNX 模型加载到内存缓存中。

**参数说明：**

| 参数 | 类型 | 说明           |
|------|------|--------------|
| model_key | TEXT | 已注册的ONNX模型标识 |

**语法：**

```sql
SELECT load_onnx_model('local_bge');
```

#### unload_onnx_model

**功能：** 从内存缓存中卸载 ONNX 模型。

**参数说明：**

| 参数 | 类型 | 说明           |
|------|------|--------------|
| model_key | TEXT | 已注册的ONNX模型标识 |


**语法：**

```sql
SELECT unload_onnx_model('local_bge');
```

### Schema 系统函数

以下函数定义在 `ogai` schema 中，用于管理向量化任务和执行检索操作。

#### ogai.ai_vectorize

***功能：** 创建自动向量化任务，对表数据进行向量化处理。

**语法：**

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

**参数详解：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| p_task_name | TEXT | 是 | - | 任务名称，同一用户下唯一 |
| p_task_type | TEXT | 是 | - | `sync`：同步处理；`async`：异步后台处理 |
| p_index_type | TEXT | 是 | - | 向量索引类型：`l2`、`ip`、`cosine` |
| p_embed_model | TEXT | 是 | - | 已注册的嵌入模型 model_key |
| p_src_schema | TEXT | 是 | - | 源表所在 schema |
| p_src_table | TEXT | 是 | - | 源表名称 |
| p_src_col | TEXT | 是 | - | 待向量化的文本列名 |
| p_primary_key | TEXT | 是 | - | 源表主键列名 |
| p_table_method | TEXT | 是 | - | `append`：在源表添加向量列；`join`：创建独立向量表 |
| p_dim | INTEGER | 是 | - | 向量维度（需与模型输出匹配） |
| p_max_chunk_size | INTEGER | 否 | 1000 | 最大分块大小（0表示不分块） |
| p_max_chunk_overlap | INTEGER | 否 | 200 | 分块重叠大小 |
| p_enable_bm25 | BOOLEAN | 否 | true | 是否创建 BM25 全文索引 |

**语法：**

```sql
-- 同步模式，append 方式
SELECT * FROM ogai.ai_vectorize(
    'simple_task', 'sync', 'cosine', 'my_embed_model',
    'public', 'articles', 'content', 'id',
    'append', 1536, 0, 0, true
);

-- 异步模式，join 方式，启用分块
SELECT * FROM ogai.ai_vectorize(
    'chunked_task', 'async', 'cosine', 'my_embed_model',
    'public', 'documents', 'content', 'id',
    'join', 1536, 500, 100, true
);
```

#### ogai.search

**功能：** 基于向量相似度进行语义检索。

**参数说明：**

| 参数 | 类型 | 默认值 | 说明             |
|------|------|--------|----------------|
| p_task_name | TEXT | - | 向量化任务(知识库)名称   |
| p_query | TEXT | - | 查询文本（会自动向量化）   |
| p_return_cols | TEXT | '' | 返回列（逗号分隔，空为全部） |
| p_limit | INTEGER | 10 | 返回结果数量         |
| p_where_clause | TEXT | '' | 额外 SQL 过滤条件    |

**示例：**

```sql
-- 基础搜索
SELECT * FROM ogai.search('my_task', '数据库优化方法');

-- 指定返回列和数量
SELECT * FROM ogai.search('my_task', '数据库优化', 'id, title, content', 5, '');
```

#### ogai.hybrid_search

**功能：** 结合向量相似度与 BM25 关键词匹配的混合检索。

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| p_task_name | TEXT | - | 向量化任务(知识库)名称 |
| p_query | TEXT | - | 查询文本（会自动向量化） |
| p_return_cols | TEXT | '' | 返回列（逗号分隔，空为全部） |
| p_limit | INTEGER | 10 | 返回结果数量 |
| p_where_clause | TEXT | '' | 额外 SQL 过滤条件 |

**前置条件：**

- 任务创建时 `p_enable_bm25` 必须为 `true`
- 文本列必须为 TEXT 类型

**语法：**

```sql
-- 设置混合搜索权重
SET ogai.hybrid_search_ratio = 0.7;

-- 执行混合搜索
SELECT * FROM ogai.hybrid_search('my_task', 'openGauss数据库优化', 'id, title', 10, '');
```

#### ogai.rag

**功能：** 端到端的检索增强生成问答。

**参数说明：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| p_user_question | TEXT | - | 用户问题 |
| p_task_name | TEXT | - | 向量化任务(知识库)名称 |
| p_reranker_model | TEXT | - | 重排序模型 model_key |
| p_chat_model | TEXT | - | 对话模型 model_key |
| p_rerank_limit | INTEGER | 5 | 重排序后保留的文档数量 |
| p_search_limit | INTEGER | 20 | 初始向量检索数量 |

**执行流程：**

1. 使用向量搜索检索 `p_search_limit` 篇相关文档
2. 使用重排序模型对结果重新排序
3. 取 Top `p_rerank_limit` 篇文档作为上下文
4. 调用对话模型生成最终回答

**语法：**

```sql
SELECT ogai.rag(
    'openGauss支持哪些向量索引类型？',
    'knowledge_base_task',
    'rerank_model',
    'chat_model',
    5,
    20
);
```

#### ogai.ai_unvectorize

**功能：** 删除向量化任务及相关资源。

**参数说明：**

| 参数          | 类型 | 默认值 | 说明                   |
|-------------|------|--------|----------------------|
| p_task_name | TEXT | - | 向量化任务(知识库)名称，同一用户下唯一 |

**语法：**

```sql
SELECT * FROM ogai.ai_unvectorize('my_task');
```

## 使用指导

完成[环境准备](#环境准备)后，即可开始使用 OGAI。

### 1. 注册模型

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

### 2. 完整示例

```sql
-- 1. 注册模型
INSERT INTO ogai.model_sources (model_key, model_name, model_provider, url, api_key, owner_name)
VALUES 
    ('embed_model', 'text-embedding-v3', 'Qwen', 
     'https://dashscope.aliyuncs.com/compatible-mode/v1', 'your-api-key', CURRENT_USER),
    ('chat_model', 'qwen-turbo', 'Qwen',
     'https://dashscope.aliyuncs.com/compatible-mode/v1', 'your-api-key', CURRENT_USER),
    ('rerank_model', 'gte-rerank', 'Qwen',
     'https://dashscope.aliyuncs.com/compatible-mode/v1', 'your-api-key', CURRENT_USER);

-- 2. 创建知识库表
CREATE TABLE knowledge_base (
    id SERIAL PRIMARY KEY,
    title TEXT,
    content TEXT,
    category TEXT
);

-- 3. 插入数据
INSERT INTO knowledge_base (title, content, category) VALUES
('openGauss简介', 'openGauss是一款开源关系型数据库...', '产品'),
('向量索引', 'openGauss支持HNSW、IVFFlat等向量索引...', '技术');

-- 4. 创建向量化任务
SELECT * FROM ogai.ai_vectorize(
    'kb_task', 'sync', 'cosine', 'embed_model',
    'public', 'knowledge_base', 'content', 'id',
    'join', 1024, 500, 100, true
);

-- 5. 向量搜索
SELECT * FROM ogai.search('kb_task', '什么是openGauss', 'id, title', 5, '');

-- 6. 混合搜索
SET ogai.hybrid_search_ratio = 0.7;
SELECT * FROM ogai.hybrid_search('kb_task', 'openGauss向量索引', '', 5, '');

-- 7. RAG问答
SELECT ogai.rag('openGauss支持哪些索引？', 'kb_task', 'rerank_model', 'chat_model', 5, 20);

-- 8. 清理
SELECT * FROM ogai.ai_unvectorize('kb_task');
