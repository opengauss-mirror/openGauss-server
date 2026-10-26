# 使用Feast和openGauss构建知识工程在线特征与向量检索

## 概述

在知识工程（Knowledge Engineering）项目中，应用通常需要同时管理结构化在线特征和可检索的向量表示。例如，推荐系统需要按实体读取实时特征，RAG（Retrieval-Augmented Generation）系统需要根据查询向量召回相关文档。Feast负责特征定义、注册和访问接口，openGauss负责在线数据的持久化、事务处理和向量相似性查询。

本方案使用Feast PostgreSQL Online Store接口，并通过轻量适配器连接openGauss：

- openGauss作为online feature store backend，保存结构化特征、实体键、时间戳和更新后的在线值。
- openGauss作为vector backend，保存文档向量，并使用向量距离操作符完成TopK检索。
- Feast统一管理Feature View、PushSource和FeatureStore API，应用代码不需要直接拼接upsert SQL。
- openGauss的向量数据库能力为知识库召回提供存储和检索基础，工具编排层可以在召回结果之上继续调用模型或业务工具。

该文档介绍项目级集成方式。它不是单独的向量数据库安装指南；数据库安装、向量类型和索引能力请参阅[向量数据库特性说明](https://docs.opengauss.org/zh/docs/latest/datavec/datavec_overview.html)。

## 整体架构

典型数据流如下：

```text
业务应用 / Agent 编排
        |
        | Feast FeatureStore.push / get_online_features / retrieve_online_documents
        v
Feast Feature Repo 与 Online Store 适配层
        |
        | SQL、事务、实体键、特征值和向量查询
        v
openGauss
  |-- 结构化在线特征表：实时状态、画像、统计值
  `-- 向量特征表：文档 embedding、标题和实体标识
```

在该架构中，Feast解决“特征如何定义和访问”的问题，openGauss解决“数据如何可靠存储和检索”的问题。二者组合后，工具编排流程可以先从openGauss读取实时特征和知识片段，再把结果传递给模型、检索器或其他业务工具。

## 安装与配置

项目需要Python 3.11或更高版本、Feast 0.65.0以及`psycopg`驱动。适配器代码位于官方KunpengRAG仓库的`Feast_OpenGauss`目录，可按以下步骤下载并安装：

```bash
git clone https://gitee.com/kunpeng_compute/KunpengRAG.git
cd KunpengRAG/Feast_OpenGauss
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
python -m pip check
```

安装适配器后，使用Feast Feature Repo中的配置文件指定openGauss：

```yaml
online_store:
  type: feast_opengauss.online_store.OpenGaussOnlineStore
  host: <OPENGAUSS_HOST>
  port: <OPENGAUSS_PORT>
  database: <OPENGAUSS_DATABASE>
  db_schema: <OPENGAUSS_SCHEMA>
  user: <OPENGAUSS_USER>
  password: <OPENGAUSS_PASSWORD>
  sslmode: disable
  vector_enabled: true
  similarity: L2
```

实际部署时应使用环境变量注入或secret manager提供凭据。不要把真实密码、私网地址或密钥提交到代码仓库。`feature_store.example.yaml`只用于说明字段，不应作为生产运行时配置。

初始化Feature Repo：

```bash
cd feature_repo
feast apply
```

`feast apply`会根据Feature View和Online Store配置创建或更新所需的在线存储结构。普通应用不需要修改Feast源码，也不需要设置`PYTHONPATH`。

## 结构化在线特征

使用PushSource写入实时特征：

```python
import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo")
now = pd.Timestamp.now(tz="UTC")
store.push(
    "driver_stats_push_source",
    pd.DataFrame([
        {"driver_id": 1, "event_timestamp": now, "score": 2.0, "trips": 11},
    ]),
)
```

按单个实体或批量实体读取：

```python
result = store.get_online_features(
    features=["driver_stats:score", "driver_stats:trips"],
    entity_rows=[{"driver_id": 1}, {"driver_id": 2}],
).to_dict()
```

适配器保留Feast的实体键、时间戳、批量读取和事务语义。openGauss侧使用与其兼容的写入upsert语法，应用层不需要直接实现数据库方言。

## 向量特征与知识召回

知识工程流程可以把文档切分、embedding生成和元数据写入同一个Feature Repo。示例中的`document_embeddings` Feature View包含：

- `document_id`实体键，用于把召回结果关联回原始文档；
- `embedding`三维示例向量，实际项目应按模型输出维度配置；
- `title`等普通特征，用于展示或后续工具调用。

写入向量特征：

```python
store.push(
    "document_embeddings_push_source",
    pd.DataFrame([
        {
            "document_id": 10,
            "event_timestamp": now,
            "embedding": [0.1, 0.0, 0.0],
            "title": "openGauss知识库文档",
        },
    ]),
)
```

使用查询向量执行TopK检索：

```python
matches = store.retrieve_online_documents(
    query=[0.0, 0.0, 0.0],
    top_k=3,
    features=["document_embeddings:embedding"],
    distance_metric="L2",
).to_dict()
```

openGauss在这里承担vector backend角色：向量值存储在数据库中，距离计算和排序在数据库侧完成。应用或Agent编排器可以把`document_id`、`title`和召回距离传给下游生成工具，形成“查询理解→向量召回→上下文组织→回答生成”的知识工程流程。

## 工具编排中的职责边界

建议将职责划分如下：

| 组件 | 主要职责 |
| --- | --- |
| 业务应用或Agent | 接收请求、决定调用哪些工具、组织最终结果 |
| Embedding服务 | 将文档和用户查询转换为向量 |
| Feast | 管理实体、Feature View、PushSource和统一访问API |
| openGauss | 持久化在线特征、向量数据和元数据，并执行事务与相似性查询 |
| 文档或对象存储 | 保存原始文档和较大附件，数据库保存可检索的标识和必要元数据 |

openGauss不是Agent编排器本身。它为编排器提供低延迟、可事务更新的在线特征和向量召回能力；编排器根据这些结果继续调用模型、搜索、权限校验或业务系统工具。

## 使用注意事项

- 通过统一的Feast / FeatureStore入口操作在线特征和向量数据，避免绕过适配器维护Online Store状态。
- 生产环境使用最小权限账号，并通过环境变量或secret manager等安全方式提供凭据。
- 向量维度需要与Feature View声明一致，查询距离类型需要与项目配置匹配。

更多项目配置和示例代码可参考KunpengRAG的`Feast_OpenGauss`目录。
