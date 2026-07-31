# Spring AI 接入 openGauss 指南

本文说明如何在 Spring AI 应用中接入 openGauss，用于向量检索和会话记忆持久化。内容面向实际业务项目接入，覆盖依赖引入、数据源配置、`VectorStore` 使用、`JDBC Chat Memory` 使用以及本地构建产物的安装方式。

openGauss 向量能力由 DataVec 提供。Spring AI 的 openGauss 集成是独立的 `VectorStore` 实现，使用 JDBC 访问 openGauss 的 `vector` 类型，不是 `pgvector` 集成的别名或简单复用。

## 1. 适用范围

当前接入能力包括：

- `Vector Store`：将文档向量写入 openGauss DataVec，并基于相似度执行 Top-K 检索。
- `Metadata Filter`：在相似度检索时使用 Spring AI 通用 metadata 过滤表达式。
- `JDBC Chat Memory`：将聊天消息持久化到 openGauss，供 `ChatMemory` 使用。
- 基础 schema 初始化：按配置自动创建向量表、索引以及 Chat Memory 表结构。

当前不包含：

- 面向 openGauss 全文检索的独立 Spring AI 抽象。
- 统一的全文检索加向量检索混合召回 API。
- `halfvec`、`sparsevec` 等高级 DataVec 类型的 Spring AI 封装。

如果业务需要全文检索或混合召回，可以在应用层组合 openGauss SQL、业务召回逻辑和 Spring AI `VectorStore` 检索结果。

## 2. 前提条件

接入前需要准备：

- 一个可访问的 openGauss 实例。
- 已启用 DataVec 的 openGauss 数据库环境。
- 可用的 JDBC 连接信息。
- 一个 Spring AI `EmbeddingModel`，用于生成文档向量。
- 如果使用 `JDBC Chat Memory`，需要项目中已有可用的 `DataSource`。

已验证的 JDBC URL 形式如下：

```properties
spring.datasource.url=jdbc:postgresql://localhost:15432/postgres
spring.datasource.username=gaussdb
spring.datasource.password=Huawei@123
```

说明：

- openGauss Vector Store 依赖 openGauss JDBC 驱动。
- 当前验证路径使用 `jdbc:postgresql://...` 形式的连接串。
- 如果你的环境支持其他 openGauss JDBC URL 形式，可以按项目标准自行验证。

## 3. 接入 Vector Store

### 3.1 引入依赖

Spring Boot 应用推荐优先使用 starter：

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-vector-store-opengauss</artifactId>
</dependency>
```

如果使用 Gradle：

```groovy
dependencies {
    implementation 'org.springframework.ai:spring-ai-starter-vector-store-opengauss'
}
```

`VectorStore` 需要 `EmbeddingModel`。以下示例使用 OpenAI embedding 模型：

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-model-openai</artifactId>
</dependency>
```

建议通过 Spring AI BOM 管理版本，避免在每个 Spring AI 模块上重复声明版本号。

### 3.2 最小配置

`application.yml` 示例：

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:15432/postgres
    username: gaussdb
    password: Huawei@123

  ai:
    openai:
      api-key: ${OPENAI_API_KEY}

    vectorstore:
      type: opengauss
      opengauss:
        initialize-schema: true
        index-type: HNSW
        distance-type: COSINE_DISTANCE
        dimensions: 1536
```

该配置会完成以下工作：

- 将 Spring AI 自动装配的 `VectorStore` 类型切换为 `opengauss`。
- 使用 openGauss 作为向量数据存储后端。
- 在启动时创建 schema、向量表和索引。
- 使用余弦距离执行相似度检索。

`dimensions` 应与 `EmbeddingModel` 输出维度一致。例如 OpenAI `text-embedding-ada-002` 和部分 embedding 模型输出维度为 `1536`。如果未显式配置，openGauss Vector Store 会尝试从 `EmbeddingModel` 获取维度，获取失败时回退到 `1536`。

### 3.3 常用配置项

| 配置项 | 说明 | 默认值 |
|---|---|---|
| `spring.ai.vectorstore.type` | 自动装配的向量库类型，使用 openGauss 时设置为 `opengauss`。 | `simple` |
| `spring.ai.vectorstore.opengauss.initialize-schema` | 是否自动创建 schema、表和索引。 | `false` |
| `spring.ai.vectorstore.opengauss.index-type` | 近邻检索索引类型，支持 `HNSW`、`IVFFLAT`、`NONE`。 | `HNSW` |
| `spring.ai.vectorstore.opengauss.distance-type` | 距离类型，支持 `COSINE_DISTANCE`、`EUCLIDEAN_DISTANCE`、`INNER_PRODUCT`。 | `COSINE_DISTANCE` |
| `spring.ai.vectorstore.opengauss.dimensions` | 向量维度。未指定时尝试从 `EmbeddingModel` 解析。 | `-1` |
| `spring.ai.vectorstore.opengauss.remove-existing-vector-store-table` | 启动时是否删除已有向量表。仅建议测试环境使用。 | `false` |
| `spring.ai.vectorstore.opengauss.schema-name` | 向量表所在 schema。 | `public` |
| `spring.ai.vectorstore.opengauss.table-name` | 向量表名。 | `vector_store` |
| `spring.ai.vectorstore.opengauss.id-type` | 文档 ID 列类型，支持 `UUID`、`STRING`。 | `UUID` |
| `spring.ai.vectorstore.opengauss.schema-validation` | 启动时是否校验表结构。 | `false` |
| `spring.ai.vectorstore.opengauss.max-document-batch-size` | 单批写入文档数量上限。 | `10000` |

生产环境建议：

- 显式配置 `schema-name` 和 `table-name`，避免多个应用共用默认表。
- 开启 `schema-validation`，在启动阶段暴露表结构不匹配问题。
- 不要开启 `remove-existing-vector-store-table`，除非是临时验证环境。
- 根据 openGauss DataVec 部署能力确认索引类型和向量维度上限。

### 3.4 自定义 schema 和表名

```yaml
spring:
  ai:
    vectorstore:
      type: opengauss
      opengauss:
        initialize-schema: true
        schema-name: ai
        table-name: kb_embeddings
        schema-validation: true
```

如果使用自定义 schema 或表名，建议由 Flyway、Liquibase 或数据库变更流程统一管理表结构；`initialize-schema=true` 更适合 demo、开发环境或首次验证。

### 3.5 手动建表

如果不希望应用启动时自动建表，可以将 `initialize-schema` 设置为 `false`，并提前创建表结构。

```sql
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.vector_store (
    id uuid PRIMARY KEY,
    content text,
    metadata json,
    embedding vector(1536)
);

CREATE INDEX IF NOT EXISTS spring_ai_opengauss_vector_index
    ON public.vector_store USING hnsw (embedding vector_cosine_ops);
```

将 `1536` 替换为当前 embedding 模型的实际输出维度。索引是否可创建取决于 openGauss DataVec 版本、索引类型和维度限制。

## 4. 使用 Vector Store

### 4.1 写入和检索文档

```java
import java.util.List;
import java.util.Map;

import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;

@Service
public class KnowledgeBaseService {

    private final VectorStore vectorStore;

    public KnowledgeBaseService(VectorStore vectorStore) {
        this.vectorStore = vectorStore;
    }

    public void load() {
        List<Document> documents = List.of(
                new Document("Spring AI supports openGauss vector search", Map.of("category", "spring")),
                new Document("openGauss can persist embeddings in DataVec", Map.of("category", "database"))
        );

        vectorStore.add(documents);
    }

    public List<Document> search(String query) {
        return vectorStore.similaritySearch(
                SearchRequest.builder()
                        .query(query)
                        .topK(5)
                        .build()
        );
    }
}
```

`vectorStore.add(documents)` 会调用已配置的 `EmbeddingModel` 生成向量，然后写入 openGauss。`similaritySearch` 会对查询文本生成向量，并根据配置的距离类型执行相似度检索。

### 4.2 使用 metadata 过滤

openGauss Vector Store 支持 Spring AI 通用 metadata filter 表达式。示例：

```java
List<Document> results = vectorStore.similaritySearch(
        SearchRequest.builder()
                .query("spring ai")
                .topK(5)
                .similarityThresholdAll()
                .filterExpression("category == 'spring' && year >= 2026")
                .build()
);
```

过滤表达式会被转换为 openGauss 可执行的 SQL 条件，并作用于 `metadata` JSON 列。

## 5. 手动配置 Vector Store

如果不使用 Spring Boot starter，可以手动声明依赖和 `VectorStore` Bean。

Maven 依赖：

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-jdbc</artifactId>
</dependency>

<dependency>
    <groupId>org.opengauss</groupId>
    <artifactId>opengauss-jdbc</artifactId>
    <scope>runtime</scope>
</dependency>

<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-opengauss-store</artifactId>
</dependency>
```

手动装配：

```java
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.opengauss.OpenGaussVectorStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class OpenGaussVectorStoreConfig {

    @Bean
    VectorStore vectorStore(JdbcTemplate jdbcTemplate, EmbeddingModel embeddingModel) {
        return OpenGaussVectorStore.builder(jdbcTemplate, embeddingModel)
                .dimensions(1536)
                .distanceType(OpenGaussVectorStore.OpenGaussDistanceType.COSINE_DISTANCE)
                .indexType(OpenGaussVectorStore.OpenGaussIndexType.HNSW)
                .initializeSchema(true)
                .schemaName("public")
                .vectorTableName("vector_store")
                .maxDocumentBatchSize(10000)
                .build();
    }
}
```

手动配置适合以下场景：

- 需要完全控制 Bean 创建过程。
- 不希望使用 Spring Boot 自动装配。
- 需要在同一应用中管理多个向量库实例。

## 6. 接入 JDBC Chat Memory

`JDBC Chat Memory` 用于将聊天消息持久化到关系型数据库。openGauss 接入走 Spring AI JDBC repository 路径，不依赖 DataVec，也不要求启用向量能力。

### 6.1 引入依赖

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-model-chat-memory-repository-jdbc</artifactId>
</dependency>
```

Gradle：

```groovy
dependencies {
    implementation 'org.springframework.ai:spring-ai-starter-model-chat-memory-repository-jdbc'
}
```

### 6.2 最小配置

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:15432/postgres
    username: gaussdb
    password: Huawei@123

  ai:
    chat:
      memory:
        repository:
          jdbc:
            initialize-schema: always
```

说明：

- `JdbcChatMemoryRepositoryDialect` 会根据 JDBC metadata 识别 openGauss，并使用 openGauss 方言。
- 启动初始化时会使用 `schema-opengauss.sql` 创建表结构。
- 如果项目已经使用 Flyway 或 Liquibase 管理数据库变更，建议设置为 `never`，由迁移脚本创建表。
- `JDBC Chat Memory` 和 openGauss Vector Store 可以共用同一个 `DataSource`。

### 6.3 常用配置项

| 配置项 | 说明 | 默认值 |
|---|---|---|
| `spring.ai.chat.memory.repository.jdbc.initialize-schema` | schema 初始化策略，支持 `embedded`、`always`、`never`。 | `embedded` |
| `spring.ai.chat.memory.repository.jdbc.schema` | 初始化脚本位置，支持 `classpath:` 和 `@@platform@@` 占位符。 | `classpath:org/springframework/ai/chat/memory/repository/jdbc/schema-@@platform@@.sql` |
| `spring.ai.chat.memory.repository.jdbc.platform` | 初始化脚本平台标识。通常由框架自动识别。 | 自动识别 |

### 6.4 配置 ChatMemory

```java
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.chat.memory.MessageWindowChatMemory;
import org.springframework.ai.chat.memory.repository.jdbc.JdbcChatMemoryRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ChatMemoryConfig {

    @Bean
    ChatMemory chatMemory(JdbcChatMemoryRepository repository) {
        return MessageWindowChatMemory.builder()
                .chatMemoryRepository(repository)
                .maxMessages(20)
                .build();
    }
}
```

`JdbcChatMemoryRepository` 负责消息存取，`MessageWindowChatMemory` 负责控制对话窗口保留策略。`maxMessages(20)` 表示每个会话最多保留最近 20 条消息用于上下文。

## 7. 获取并使用离线产物包

如果 openGauss 相关模块尚未发布到你使用的 Maven 仓库，可以先使用离线产物包完成接入验证。该产物当前只能从以下地址下载：

[opengauss-1.1.2-package.zip](https://gitee.com/kunpeng_compute/KunpengRAG/blob/master/spring-ai/dist/opengauss-1.1.2-package.zip)

下载并解压后，主要 jar 位于以下目录：

```text
opengauss-1.1.2-package/artifacts/
```

主要产物如下：

| 产物 | 用途 |
|---|---|
| `spring-ai-starter-vector-store-opengauss-1.1.2.jar` | Spring Boot 应用推荐入口，包含 openGauss Vector Store 所需依赖和自动配置。 |
| `spring-ai-autoconfigure-vector-store-opengauss-1.1.2.jar` | 自动配置模块。适合不使用 starter、但仍希望保留 Boot 自动装配能力的场景。 |
| `spring-ai-opengauss-store-1.1.2.jar` | 底层 openGauss `VectorStore` 实现。适合手动声明 `OpenGaussVectorStore` Bean 的场景。 |
| `spring-ai-model-chat-memory-repository-jdbc-1.1.2.jar` | JDBC Chat Memory Repository 模块，包含 openGauss 方言识别和 `schema-opengauss.sql`。 |

部分模块同时提供辅助产物：

- `*-sources.jar`：用于 IDE 跳转源码、调试和查看实现。
- `*-javadoc.jar`：用于查看 API 文档，不是运行时必需依赖。

### 7.1 推荐使用方式

业务项目推荐通过标准 Maven 依赖管理引用这些产物：

- 本地验证时，先从上述链接下载 zip，解压后安装到本地 Maven 仓库。
- 团队使用时，从上述链接获取产物后上传到公司内部 Maven 私服。
- 不建议长期将 jar 文件手工放入业务项目目录直接引用，否则版本管理、依赖传递和升级都会更难维护。

### 7.2 安装到本地 Maven 仓库

以下命令假设当前目录为解压后的 `opengauss-1.1.2-package` 目录。以 starter 为例：

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-starter-vector-store-opengauss-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-starter-vector-store-opengauss -Dversion=1.1.2 -Dpackaging=jar
```

安装底层 store 模块：

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-opengauss-store-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-opengauss-store -Dversion=1.1.2 -Dpackaging=jar
```

安装自动配置模块：

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-autoconfigure-vector-store-opengauss-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-autoconfigure-vector-store-opengauss -Dversion=1.1.2 -Dpackaging=jar
```

安装 JDBC Chat Memory Repository：

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-model-chat-memory-repository-jdbc-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-model-chat-memory-repository-jdbc -Dversion=1.1.2 -Dpackaging=jar
```

安装完成后，业务项目可以按普通 Maven 坐标引用这些模块。

### 7.3 业务项目依赖示例

openGauss Vector Store：

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-vector-store-opengauss</artifactId>
    <version>1.1.2</version>
</dependency>
```

JDBC Chat Memory：

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-model-chat-memory-repository-jdbc</artifactId>
    <version>1.1.2</version>
</dependency>
```

手动配置 `OpenGaussVectorStore` 时至少需要：

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-opengauss-store</artifactId>
    <version>1.1.2</version>
</dependency>
```

如果项目已经引入 Spring AI BOM，通常不需要在单个依赖上重复声明 `<version>`。

## 8. 产物选择建议

| 场景 | 推荐产物 |
|---|---|
| 标准 Spring Boot 应用接入 openGauss 向量检索 | `spring-ai-starter-vector-store-opengauss` |
| 需要手动创建 `OpenGaussVectorStore` Bean | `spring-ai-opengauss-store` |
| 不使用 starter，但需要 Boot 自动配置 | `spring-ai-autoconfigure-vector-store-opengauss` |
| 需要将聊天消息持久化到 openGauss | `spring-ai-model-chat-memory-repository-jdbc` |

大多数业务项目使用 `spring-ai-starter-vector-store-opengauss` 加 `spring-ai-model-chat-memory-repository-jdbc` 即可。只有在需要更细粒度控制 Bean 创建、依赖拆分或多实例配置时，才建议单独使用 `store` 或 `autoconfigure` 模块。

## 9. 常见问题

### 9.1 为什么 JDBC URL 使用 `jdbc:postgresql://...`？

当前验证路径使用 openGauss JDBC 驱动配合 `jdbc:postgresql://...` 形式的连接串。该形式与已验证的 openGauss 环境兼容。若你的部署要求其他 URL 形式，应以实际驱动和数据库版本验证结果为准。

### 9.2 是否支持全文检索或混合检索？

openGauss 数据库本身可以提供全文检索能力，但当前 Spring AI openGauss 接入没有额外提供独立的全文检索或混合检索抽象。业务如需混合召回，可以在应用层组合 SQL 全文检索、向量检索和结果重排。

### 9.3 是否必须自动建表？

不是。`initialize-schema` 默认关闭。生产环境通常建议关闭自动建表，通过数据库迁移工具维护 schema；开发和快速验证环境可以开启自动建表。

### 9.4 向量维度应该如何设置？

向量维度必须与 `EmbeddingModel` 输出维度一致。维度不一致会导致写入或检索失败。建议在配置中显式声明 `spring.ai.vectorstore.opengauss.dimensions`，并在切换 embedding 模型时同步调整表结构。

### 9.5 `id-type` 应该选 `UUID` 还是 `STRING`？

如果文档 ID 是标准 UUID，使用默认值 `UUID`。如果业务已有非 UUID 格式的文档 ID，应配置为 `STRING`，并确保表结构中的 `id` 列与之匹配。

### 9.6 JDBC Chat Memory 是否依赖 DataVec？

不依赖。JDBC Chat Memory 只使用关系表存储聊天消息，不需要向量类型，也不需要 DataVec。
