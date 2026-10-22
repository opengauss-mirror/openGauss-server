# Guide to Integrating openGauss with Spring AI

This guide describes how to integrate openGauss into a Spring AI application for vector search and persistent chat memory. It is intended for practical integration into business applications and covers dependency setup, data source configuration, `VectorStore` usage, `JDBC Chat Memory` usage, and installation of locally built artifacts.

The vector capabilities of openGauss are provided by DataVec. Spring AI's openGauss integration is an independent `VectorStore` implementation that uses JDBC to access the openGauss `vector` type. It is not an alias for, or a simple reuse of, the `pgvector` integration.

## 1. Scope

The current integration supports the following:

- `Vector Store`: writes document embeddings to openGauss DataVec and performs Top-K similarity search.
- `Metadata Filter`: uses Spring AI's generic metadata filter expressions during similarity search.
- `JDBC Chat Memory`: persists chat messages to openGauss for use with `ChatMemory`.
- Basic schema initialization: automatically creates vector tables, indexes, and Chat Memory table structures based on the configuration.

The current integration does not include the following:

- A dedicated Spring AI abstraction for full-text search in openGauss.
- A unified API for hybrid full-text and vector retrieval.
- Spring AI wrappers for advanced DataVec types such as `halfvec` and `sparsevec`.

If full-text search or hybrid retrieval is required, you can combine openGauss SQL, application-level retrieval logic, and Spring AI `VectorStore` search results at the application layer.

## 2. Prerequisites

Before integration, prepare the following:

- An accessible openGauss instance.
- An openGauss database environment with DataVec enabled.
- Available JDBC connection information.
- A Spring AI `EmbeddingModel` for generating document embeddings.
- If `JDBC Chat Memory` is used, an available `DataSource` in the project.

The following JDBC URL format has been verified:

```properties
spring.datasource.url=jdbc:postgresql://localhost:15432/postgres
spring.datasource.username=gaussdb
spring.datasource.password=Huawei@123
```

Notes:

- The openGauss Vector Store depends on the openGauss JDBC driver.
- The currently verified setup uses the `jdbc:postgresql://...` connection URL format.
- If your environment supports other openGauss JDBC URL formats, verify them according to your project requirements.

## 3. Integrating with Vector Store

### 3.1 Adding Dependencies

For Spring Boot applications, using the starter is recommended:

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-vector-store-opengauss</artifactId>
</dependency>
```

For Gradle:

```groovy
dependencies {
    implementation 'org.springframework.ai:spring-ai-starter-vector-store-opengauss'
}
```

`VectorStore` requires an `EmbeddingModel`. The following example uses an OpenAI embedding model:

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-model-openai</artifactId>
</dependency>
```

You are advised to use the Spring AI BOM to manage versions and avoid declaring a version number for each Spring AI module.

### 3.2 Minimal Configuration

Example `application.yml`:

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

This configuration performs the following operations:

- Sets the auto-configured `VectorStore` type to `opengauss`.
- Uses openGauss as the vector data storage backend.
- Creates the schema, vector table, and index at startup.
- Uses cosine distance for similarity search.

`dimensions` must match the output dimension of the `EmbeddingModel`. For example, OpenAI's `text-embedding-ada-002` and some other embedding models output vectors with 1536 dimensions. If `dimensions` is not explicitly configured, the openGauss Vector Store attempts to obtain the dimension from the `EmbeddingModel` and falls back to `1536` if the dimension cannot be determined.

### 3.3 Common Configuration Options

| Configuration Item | Description | Default |
| ------------------ | ----------- | ------- |
| `spring.ai.vectorstore.type` | Vector store type to be auto-configured. | `simple` |
| `spring.ai.vectorstore.opengauss.initialize-schema` | Specifies whether to automatically create the schema, tables, and indexes. | `false` |
| `spring.ai.vectorstore.opengauss.index-type` | Index type for nearest-neighbor search. Supported values are `HNSW`, `IVFFLAT`, and `NONE`. | `HNSW` |
| `spring.ai.vectorstore.opengauss.distance-type` | Distance type. Supported values are `COSINE_DISTANCE`, `EUCLIDEAN_DISTANCE`, and `INNER_PRODUCT`. | `COSINE_DISTANCE` |
| `spring.ai.vectorstore.opengauss.dimensions` | Vector dimension. If not specified, the dimension is obtained from the `EmbeddingModel` when possible. | `-1` |
| `spring.ai.vectorstore.opengauss.remove-existing-vector-store-table` | Specifies whether to delete the existing vector table at startup. Recommended only for testing environments. | `false` |
| `spring.ai.vectorstore.opengauss.schema-name` | Schema containing the vector table. | `public` |
| `spring.ai.vectorstore.opengauss.table-name` | Name of the vector table. | `vector_store` |
| `spring.ai.vectorstore.opengauss.id-type` | Data type of the document ID column. Supported values are `UUID` and `STRING`. | `UUID` |
| `spring.ai.vectorstore.opengauss.schema-validation` | Specifies whether to validate the table schema at startup. | `false` |
| `spring.ai.vectorstore.opengauss.max-document-batch-size` | Maximum number of documents written in a single batch. | `10000` |

For production environments:

- Explicitly configure `schema-name` and `table-name` to avoid multiple applications sharing the default table.
- Enable `schema-validation` to detect table schema mismatches during startup.
- Do not enable `remove-existing-vector-store-table` unless you are using a temporary testing environment.
- Check the supported index types and vector dimension limits for your openGauss DataVec deployment.

### 3.4 Custom Schema and Table Names

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

If you use a custom schema or table name, you are advised to manage the table schema through Flyway, Liquibase, or your database change management process. `initialize-schema=true` is more suitable for demos, development environments, or initial validation.

### 3.5 Creating the Table Manually

If you do not want the application to create the table at startup, set `initialize-schema` to `false` and create the table structure in advance.

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

Replace `1536` with the actual output dimension of the embedding model. Whether the index can be created depends on the openGauss DataVec version, index type, and dimension limits.

## 4. Using Vector Store

### 4.1 Adding and Searching Documents

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

`vectorStore.add(documents)` calls the configured `EmbeddingModel` to generate embeddings and then writes them to openGauss. `similaritySearch` generates an embedding for the query text and performs similarity search using the configured distance type.

### 4.2 Using Metadata Filters

The openGauss Vector Store supports Spring AI's generic metadata filter expressions. Example:

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

The filter expression is converted into an SQL condition that can be executed by openGauss and applied to the `metadata` JSON column.

## 5. Manually Configuring Vector Store

If you do not use the Spring Boot starter, you can manually declare the dependencies and a `VectorStore` bean.

Maven dependencies:

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

Manual configuration:

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

Manual configuration is suitable for the following scenarios:

- You need full control over bean creation.
- You do not want to use Spring Boot auto-configuration.
- You need to manage multiple vector store instances in the same application.

## 6. Integrating JDBC Chat Memory

`JDBC Chat Memory` persists chat messages in a relational database. The openGauss integration uses the Spring AI JDBC repository path and does not depend on DataVec or require vector capabilities to be enabled.

### 6.1 Adding Dependencies

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-model-chat-memory-repository-jdbc</artifactId>
</dependency>
```

For Gradle:

```groovy
dependencies {
    implementation 'org.springframework.ai:spring-ai-starter-model-chat-memory-repository-jdbc'
}
```

### 6.2 Minimal Configuration

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

Notes:

- `JdbcChatMemoryRepositoryDialect` identifies openGauss based on JDBC metadata and uses the openGauss dialect.
- During startup initialization, `schema-opengauss.sql` is used to create the table structure.
- If your project already uses Flyway or Liquibase to manage database changes, you are advised to set this property to `never` and create the tables through migration scripts.
- `JDBC Chat Memory` and the openGauss Vector Store can share the same `DataSource`.

### 6.3 Common Configuration Options

| Configuration Item | Description | Default |
| ------------------ | ----------- | ------- |
| `spring.ai.chat.memory.repository.jdbc.initialize-schema` | Schema initialization strategy. Supported values are `embedded`, `always`, and `never`. | `embedded` |
| `spring.ai.chat.memory.repository.jdbc.schema` | Location of the initialization script. Supports `classpath:` and the `@@platform@@` placeholder. | `classpath:org/springframework/ai/chat/memory/repository/jdbc/schema-@@platform@@.sql` |
| `spring.ai.chat.memory.repository.jdbc.platform` | Platform identifier for the initialization script. Normally detected automatically by the framework. | Automatically detected |

### 6.4 Configuring ChatMemory

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

`JdbcChatMemoryRepository` stores and retrieves messages, while `MessageWindowChatMemory` controls the conversation window retention strategy. `maxMessages(20)` means that each conversation retains the 20 most recent messages for context.

## 7. Obtaining and Using Offline Artifacts

If the openGauss-related modules have not yet been published to the Maven repository you use, you can use the offline artifact package to validate the integration. The package is currently available only from the following location:

[opengauss-1.1.2-package.zip](https://gitee.com/kunpeng_compute/KunpengRAG/blob/master/spring-ai/dist/opengauss-1.1.2-package.zip)

After downloading and extracting the package, the main JAR files are located in the following directory:

```text
opengauss-1.1.2-package/artifacts/
```

The main artifacts are as follows:

| Artifact | Purpose |
| -------- | ------- |
| `spring-ai-starter-vector-store-opengauss-1.1.2.jar` | Recommended entry point for Spring Boot applications. Includes the dependencies and auto-configuration required by the openGauss Vector Store. |
| `spring-ai-autoconfigure-vector-store-opengauss-1.1.2.jar` | Auto-configuration module. Suitable for scenarios where the starter is not used but Spring Boot auto-configuration is still required. |
| `spring-ai-opengauss-store-1.1.2.jar` | Underlying openGauss `VectorStore` implementation. Suitable for manually declaring an `OpenGaussVectorStore` bean. |
| `spring-ai-model-chat-memory-repository-jdbc-1.1.2.jar` | JDBC Chat Memory Repository module. Includes openGauss dialect detection and `schema-opengauss.sql`. |

Some modules also provide auxiliary artifacts:

- `*-sources.jar`: used by IDEs for source navigation, debugging, and viewing the implementation.
- `*-javadoc.jar`: used for viewing API documentation and is not required at runtime.

### 7.1 Recommended Usage

For business applications, you are advised to reference these artifacts through standard Maven dependency management:

- For local validation, download the ZIP file from the link above, extract it, and install the artifacts into the local Maven repository.
- For team use, obtain the artifacts from the link above and upload them to the company's internal Maven repository.
- Avoid keeping JAR files directly in the business application's directory and referencing them manually over the long term, as this makes version management, transitive dependencies, and upgrades more difficult to maintain.

### 7.2 Installing Artifacts to the Local Maven Repository

The following commands assume that the current directory is the extracted `opengauss-1.1.2-package` directory. The following example installs the starter:

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-starter-vector-store-opengauss-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-starter-vector-store-opengauss -Dversion=1.1.2 -Dpackaging=jar
```

Install the underlying store module:

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-opengauss-store-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-opengauss-store -Dversion=1.1.2 -Dpackaging=jar
```

Install the auto-configuration module:

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-autoconfigure-vector-store-opengauss-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-autoconfigure-vector-store-opengauss -Dversion=1.1.2 -Dpackaging=jar
```

Install the JDBC Chat Memory Repository:

```bash
mvn install:install-file -Dfile=artifacts/spring-ai-model-chat-memory-repository-jdbc-1.1.2.jar -DgroupId=org.springframework.ai -DartifactId=spring-ai-model-chat-memory-repository-jdbc -Dversion=1.1.2 -Dpackaging=jar
```

After installation, business applications can reference these modules using their standard Maven coordinates.

### 7.3 Example Dependencies for Business Applications

For the openGauss Vector Store:

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-starter-vector-store-opengauss</artifactId>
    <version>1.1.2</version>
</dependency>
```

For JDBC Chat Memory:

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-model-chat-memory-repository-jdbc</artifactId>
    <version>1.1.2</version>
</dependency>
```

When manually configuring `OpenGaussVectorStore`, the minimum required dependency is:

```xml
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-opengauss-store</artifactId>
    <version>1.1.2</version>
</dependency>
```

If the project already uses the Spring AI BOM, you generally do not need to declare `<version>` for individual dependencies.

## 8. Artifact Selection Recommendations

| Scenario | Recommended Artifact |
| -------- | -------------------- |
| Integrating openGauss vector search into a standard Spring Boot application | `spring-ai-starter-vector-store-opengauss` |
| Manually creating an `OpenGaussVectorStore` bean | `spring-ai-opengauss-store` |
| Not using the starter but requiring Spring Boot auto-configuration | `spring-ai-autoconfigure-vector-store-opengauss` |
| Persisting chat messages to openGauss | `spring-ai-model-chat-memory-repository-jdbc` |

For most business applications, `spring-ai-starter-vector-store-opengauss` and `spring-ai-model-chat-memory-repository-jdbc` are sufficient. Use the `store` or `autoconfigure` modules separately only when you need finer-grained control over bean creation, dependency separation, or multiple instances.

## 9. FAQ

### 9.1 Why Does the JDBC URL Use `jdbc:postgresql://...`?

The currently verified setup uses the openGauss JDBC driver with a `jdbc:postgresql://...` connection URL. This format is compatible with the verified openGauss environment. If your deployment requires another URL format, follow the verification results for the actual driver and database version.

### 9.2 Does It Support Full-Text Search or Hybrid Retrieval?

The openGauss database itself provides full-text search capabilities. However, the current Spring AI openGauss integration does not provide a separate abstraction for full-text search or hybrid retrieval. If hybrid retrieval is required, you can combine SQL-based full-text search, vector search, and result reranking at the application layer.

### 9.3 Is Automatic Table Creation Required?

No. `initialize-schema` is disabled by default. In production environments, we generally recommend disabling automatic table creation and managing the schema through a database migration tool. Automatic table creation can be enabled for development and rapid validation.

### 9.4 How Should the Vector Dimension Be Set?

The vector dimension must match the output dimension of the `EmbeddingModel`. A dimension mismatch causes vector insertion or search to fail. You are advised to explicitly set `spring.ai.vectorstore.opengauss.dimensions` in the configuration and update the table schema accordingly when changing the embedding model.

### 9.5 Which Value Should `id-type` Use: `UUID` or `STRING`?

If the document IDs are standard UUIDs, use the default value `UUID`. If the application already uses non-UUID document IDs, set this property to `STRING` and ensure that the `id` column in the table has the corresponding data type.

### 9.6 Does JDBC Chat Memory Depend on DataVec?

No. JDBC Chat Memory stores chat messages in relational tables. It does not require the vector type or DataVec.
