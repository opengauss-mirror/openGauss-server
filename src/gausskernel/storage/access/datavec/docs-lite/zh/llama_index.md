# 使用openGauss部署LlamaIndex

LlamaIndex（原GPT Index）是一个专为大型语言模型（LLM）应用设计的数据框架。使用LlamaIndex进行开发时，通常需要结合其核心功能与选定的集成插件（plugins）。

## openGauss容器化部署

详见[容器镜像安装](https://docs.opengauss.org/zh/docs/latest-lite/installation_guide/installing_the_container_image.html)。

## LlamaIndex部署

### 安装依赖

从源码安装依赖：

```bash
pip install llama-index-vector-stores-opengauss
```

### 撰写代码

LlamaIndex可以自定义流程，以下为openGauss配合ollama构成RAG应用的demo：
注意，需要在同目录下新建data文件夹，并在文件夹内存放文档

安装所需依赖：

```bash
pip install llama-index-llms-ollama
pip install llama-index-embeddings-ollama
```

```python
import os
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader, StorageContext
from llama_index.core import Settings
from llama_index.llms.ollama import Ollama
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.vector_stores.openGauss import OpenGaussStore

def build_rag_app():
    vector_store = OpenGaussStore.from_params(
        database="postgres",
        host="127.0.0.1",
        password="xxxxxx",
        port=8888,
        user="postgres",
        table_name="paul_graham_essay",
        embed_dim=768  # openai embedding dimension
    )

    # 2. 创建存储上下文
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    # 3. 加载文档
    documents = SimpleDirectoryReader("data").load_data()

    # 4. 创建索引
    index = VectorStoreIndex.from_documents(
        documents,
        storage_context=storage_context,
        show_progress=True
    )

    # 5. 创建查询引擎
    query_engine = index.as_query_engine(
        similarity_top_k=3,
        vector_store_kwargs={
            "hybrid_search": True,
            "text_search_config": "english"
        }
    )
    return query_engine

if __name__ == "__main__":
    # 配置本地模型
    Settings.llm = Ollama(
        model="llama3:latest",
        base_url="http://127.0.0.1:11434",
        request_timeout=300.0,
        
        temperature=0.3,
        top_p=0.9,
        top_k=40,
        
        num_ctx=2048,
        num_gpu=1,
        num_thread=8,
        
        keep_alive="5m",
        tfs_z=1.0,
        
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Connection": "keep-alive"
        },
        
        max_retries=3,
        retry_delay=5,
        
        format="json",
        stream=False
    )
    
    Settings.embed_model = OllamaEmbedding(
        model_name="nomic-embed-text:latest",
        base_url="http://127.0.0.1:11434",
        ollama_additional_kwargs={
            "embedding_only": True,
            "options": {
                "num_ctx": 2048,
                "num_gpu": 1,
                "use_mlock": True
            }
        },
        request_timeout=300,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    )

    # 初始化RAG应用
    rag_engine = build_rag_app()
    
    # 示例查询
    query = "作者早餐吃了什么？"
    response = rag_engine.query(query)
    
    print(f"问题：{query}")
    print(f"回答：{str(response).strip()}")
```
