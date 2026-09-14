# Deploying LlamaIndex with openGauss

LlamaIndex (formerly GPT Index) is a data framework designed specifically for large language model (LLM) applications. When developing with LlamaIndex, you typically need to combine its core functionality with selected integration plugins.

## Containerized Deployment of openGauss

For details, see [Container Image Installation](https://docs.opengauss.org/en/docs/latest/installation_guide/installation_overview.html).

## LlamaIndex Deployment

### Install Dependencies

Install dependencies from source:

```bash
pip install llama-index-vector-stores-opengauss
```

### Write the Code

LlamaIndex supports custom workflows. The following is a demo of building a RAG application with openGauss and ollama:
Note: Create a data folder in the same directory and place documents in the folder.

Install the required dependencies:

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

    # 2. Create the storage context
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    # 3. Load documents
    documents = SimpleDirectoryReader("data").load_data()

    # 4. Create the index
    index = VectorStoreIndex.from_documents(
        documents,
        storage_context=storage_context,
        show_progress=True
    )

    # 5. Create the query engine
    query_engine = index.as_query_engine(
        similarity_top_k=3,
        vector_store_kwargs={
            "hybrid_search": True,
            "text_search_config": "english"
        }
    )
    return query_engine

if __name__ == "__main__":
    # Configure the local model
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

    # Initialize the RAG application
    rag_engine = build_rag_app()
    
    # Example query
    query = "What did the author have for breakfast?"
    response = rag_engine.query(query)
    
    print(f"Question: {query}")
    print(f"Answer: {str(response).strip()}")
```
