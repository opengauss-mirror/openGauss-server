# openGauss-SentenceTransformer Practice Guide

To use this feature, first install `torch`, `torchvision`, and `torchaudio`. You can install these packages using `pip` or `conda`.

The Sentence Transformers framework uses models such as BERT to generate embeddings for sentences, paragraphs, and images. It provides a simple way to compute dense vector representations of sentences, paragraphs, and images. These models are based on Transformer networks such as BERT, RoBERTa, and XLM-RoBERTa and achieve state-of-the-art performance on various tasks.

## 1. Installing `sentence-transformers`, `torch`, `torchvision`, and `torchaudio`

### 1.1 Using the Tsinghua University PyPI Mirror (Recommended for Users in China)

```bash
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple sentence-transformers

pip install torch

pip install torchvision

pip install torchaudio

pip install psycopg2
```

If installation fails due to version incompatibility, force a package reinstallation by running `pip install --force-reinstall`.  
If the models installed using `pip` are incompatible and the issue cannot be resolved, uninstall all models installed using `pip` and reinstall them using `conda`.

### 1.2 Importing the Sentence Transformers Package

```
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('model_name')
```

### 1.3 Generating Sentence Embeddings Using a Pretrained Sentence Transformers Model

The default Sentence Transformers model used for encoding is `all-MiniLM-L6-v2`. You can use any pretrained Sentence Transformers model.

```
from sentence_transformers import SentenceTransformer
model = SentenceTransformer('all-MiniLM-L6-v2')

sentences = ['Artificial intelligence was founded as an academic discipline in 1956.',
    'Alan Turing was the first person to conduct substantial research in AI.',
    'Born in Maida Vale, London, Turing was raised in southern England.']
sentence_embeddings = model.encode(sentences)

for sentence, embedding in zip(sentences, sentence_embeddings):
    print("Sentence:", sentence)
    print("Embedding:", embedding)
    print("")
```

The output is as follows:

```
Embeddings: [array([-3.09392996e-02, -1.80662833e-02,  1.34775648e-02,  2.77156215e-02,
       -4.86349640e-03, -3.12581174e-02, -3.55921760e-02,  5.76934684e-03,
        2.80773244e-03,  1.35783911e-01,  3.59678417e-02,  6.17732145e-02,
...
       -4.61330153e-02, -4.85207550e-02,  3.13997865e-02,  7.82178566e-02,
       -4.75336798e-02,  5.21207601e-02,  9.04406682e-02, -5.36676683e-02],
      dtype=float32)]
Dim: 384 (384,)
```

To generate embeddings for queries, use the `encode_queries()` method.

```
queries = ["When was artificial intelligence founded", 
           "Where was Alan Turing born?"]

query_embeddings = model.encode_queries(queries)

print("Embeddings:", query_embeddings)
print("Dim:", model.dim, query_embeddings[0].shape)
```

The output is as follows:

```
Embeddings: [array([-2.52114702e-02, -5.29330298e-02,  1.14570223e-02,  1.95571519e-02,
       -2.46500354e-02, -2.66519729e-02, -8.48201662e-03,  2.82961670e-02,
       -3.65092754e-02,  7.50745758e-02,  4.28900979e-02,  7.18822703e-02,
...
       -6.76431581e-02, -6.45996556e-02, -4.67132553e-02,  4.78532910e-02,
       -2.31596199e-03,  4.13446948e-02,  1.06935494e-01, -1.08258888e-01],
      dtype=float32)]
Dim: 384 (384,)
```

## Corpus Embedding

Install the openGauss database by following the instructions in [Installing the Database](https://docs.opengauss.org/en/docs/latest/installation_guide/installing_the_container_image.html).

After openGauss is successfully installed and deployed, use `psycopg2` to connect to openGauss and view the version information.

```
import psycopg2

conn = psycopg2.connect(
    database="postgres",
    user="gauss",
    password="******",
    host="127.0.0.1",
    port="8888"
)

cur = conn.cursor()
cur.execute("select version();")
rows = cur.fetchall()
print(rows)
```

```
[('(openGauss 7.0.0-RC2 build 3adb1fec) compiled at 2025-04-27 15:23:26 commit 0 last mr   on aarch64-unknown-linux-gnu, compiled by g++ (GCC) 10.3.1, 64-bit',)]
```

## Converting Text to Vector Embeddings Using the Installed Model

```python
# Create the table if it does not exist
create_table_sql = """
CREATE TABLE IF NOT EXISTS sentence_embeddings (
    id SERIAL PRIMARY KEY,
    sentence TEXT NOT NULL,
    embedding vector(384),
    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
cursor.execute(create_table_sql)
conn.commit()
```

```python
# Insert data
for sentence, embedding in zip(sentences, embeddings):
    # Convert the NumPy array to a list
    embedding_list = embedding.tolist()
    
    insert_sql = """
    INSERT INTO sentence_embeddings (sentence, embedding)
    VALUES (%s, %s)
    """
    cursor.execute(insert_sql, (sentence, embedding_list))
```

## Query and Retrieval

1. Model initialization: use the `all-MiniLM-L6-v2` model to convert text into 384-dimensional vectors.

2. Query processing: encode the queries as vectors.

3. Similarity search: use `1 - L2 distance` to approximate cosine similarity and return the most similar results.

Use the following queries:

```
Who is the father of AI?
Where did Turing grow up?
```

Search for the queries in openGauss to retrieve relevant information previously imported into the database.

```python
queries = [
    "Who is the father of AI?",
    "Where did Turing grow up?"
]

# Convert the queries to vectors
query_embeddings = model.encode(queries)  # shape: (n_queries, 384)

# Perform semantic search (cosine similarity)
results = []
for query, query_embedding in zip(queries, query_embeddings):
    # Convert the NumPy array to a list
    query_embedding_list = query_embedding.tolist()
    
    # Perform similarity search. Calculate the L2 distance between the query vector and the vectors stored in the database using embedding <=> %s, and approximate cosine similarity as 1 - distance.
    search_sql = """
    SELECT id, sentence, 1 - (embedding <=> %s) AS cosine_similarity
    FROM sentence_embeddings
    ORDER BY cosine_similarity DESC
    LIMIT 3
    """
    cursor.execute(search_sql, (query_embedding_list,))
    
    # Retrieve the results
    query_results = cursor.fetchall()
    results.append((query, query_results))
```

The output is as follows:

```
Query: 'Who is the father of AI?'
  1. [Similarity: 0.7821] Alan Turing was the first person to conduct substantial research in AI.
  2. [Similarity: 0.4325] Artificial intelligence was founded as an academic discipline in 1956.

Query: 'Where did Turing grow up?'
  1. [Similarity: 0.8532] Born in Maida Vale, London, Turing was raised in southern England.
  2. [Similarity: 0.3214] Alan Turing was the first person to conduct substantial research in AI.
```
