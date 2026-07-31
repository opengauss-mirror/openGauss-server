# openGauss-SentenceTransformer实践

要使用该功能首先安装 torch、torchvision、torchaudio，可通过pip方式安装、conda方式安装

它使用BERT等模型进行多语句、段落和图像嵌入。该框架提供了一种简单的方法来计算句子、段落和图像的稠密向量表示。这些模型基于Transformers网络，如BERT / RoBERTa / XLM-RoBERTa等，在各种任务中取得了最先进的性能。

## 1、安装**sentence-transformers和torch、torchvision、torchaudio**

1.1、 国内推荐使用清华源

```
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple sentence-transformers

pip install torch

pip install torchvision

pip install orchaudio

pip install psycopg2
```

如遇到版本不匹配升级失败进行版本强制升级pip install --force;
如果遇到pip装的模型版本不兼容问题且无法解决时，建议卸载所有pip装的模型，重新用conda进行安装;

1.2、引入句子转换包

```
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('model_name')
```

1.3、使用已经训练好的sentence transformer模型来为另一个任务嵌入句子

用于编码的 Sentence Transformer 模型名称默认值为**all-MiniLM-L6-v2**。此处可以使用任何一个 Sentence Transformers 的预训练模型。

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

输出以下图示：

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

为查询创建Embeddings，使用**encode_queries()**方法

```
queries = ["When was artificial intelligence founded", 
           "Where was Alan Turing born?"]

query_embeddings = model.encode_queries(queries)

print("Embeddings:", query_embeddings)
print("Dim:", model.dim, query_embeddings[0].shape)
```

输出以下内容：

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

## 语料嵌入

通过openGauss官网文档安装数据库：<https://docs.opengauss.org/zh/docs/latest-lite/docs/installation_guide/%E5%AE%B9%E5%99%A8%E9%95%9C%E5%83%8F%E5%AE%89%E8%A3%85.html>

openGauss已经成功安装部署，我们可以通过psycopg2连接openGauss查看版本信息：

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

## 通过已安装好的模型进行文本信息转换向量

```
# 创建表（如果不存在）
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

```
#  插入数据
for sentence, embedding in zip(sentences, embeddings):
    # 将numpy数组转换为列表
    embedding_list = embedding.tolist()
    
    insert_sql = """
    INSERT INTO sentence_embeddings (sentence, embedding)
    VALUES (%s, %s)
    """
    cursor.execute(insert_sql, (sentence, embedding_list))
```

## 查询检索

1、模型初始化：使用all-MiniLM-L6-v2模型将文字转换为384维向量

2、查询处理将文本编码为向量

3、通过1 - L2距离近似余弦相似度，返回最相似的结果

我们尝试询问如下问题：

```
Who is the father of AI?
Where did Turing grow up?
```

在openGauss中检索该问题以获取之前导入数据库中的相关信息：

```
queries = [
    "Who is the father of AI?",
    "Where did Turing grow up?"
    ]

#  将查询转换为向量
query_embeddings = model.encode(queries)  # shape: (n_queries, 384)

#  执行语义搜索（余弦相似度）
results = []
for query, query_embedding in zip(queries, query_embeddings):
    # 将numpy数组转为列表
    query_embedding_list = query_embedding.tolist()
    
    # 执行相似度查询（通过embedding <=> %s计算要查询向量与库中向量的L2距离，通过1 - (距离)近似得到余弦相似度）
    search_sql = """
    SELECT id, sentence, 1 - (embedding <=> %s) AS cosine_similarity
    FROM sentence_embeddings
    ORDER BY cosine_similarity DESC
    LIMIT 3
    """
    cursor.execute(search_sql, (query_embedding_list,))
    
    # 获取结果
    query_results = cursor.fetchall()
    results.append((query, query_results))
```

输出

```
查询: 'Who is the father of AI?'
  1. [相似度: 0.7821] Alan Turing was the first person to conduct substantial research in AI.
  2. [相似度: 0.4325] Artificial intelligence was founded as an academic discipline in 1956.

查询: 'Where did Turing grow up?'
  1. [相似度: 0.8532] Born in Maida Vale, London, Turing was raised in southern England.
  2. [相似度: 0.3214] Alan Turing was the first person to conduct substantial research in AI.
```
