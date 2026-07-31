# 打破AI黑盒，拥抱开源力量：基于openGauss+DeepSeek的本地知识库，打造你的专属AI助手

## 引言：什么是RAG和LLM？

LLM (Large Language Model，大语言模型): 就像 ChatGPT 这样的 AI 模型，拥有强大的语言理解和生成能力，但它们的知识局限于训练数据，且可能产生“幻觉”（即生成不准确的信息）。

RAG (Retrieval-Augmented Generation，检索增强生成): 一种将信息检索与 LLM 结合的框架，可以实时从外部知识库中检索相关信息，并利用 LLM 生成更准确、可靠的回答。

当前，以 ChatGPT 为代表的外部 AI 助手暴露出诸多问题，回答内容的准确性存疑，数据安全存在隐患，访问过程中也时常出现不稳定的状况。

**这篇教程，将手把手教你基于openGauss开源数据库和DeepSeek，在本地部署一个专属的AI知识库，拥有一个更懂你、更安全、更可控的AI助手！**

## 为什么选择openGauss + DeepSeek？

- 开源开放，透明可信： 告别AI“黑盒”，代码完全开放，数据随心掌控，安全可靠。

- 性能强劲，稳定高效： openGauss作为国产数据库佼佼者，性能优异且更加安全稳定，DeepSeek的RAG模型更是能精准理解你的需求。

- 灵活定制，无限可能： 本地部署意味着你可以根据自身需求，自由定制知识库内容和功能，打造更懂你的AI助手。

## 为什么要本地部署？

- 数据安全，隐私保障： 敏感数据不出本地，杜绝信息泄露风险，安心使用无顾虑。
- 离线可用，稳定可靠： 无需依赖网络，随时随地访问你的知识库，稳定流畅不卡顿。
- 成本可控，长期受益： 一次部署，长期使用，无需持续支付高昂的云服务费用。

## 实践准备：搭建基础环境

### 操作系统与 Python 环境配置

本文实践所采用的操作系统为 **openEuler 22.03 LTS(鲲鹏arm服务器)**。为了确保各个组件之间能够实现无缝兼容并顺利运行，我们选择使用 **python3.11**。

| CPU | Memory | Disk | OS |
| :--- | :---: | ---: | ---: |
| Kunpeng-920 | 32GB * 16 DDR4 | 3.2T * 1 NVMe | openEuler 22.03 LTS |

## DeepSeek推理模型部署：解锁强大文本生成能力

### 安装Ollama服务

首先，我们需要下载 ollama 的 pypi 包，以便后续访问 ollama 服务：

```shell
[test@localhost ~]$ pip3 install ollama
```

然后，我们可以使用官网提供的一键式部署脚本来部署 ollama 服务：

```shell
curl -fsSL https://ollama.com/install.sh | sh
```

如果遇到网络问题无法直接安装，我们可以考虑采用手动方案：

```shell
[test@localhost ~]$ wget https://ollama.com/download/ollama-linux-amd64.tgz
[test@localhost ~]$ tar -zxvf ollama-linux-amd64.tgz -C /usr/
[test@localhost ~]$ which ollama
/usr/bin/ollama
```

注：arm 架构的下载地址为 <https://ollama.com/download/ollama-linux-arm64.tgz>

安装成功后，我们可以启动 ollama 服务：

```shell
[test@localhost ~]$ ollama serve &
```

### 选择关键模型：DeepSeek与nomic-embed-text的协同

在RAG应用中，文本嵌入模型和文本生成模型是至关重要的组成部分。在本次实践中，我们着重引入了DeepSeek家族中的**deepseek-r1**模型来承担文本生成这一核心任务。

**deepseek-r1** 模型是基于先进的深度学习技术开发的，它具有独特的架构和训练方式，能够更好地捕捉文本中的语义信息，从而为文本生成带来更出色的效果。同时，我们选用**nomic-embed-text**模型用于嵌入任务，该模型能够将文本转换为高维向量表示，为后续的检索和匹配提供了强有力的支持。二者协同工作，共同助力我们基于DeepSeek与openGauss打造的本地AI知识库，使其能够更高效、更精准地为用户提供服务。

```shell
[test@localhost ~]$ ollama --version
ollama version is 0.5.6
```

```shell
[test@localhost ~]$ ollama pull deepseek-r1
[test@localhost ~]$ ollama pull nomic-embed-text
```

## openGauss安装部署：构建可靠数据存储基石

通过openGauss向量数据库存储本地私域知识，在快速检索的同时提供更加稳定、更加安全的数据防护。

### 下载依赖

首先，我们需要下载 psycopg2 依赖：

```shell
[test@localhost ~]$ pip3 install psycopg2
```

### 获取镜像

通过如下命令获取 openGauss 镜像：

```shell
[root@localhost ~]$ docker pull opengauss/opengauss:7.0.0-RC1
```

### 启动服务

镜像拉取完成后，我们可以启动openGauss服务：

```shell
[root@localhost ~]$ docker run --name opengauss --privileged=true -d -e GS_PASSWORD=****** -p 8888:5432 -v /home/test/opengauss:/var/lib/opengauss opengauss/opengauss:7.0.0-RC1
```

至此，openGauss已经成功安装部署，我们可以通过psycopg2连接openGauss查看版本信息：

```python
import psycopg2

conn = psycopg2.connect(
    database="postgres",
    user="gaussdb",
    password="******",
    host="127.0.0.1",
    port="8888"
)

cur = conn.cursor()
cur.execute("select version();")
rows = cur.fetchall()
print(rows)
```

```python
[('(openGauss 7.0.0-RC1 build 3fb58c89) compiled at 2025-01-20 00:24:26 commit 0 last mr   on x86_64-unknown-linux-gnu, compiled by g++ (GCC) 10.3.0, 64-bit',)]
```

## 构建RAG实例：实现知识的高效检索与生成

我们以openGauss基础语料知识为例，通过**deepseek-r1**，做一个基础版的数据库智能问答小助手。

### 准备数据

以 openGauss 部分语料数据作为私域知识，下载文件：

```python
[test@localhost ~]$ wget https://gitcode.com/opengauss/website/raw/v2/app/zh/faq/index.md
```

对语料进行预处理：

```python
file_path = '/home/test/index.md'

with open(file_path, 'r', encoding='utf-8') as file:
    content = file.read()

paragraphs = content.split('##')

for i, paragraph in enumerate(paragraphs):
    print(f'段落 {i + 1}:\n{paragraph}\n')
    print('-' * 20)
```

### 语料嵌入

在前述章节里，我们已经准备好了**nomic-embed-text**嵌入模型。这里进行简单测试如下：

```python
import ollama

def embedding(text):
    vector = ollama.embeddings(model="nomic-embed-text", prompt=text)
    return vector["embedding"]

text = "openGauss 是一款开源数据库"
emb = embedding(text)
dimensions = len(emb)
print("text : {}, embedding dim : {}, enbedding : {} ...".format(text, dimensions, emb[:10]))
```

```python
text : openGauss 是一款开源数据库, embedding dim : 768, enbedding : [-0.5359194278717041, 1.3424185514450073, -3.524909734725952, -1.0017194747924805, -0.1950572431087494, 0.28160029649734497, -0.473337858915329, 0.08056074380874634, -0.22012852132320404, -0.9982725977897644] ...
```

### 数据导入

根据事先启动的openGauss docker服务连接信息建立连接：

```python
import psycopg2

table_name = "opengauss_data"

conn = psycopg2.connect(
    database="postgres",
    user="gaussdb",
    password="******",
    host="127.0.0.1",
    port="8888"
)
```

创建包含文本数据和向量数据的table：

```python
# 建表
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS {};".format(table_name))
cur.execute("CREATE TABLE {} (id INT PRIMARY KEY, content TEXT, emb vector({}));".format(table_name, dimensions))
conn.commit()
```

将处理好的语料数据向量化后导入到openGauss数据库中，并创建索引：

```python
# 插入数据
for i, paragraph in enumerate(paragraphs):
    emb = embedding(paragraph)
    insert_data_sql = f'''INSERT INTO {table_name} (id, content, emb) VALUES (%s, %s, %s);'''
    cur.execute(insert_data_sql, (i, paragraph, emb))
conn.commit()

# 创建索引
cur.execute("CREATE INDEX ON {} USING hnsw (emb vector_l2_ops);".format(table_name))
conn.commit()
```

### 查询检索

我们尝试询问如下问题：

```shell
question = "openGauss 发布了哪些版本？"
```

在openGauss中检索该问题以获取之前导入数据库中的相关文档：

```python
emb_data = embedding(question)
dimensions = len(emb_data)

cur = conn.cursor()
cur.execute("select content from {} order by emb <-> '{}' limit 1;".format(table_name, emb_data))
conn.commit()

rows = cur.fetchall()
print(rows)

cur.close()
conn.close()
```

查询结果如下：

```shell
[(' 3. openGauss 有哪些版本？\n\nopenGauss 社区每两年发布一个 LTS 版本，LTS 版本作为长期支持版本，可规模上线使用。半年发布一个创新版本，创新版本供用户联创测试使用；涉及重大问题修复时，会按需发布补丁版本。同时按照不同场景分为以下版本：\n\n1. openGauss 企业版:具备更齐全的集群管理功能,适合企业用户；\n2. openGauss 极简版:安装配置简单,解压可用,适合个人开发者；\n3. openGauss 轻量版:精简功能,缩减安装包大小,内存占用更少；\n4. openGauss 分布式镜像:基于 ShardingSphere 和 k8s 的分布式容器化镜像。\n\n详情参考 openGauss 官网[“学习”->“文档”](https://docs-opengauss.osinfra.cn)区域。\n\n',)]
```

### 结合LLM实现RAG

首先，为对比RAG前后效果，我们直接询问大模型得到如下答案，结果并不理想：

```shell
嗯，我现在想了解一下 openGauss 有哪些发布版本。我之前对这个软件不太熟悉，但听说它是一个用于关系型数据库的高可用性云原生解决方案，可能和 PostgreSQL 类似。首先，我应该确认一下什么是 openGauss，它的主要用途是什么，以及它在市场上的地位。

然后，关于发布版本的部分，用户想知道有哪些具体的版本号。我记得有时候软件会有主版本（major version）和次版本（minor version），还有内部版本（微内核）。例如，PostgreSQL 的版本通常以 9.x、10.x 这样的命名，所以 openGauss 可能也有类似的结构。

我需要查一下最新的信息，看看官方或者可靠的第三方网站有没有发布过的版本列表。可能的话，还可以查看社区讨论或开发者文档，了解不同版本之间的区别和新增的功能。

另外，开放源代码项目通常会记录所有已发布的版本，甚至包括一些内部版本。所以，我应该考虑访问项目官网的版本历史部分，或者是相关的 GitHub 仓库，看看是否有详细的发布日志。

还有一点是，有时候软件会在不同地区或国家有不同的本地版本，这可能影响到发布的名称和编号。例如，在中国，可能会有本地化的版本，比如 openGauss - GA - 1.x 这样的命名方式。
```

借助openGauss检索结果作为上下文，将文档进行格式化处理，并为大模型制定PROMPT：

```shell
context = "\n".join(row[0] for row in rows)

SYSTEM_PROMPT = "你作为一个对话 AI 助手，结合上下文信息简练高效的回答用户提出的问题"
USER_PROMPT = f"请结合{context}信息来回答{question}的问题，不需要额外的无用回答"
```

基于ollama平台，我们采用**deepseek-r1**大模型进行知识问答：

```shell
response: ChatResponse = chat(
    model="deepseek-r1",
    messages=[
        {"role": "user", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT}
    ],
)

print(response["message"]["content"])
```

```shell
<think>
好的，我现在要解决用户关于openGauss发布版本的问题。根据提供的上下文信息，openGauss每两年发布一个LTS版本，并且半年发布创新版本，当有重大问题修复时会发布补丁版本。

首先，我需要整理已知的版本类型：

1. **企业版**：面向企业用户，功能齐全。
2. **极简版**：适合个人开发者，安装配置简单，解压即可使用。
3. **轻量版**：精简功能，安装包小，占用内存少。
4. **分布式镜像**：基于ShardingSphere和k8s的分布式容器化镜像。

接下来，LTS版本是长期支持版本，创新版本供联创测试。此外，还有补丁版本用于修复问题。

用户的问题是询问openGauss发布了哪些版本？因此，我需要总结这些信息，列出所有已知版本类型，并说明每个版本的目标用户和功能特点。

最后，确保回答简练、高效，避免任何无用内容。
</think>
openGauss 社区发布的主要版本类型包括：

1. **企业版**：适合企业用户，提供全面的集群管理功能。
2. **极简版**：适用于个人开发者，安装配置简单且解压即可使用。
3. **轻量版**：精简功能，适合需要小而精的应用场景。
4. **分布式镜像**：基于 ShardingSphere 和 k8s 的分布式容器化镜像。

这些版本根据不同的应用场景提供定制化的解决方案。LTS 版本作为长期支持版本，创新版本则供用户进行联创测试使用。当出现重大问题时，会发布补丁版本进行修复。更多信息可参考 openGauss 官网。
```

可见，使用 deepseek 结合 openGauss 搭建的 RAG 应用，不仅能借助 deepseek 强大的文本生成能力和精准的文本嵌入功能，还能依托 openGauss 出色的向量数据库高效存储和快速检索向量数据，从而显著提升答案的准确性、可靠性，有效避免大语言模型的幻觉问题，为企业提供更优质的本地化知识服务。

# 结语

最后，我们基于ollama，成功利用openGauss和DeepSeek从零搭建起了简易的 RAG 应用，如愿获取到了所需的知识数据。在这个过程中，我们对 RAG 技术有了更深入的理解，也切实体会到了它在解决 LLM 实际应用问题上的作用。这个简易应用只是一个开端，你可以依据自身需求对相关环节进行灵活调整和优化，以更好地满足不同场景的需要。希望本文能帮助你加深对 RAG 技术应用实践的认识。
