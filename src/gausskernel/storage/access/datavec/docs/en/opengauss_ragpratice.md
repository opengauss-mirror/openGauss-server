# Break the AI Black Box and Embrace Open Source: Build a Local Knowledge Base with openGauss + DeepSeek for Your Own AI Assistant

## Introduction: What Are RAG and LLM?

**Large Language Model (LLM):** AI models such as ChatGPT have powerful language understanding and generation capabilities. However, their knowledge is limited to their training data, and they may generate "hallucinations," or inaccurate information.

**Retrieval-Augmented Generation (RAG):** A framework that combines information retrieval with LLMs. It retrieves relevant information from an external knowledge base in real time and uses an LLM to generate more accurate and reliable answers.

External AI assistants such as ChatGPT currently have several limitations. The accuracy of their responses may be uncertain, data security may be a concern, and access may sometimes be unstable.

**In this tutorial, you will learn how to deploy a dedicated AI knowledge base locally using the open-source openGauss database and DeepSeek, giving you an AI assistant that is more personalized, secure, and controllable.**

## Benefits of openGauss + DeepSeek

- **Open source and transparent:** Say goodbye to the AI "black box." With fully open-source code, you have greater control over your data and can use the solution with greater confidence.

- **High performance and stability:** As a leading database in China, openGauss delivers exceptional performance, security, and stability, while DeepSeek's RAG model precisely understands your needs.

- **Flexible and customizable:** With local deployment, you can customize the knowledge base and its functionality based on your needs, allowing you to build an AI assistant that better serves your specific requirements.

## Benefits of Local Deployment

- **Data security and privacy:** Keep sensitive data local to reduce the risk of information leakage and use the system with greater confidence.
- **Offline availability and stability:** No network connection is required. Access your knowledge base anytime, with a stable and responsive experience.
- **Controllable costs and long-term benefits:** Deploy once and use it over the long term without continuously paying high cloud service fees.

## Preparing for the Practice: Setting Up the Basic Environment

### Configuring the Operating System and Python Environment

The operating system used in this practice is **openEuler 22.03 LTS on a Kunpeng Arm server**. To ensure seamless compatibility and smooth operation across all components, **Python 3.11** is used.

| CPU         |      Memory     |           Disk |                  OS |
| :---------- | :-------------: | -------------: | ------------------: |
| Kunpeng-920 | 16 × 32 GB DDR4 | 1 × 3.2 TB NVMe | openEuler 22.03 LTS |

## Deploying the DeepSeek Inference Model: Unlocking Powerful Text Generation

### Installing the Ollama Service

First, install the Ollama PyPI package to access the Ollama service:

```shell
[test@localhost ~]$ pip3 install ollama
```

Then, use the installation script provided on the official website to install the Ollama service:

```shell
curl -fsSL https://ollama.com/install.sh | sh
```

If network issues prevent direct installation, you can install Ollama manually:

```shell
[test@localhost ~]$ wget https://ollama.com/download/ollama-linux-amd64.tgz
[test@localhost ~]$ tar -zxvf ollama-linux-amd64.tgz -C /usr/
[test@localhost ~]$ which ollama
/usr/bin/ollama
```

Note that for the Arm architecture, use the following download address: <https://ollama.com/download/ollama-linux-arm64.tgz>

After the installation is complete, start the Ollama service:

```shell
[test@localhost ~]$ ollama serve &
```

### Selecting the Key Models: DeepSeek and nomic-embed-text

In a RAG application, the text embedding model and text generation model are essential components. In this practice, the **deepseek-r1** model from the DeepSeek family is used for the core text generation task.

The **deepseek-r1** model is based on advanced deep learning technologies and uses a distinctive architecture and training approach to better capture semantic information in text and deliver better text generation results. The **nomic-embed-text** model is used for the embedding task. It converts text into high-dimensional vector representations, providing strong support for subsequent retrieval and matching. Together, the two models support the local AI knowledge base built with DeepSeek and openGauss, enabling more efficient and accurate services.

```shell
[test@localhost ~]$ ollama --version
ollama version is 0.5.6
```

```shell
[test@localhost ~]$ ollama pull deepseek-r1
[test@localhost ~]$ ollama pull nomic-embed-text
```

## Installing and Deploying openGauss: Building a Reliable Data Storage Foundation

The openGauss vector database is used to store private local knowledge, providing fast retrieval while ensuring stable and secure data protection.

### Installing Dependencies

First, install the `psycopg2` dependency:

```shell
[test@localhost ~]$ pip3 install psycopg2
```

### Obtaining the Image

Obtain the openGauss image using the following command:

```shell
[root@localhost ~]$ docker pull opengauss/opengauss:7.0.0-RC1
```

### Starting the Service

After pulling the image, start the openGauss service:

```shell
[root@localhost ~]$ docker run --name opengauss --privileged=true -d -e GS_PASSWORD=****** -p 8888:5432 -v /home/test/opengauss:/var/lib/opengauss opengauss/opengauss:7.0.0-RC1
```

openGauss is now successfully installed and deployed. You can use `psycopg2` to connect to openGauss and view its version information:

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

## Building a RAG Application: Efficient Knowledge Retrieval and Generation

Using basic openGauss corpus data as an example, build a basic intelligent database question-answering assistant with **deepseek-r1**.

### Preparing Data

Use some openGauss corpus data as private knowledge and download the file:

```python
[test@localhost ~]$ wget https://gitcode.com/opengauss/website/raw/v2/app/zh/faq/index.md
```

Preprocess the corpus:

```python
file_path = '/home/test/index.md'

with open(file_path, 'r', encoding='utf-8') as file:
    content = file.read()

paragraphs = content.split('##')

for i, paragraph in enumerate(paragraphs):
    print(f'Paragraph {i + 1}:\n{paragraph}\n')
    print('-' * 20)
```

### Embedding the Corpus

The **nomic-embed-text** embedding model was set up in the preceding section. Perform a simple test as follows:

```python
import ollama

def embedding(text):
    vector = ollama.embeddings(model="nomic-embed-text", prompt=text)
    return vector["embedding"]

text = "openGauss is an open-source database"
emb = embedding(text)
dimensions = len(emb)
print("text : {}, embedding dim : {}, embedding : {} ...".format(text, dimensions, emb[:10]))
```

```python
text : openGauss is an open-source database, embedding dim : 768, embedding : [-0.5359194278717041, 1.3424185514450073, -3.524909734725952, -1.0017194747924805, -0.1950572431087494, 0.28160029649734497, -0.473337858915329, 0.08056074380874634, -0.22012852132320404, -0.9982725977897644] ...
```

### Importing Data

Establish a connection using the connection information of the openGauss Docker service started earlier:

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

Create a table containing text and vector data:

```python
# Create the table
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS {};".format(table_name))
cur.execute("CREATE TABLE {} (id INT PRIMARY KEY, content TEXT, emb vector({}));".format(table_name, dimensions))
conn.commit()
```

Convert the preprocessed corpus into vectors, import the vectors into the openGauss database, and create an index:

```python
# Insert data
for i, paragraph in enumerate(paragraphs):
    emb = embedding(paragraph)
    insert_data_sql = f'''INSERT INTO {table_name} (id, content, emb) VALUES (%s, %s, %s);'''
    cur.execute(insert_data_sql, (i, paragraph, emb))
conn.commit()

# Create an index
cur.execute("CREATE INDEX ON {} USING hnsw (emb vector_l2_ops);".format(table_name))
conn.commit()
```

### Querying and Retrieving Data

Try the following question:

```shell
question = "What versions has openGauss released?"
```

Retrieve relevant documents from openGauss based on the question:

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

The query result is as follows:

```shell
[(' 3. What versions does openGauss have?\n\nThe openGauss community releases an LTS version every two years. LTS versions are long-term support versions and can be deployed at scale. An innovation version is released every six months for collaborative testing. When major issues need to be fixed, patch versions are released as needed. In addition, the following versions are available for different scenarios:\n\n1. openGauss Enterprise Edition: Provides more comprehensive cluster management features and is suitable for enterprise users.\n2. openGauss Minimalist Edition: Easy to install and configure, ready to use after decompression, and suitable for individual developers.\n3. openGauss Lightweight Edition: Streamlined features, smaller installation package, and lower memory usage.\n4. openGauss Distributed Image: A distributed containerized image based on ShardingSphere and Kubernetes.\n\nFor details, see the [Learn -> Documentation](https://docs.opengauss.org/en/) section on the openGauss official website.\n\n',)]
```

### Integrating the LLM to Implement RAG

To compare the results before and after using RAG, first ask the LLM directly. The result is not ideal:

```shell
Hmm, I now want to understand what release versions openGauss has. I'm not very familiar with this software, but I've heard it is a high-availability cloud-native solution for relational databases, possibly similar to PostgreSQL. First, I should confirm what openGauss is, what its main uses are, and its position in the market.

Then, regarding the release versions, the user wants to know the specific version numbers. I recall that software sometimes has major versions and minor versions, as well as internal versions (microkernels). For example, PostgreSQL versions are usually named 9.x, 10.x, so openGauss might have a similar structure.

I need to check the latest information to see if there is a list of released versions on the official website or reliable third-party websites. If possible, I can also look at community discussions or developer documentation to understand the differences between versions and new features.

Also, open-source projects usually record all released versions, including some internal ones. So, I should consider visiting the project's version history section on its official website or the relevant GitHub repository to see if there is a detailed release log.

Another point is that software sometimes has different localized versions in different regions or countries, which may affect the naming and numbering of releases. For example, in China, there may be localized versions such as openGauss-GA-1.x.
```

Use the openGauss retrieval results as context, format the document, and define a prompt for the LLM:

```shell
context = "\n".join(row[0] for row in rows)

SYSTEM_PROMPT = "As a conversational AI assistant, answer user questions concisely and efficiently based on the context information."
USER_PROMPT = f"Please answer the question '{question}' based on the following context: {context}. Do not provide unnecessary extra information."
```

Use the **deepseek-r1** LLM on the Ollama platform for knowledge-based Q&A:

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
Okay, I now need to address the user's question about the release versions of openGauss. According to the provided context, openGauss releases an LTS version every two years, releases innovation versions every six months, and releases patch versions when major issues need to be fixed.

First, I should organize the known version types:

1. **Enterprise Edition**: Designed for enterprise users and fully featured.
2. **Minimalist Edition**: Suitable for individual developers, easy to install and configure, and ready to use after decompression.
3. **Lightweight Edition**: Streamlined features, smaller installation package, and lower memory usage.
4. **Distributed Image**: A distributed containerized image based on ShardingSphere and Kubernetes.

Next, LTS versions are long-term support versions, and innovation versions are for collaborative testing. Additionally, patch versions are released to fix issues.

The user is asking what versions openGauss has released. Therefore, I need to summarize this information, list all known version types, and describe the target users and functional characteristics of each version.

Finally, ensure the answer is concise, efficient, and free of unnecessary content.
</think>
The main version types released by the openGauss community include:

1. **Enterprise Edition**: Suitable for enterprise users, providing comprehensive cluster management features.
2. **Minimalist Edition**: Designed for individual developers, with simple installation and configuration, ready to use after decompression.
3. **Lightweight Edition**: Streamlined features for scenarios that require a compact and focused solution.
4. **Distributed Image**: A distributed containerized image based on ShardingSphere and Kubernetes.

These versions provide customized solutions for different application scenarios. LTS versions serve as long-term support versions, while innovation versions are intended for collaborative testing. When major issues arise, patch versions are released to address them. For more information, refer to the openGauss official website.
```

As you can see, the RAG application built with DeepSeek and openGauss benefits from DeepSeek's powerful text generation capabilities and accurate text embedding capabilities. It also takes advantage of openGauss's vector database for efficient storage and fast retrieval of vector data. This significantly improves the accuracy and reliability of answers and helps eliminate LLM hallucinations, providing enterprises with higher-quality local knowledge services.

# Conclusion

Finally, using Ollama, we successfully built a simple RAG application with openGauss and DeepSeek from scratch and retrieved the required knowledge. Through this process, we gained a deeper understanding of RAG technology and experienced firsthand how it can address practical LLM application challenges. This simple application is only a starting point. You can flexibly adjust and optimize each component based on your needs to better meet the requirements of different scenarios. We hope this document helps deepen your understanding of practical RAG applications.
