# Mem0 + openGauss

Mem0为大型语言模型提供了一个智能、自我改进的内存层，从而实现跨应用程序的个性化AI体验。

## Mem0核心能力

- 用户、会话和 AI Agent 留存：跨用户会话、交互和 AI Agent 保留信息，确保连续性和上下文。
- 自适应个性化：自适应系统不断从用户交互中学习，随着时间的推移不断完善其理解。
- 开发者友好的 API：提供简单的 API，可无缝集成到各种应用中。
- 平台一致性：确保不同AI平台和设备上的行为和数据一致。

## Mem0如何工作

Mem0 利用混合数据库方法来管理和检索 AI 代理和助手的长期记忆。每个记忆都与一个唯一标识符相关联，例如用户 ID 或代理 ID，这使得 Mem0 能够组织和访问特定于个人或情境的记忆。当使用 add() 方法将消息添加到 Mem0 时，系统会提取相关事实和偏好，并将其存储在数据存储中：openGauss向量数据库。这种混合方法可确保以最有效的方式存储不同类型的信息，从而使后续搜索快速有效。

当 AI 代理或 LLM 需要回忆记忆时，它会使用 search() 方法。然后，Mem0 会在这些数据存储中进行搜索，从每个来源检索相关信息。然后，这些信息会通过评分层，该评分层会根据相关性、重要性和新近性评估其重要性。这可确保只显示最个性化和最有用的上下文。然后可以根据需要将检索到的记忆附加到 LLM 的提示中，从而增强其响应的个性化和相关性

## Mem0应用场景

- 个性化学习助手：跟踪用户学习进度与偏好，生成定制化学习计划。
- 医疗健康助手：长期记录患者病史、用药记录，提供连续性护理建议。
- 虚拟伴侣：通过记忆用户习惯和对话历史，建立情感化交互。

## 开始使用Mem0 + openGauss

### 下载所需程序库

下载安装mem0

```
Pip install mem0ai
```

安装openGauss数据库，windows系统可以通过[容器部署](https://docs.opengauss.org/zh/docs/7.0.0-RC1-lite/docs/InstallationGuide/%E5%AE%B9%E5%99%A8%E9%95%9C%E5%83%8F%E5%AE%89%E8%A3%85.html)并启动openGauss数据库。

### 配置Mem0并使用openGauss作为向量存储数据库

在本样例中，我们将使用OpenAI作为LLM，并配置OpenAI api秘钥。

```
import os
os.environ["OPENAI_API_KEY"] = "sk-xxxxxxx"
```

配置openGauss作为Mem0的向量存储数据库

```
from mem0 import Memory
config = {
    "vector_store": {
        "provider": "opengauss",
        "config": {
            "dbname": "your_db_name",
            "user": "your_db_user",
            "password": "your_db_password",
            "host": "your_db_host",
            "port": "your_db_port",
        }
    }
}
```

### 使用openGauss对Mem0结果进行存储

openGauss默认使用hnsw（vector_cosine_ops）创建向量索引

#### 添加记忆, 使用add方法存储记忆

user为用户输入，assistant为模拟LLM回答。

```
m = Memory.from_config(config)
messages = [
{"role": "user", "content": "I'm planning to watch a movie tonight. Any recommendations?"},
{"role": "assistant", "content": "How about a thriller movies? They can be quite engaging."},
{"role": "user", "content": "I’m not a big fan of thriller movies but I love sci-fi movies."},
{"role": "assistant", "content": "Got it! I'll avoid thriller recommendations and suggest sci-fi movies in the future."}
]

res = m.add(messages, user_id="alice", metadata={"category": "movies"})
```

输出结果，成功添加3条用户记忆并且关联到用户alice。

```
{'results': [
    {'id': 'cf375ed0-d4b4-4542-ae91-1c103d090fa7', 'memory': 'Planning to watch a movie tonight', 'event': 'ADD'},
    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'event': 'ADD'},
    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'event': 'ADD'}
    ]
}
```

#### 更新记忆，将Planning to watch a movie tonight更新为likes to watch comedy

```
mem_id = res["results"][0]["id"]
res = m.update(memory_id = mem_id, data="likes to watch comedy")
```

输出结果，Planning to watch a movie tonight记录成功更新为likes to watch comedy。

```
{'results': [
    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'hash': '028dfab4483f28980e292f62578d3293', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.229278-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'hash': '1110b1af77367917ea2022355a16f187', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.305949-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': 'cf375ed0-d4b4-4542-ae91-1c103d090fa7', 'memory': 'likes to watch comedy', 'hash': '1b1162a3c9d4edec465896f7f987ca7d', 'metadata': None, 'created_at': '2025-04-24T01:36:10.130706-07:00', 'updated_at': '2025-04-24T01:36:10.954341-07:00', 'user_id': 'alice'}
    ]
}
```

#### 搜索记忆，根据用户输出信息返回结果

```
query = "What movies do I like to watch?"
res = m.search(query, user_id = "alice")
```

输出结果，结果按照相关性排序输出。

```
{'results': [
    {'id': 'cf375ed0-d4b4-4542-ae91-1c103d090fa7', 'memory': 'likes to watch comedy', 'hash': '1b1162a3c9d4edec465896f7f987ca7d', 'metadata': None, 'score': 0.510667311390375, 'created_at': '2025-04-24T01:36:10.130706-07:00', 'updated_at': '2025-04-24T01:36:10.954341-07:00', 'user_id': 'alice'},

    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'hash': '1110b1af77367917ea2022355a16f187', 'metadata': {'category': 'movies'}, 'score': 0.520610560306129, 'created_at': '2025-04-24T01:36:10.305949-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'hash': '028dfab4483f28980e292f62578d3293', 'metadata': {'category': 'movies'}, 'score': 0.619080122030983, 'created_at': '2025-04-24T01:36:10.229278-07:00', 'updated_at': None, 'user_id': 'alice'}
    ]
}
```

#### 删除记忆

此处mem_id为更新Planning to watch a movie tonight更新为likes to watch comedy，此操作会删除此条记忆

```
m.delete(memory_id = mem_id)
res = m.get_all("alice")
```

输出结果，likes to watch comedy记忆成功删除，此时记忆仅剩2条。

```
{'results': [
    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'hash': '028dfab4483f28980e292f62578d3293', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.229278-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'hash': '1110b1af77367917ea2022355a16f187', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.305949-07:00', 'updated_at': None, 'user_id': 'alice'}
    ]
}
```
