# Mem0 + openGauss

Mem0 provides an intelligent, self-improving memory layer for LLMs, enabling personalized AI experiences across applications.

## Mem0 Core Capabilities

- User, session, and AI agent memory: Retains information across user sessions, interactions, and AI agents, ensuring continuity and context.
- Adaptive personalization: The adaptive system continuously learns from user interactions, refining its understanding over time.
- Developer-friendly API: Provides a simple API for seamless integration into various applications.
- Platform consistency: Ensures consistent behavior and data across different AI platforms and devices.

## How Mem0 Works

Mem0 leverages a hybrid database approach to manage and retrieve long-term memory for AI agents and assistants. Each memory is associated with a unique identifier, such as a user ID or agent ID, allowing Mem0 to organize and access memories specific to individuals or contexts. When messages are added to Mem0 using the `add()` method, the system extracts relevant facts and preferences and stores them in the data store: the openGauss vector database. This hybrid approach ensures that different types of information are stored in the most efficient manner, making subsequent searches fast and effective.

When an AI agent or LLM needs to recall memories, it uses the `search()` method. Mem0 then searches across these data stores, retrieving relevant information from each source. This information then passes through a scoring layer that evaluates its importance based on relevance, significance, and recency. This ensures that only the most personalized and useful context is surfaced. The retrieved memories can then be appended to the prompt of the LLM as needed, enhancing the personalization and relevance of its responses.

## Mem0 Use Cases

- Personalized learning assistant: Tracks user learning progress and preferences, generating customized learning plans.
- Healthcare assistant: Maintains long-term records of patient history and medication, providing continuous care recommendations.
- Virtual companion: Builds emotional engagement by remembering user habits and conversation history.

## Getting Started with Mem0 + openGauss

### Downloading Required Libraries

Download and install Mem0:

```
Pip install mem0ai
```

Install openGauss database. On Windows, you can deploy and start openGauss through [container deployment](https://docs.opengauss.org/zh/docs/7.0.0-RC1-lite/docs/InstallationGuide/%E5%AE%B9%E5%99%A8%E9%95%9C%E5%83%8F%E5%AE%89%E8%A3%85.html).

### Configuring Mem0 with openGauss as the Vector Store Database

In this example, we will use an OpenAI LLM and configure the OpenAI API key.

```
import os
os.environ["OPENAI_API_KEY"] = "sk-xxxxxxx"
```

Configure openGauss as the vector store database for Mem0:

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

### Using openGauss to Store Mem0 Results

openGauss uses HNSW (Hierarchical Navigable Small World) (`vector_cosine_ops`) by default to create vector indexes.

#### Adding Memories: Using the `add()` Method to Store Memories

`user` represents the user input, and `assistant` represents the simulated LLM response.

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

The output confirms that 3 user memories have been successfully added and associated with user `alice`:

```
{'results': [
    {'id': 'cf375ed0-d4b4-4542-ae91-1c103d090fa7', 'memory': 'Planning to watch a movie tonight', 'event': 'ADD'},
    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'event': 'ADD'},
    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'event': 'ADD'}
    ]
}
```

#### Updating Memories: Updating "Planning to watch a movie tonight" to "likes to watch comedy"

```
mem_id = res["results"][0]["id"]
res = m.update(memory_id = mem_id, data="likes to watch comedy")
```

The output shows that the "Planning to watch a movie tonight" record has been successfully updated to "likes to watch comedy":

```
{'results': [
    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'hash': '028dfab4483f28980e292f62578d3293', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.229278-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'hash': '1110b1af77367917ea2022355a16f187', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.305949-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': 'cf375ed0-d4b4-4542-ae91-1c103d090fa7', 'memory': 'likes to watch comedy', 'hash': '1b1162a3c9d4edec465896f7f987ca7d', 'metadata': None, 'created_at': '2025-04-24T01:36:10.130706-07:00', 'updated_at': '2025-04-24T01:36:10.954341-07:00', 'user_id': 'alice'}
    ]
}
```

#### Searching Memories: Returning Results Based on User Input

```
query = "What movies do I like to watch?"
res = m.search(query, user_id = "alice")
```

The output returns results sorted by relevance:

```
{'results': [
    {'id': 'cf375ed0-d4b4-4542-ae91-1c103d090fa7', 'memory': 'likes to watch comedy', 'hash': '1b1162a3c9d4edec465896f7f987ca7d', 'metadata': None, 'score': 0.510667311390375, 'created_at': '2025-04-24T01:36:10.130706-07:00', 'updated_at': '2025-04-24T01:36:10.954341-07:00', 'user_id': 'alice'},

    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'hash': '1110b1af77367917ea2022355a16f187', 'metadata': {'category': 'movies'}, 'score': 0.520610560306129, 'created_at': '2025-04-24T01:36:10.305949-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'hash': '028dfab4483f28980e292f62578d3293', 'metadata': {'category': 'movies'}, 'score': 0.619080122030983, 'created_at': '2025-04-24T01:36:10.229278-07:00', 'updated_at': None, 'user_id': 'alice'}
    ]
}
```

#### Deleting Memories

Here, `mem_id` refers to the memory that was updated from "Planning to watch a movie tonight" to "likes to watch comedy". This operation will delete that memory.

```
m.delete(memory_id = mem_id)
res = m.get_all("alice")
```

The output confirms that the "likes to watch comedy" memory has been successfully deleted, leaving only 2 memories:

```
{'results': [
    {'id': 'afe95bb0-093e-43c1-a81e-a24c5e56318e', 'memory': 'Not a big fan of thriller movies', 'hash': '028dfab4483f28980e292f62578d3293', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.229278-07:00', 'updated_at': None, 'user_id': 'alice'},

    {'id': '986b369f-aace-470e-ba75-ae3e5c72a5c5', 'memory': 'Loves sci-fi movies', 'hash': '1110b1af77367917ea2022355a16f187', 'metadata': {'category': 'movies'}, 'created_at': '2025-04-24T01:36:10.305949-07:00', 'updated_at': None, 'user_id': 'alice'}
    ]
}
```
