# oGMemory API

This document describes the top-level APIs of the oGMemory HTTP API. The APIs are exposed by `server/app.py`, and the core services are orchestrated by `MemoryService`. The detailed request fields, response structure, and calling examples are subject to the specific integration document or code.

Default service address:

```text
http://localhost:8090
```

## Lifecycle APIs

The lifecycle APIs serve the main loop of the agent, covering assembly before inference, incremental writing after rounds, compression, and session shutdown.

| Method | Path | Purpose | Remarks |
| --- | --- | --- | --- |
| POST | `/api/v1/compose` | Assembles the context of the current round. | Called before inference. Messages are returned after the oGMemory context is injected. |
| POST | `/api/v1/after_turn` | Incremental writing after rounds end. | Accumulates messages. When the threshold is reached, the messages are extracted, written, indexed, and archived in the background. |
| POST | `/api/v1/ingest` | Writes messages to the session buffer. | The messages are only written to the short-term session buffer, not directly to the long-term memory. |
| POST | `/api/v1/ingest_batch` | Writes messages to the session buffer in batches. | The current implementation is equivalent to `ingest`. |
| POST | `/api/v1/prepare_compaction` | Prepares for compacting. | Executes incremental extraction preparation and returns the prepare token. |
| POST | `/api/v1/compact` | Compresses and archives sessions. | It can be directly called or used together with `prepare_compaction` in two phases. |
| POST | `/api/v1/bootstrap` | Entrance to session cold start. | Currently, it is a lightweight placeholder API. |
| POST | `/api/v1/dispose` | Cleans up and shuts down sessions. | Refreshes remaining messages and clears active sessions. |

## Memory Retrieval API

The memory retrieval API is used to search for and read the context that has been stored in the oGMemory.

| Method | Path | Purpose | Remarks |
| --- | --- | --- | --- |
| POST | `/api/v1/search_memory` | Semantic search memory. | Performs query planning, L0/L1/L2 layered recall, and result assembly. |
| POST | `/api/v1/get_memory` | Reads memory by URI. | Reads the specified `ctx://` memory node. |

## Session API

The session API is used to manage messages, status, archiving, and context of active sessions.

| Method | Path | Purpose | Remarks |
| --- | --- | --- | --- |
| POST | `/api/v1/sessions/{session_id}/messages` | Adds messages to a session. | Writes active session buffers. |
| GET | `/api/v1/sessions/{session_id}` | Obtains the session status. | Returns the summary of pending tokens, number of messages, and number of commits. |
| POST | `/api/v1/sessions/{session_id}/commit` | Submits session archiving. | Can be submitted in asynchronous or synchronous mode. |
| GET | `/api/v1/sessions/{session_id}/context` | Obtains the session context summary. | Returns the summary of active messages and historical archiving. |

## Observation API

The observation API is used for health check, token usage statistics, and tool invocation statistics.

| Method | Path | Purpose | Remarks |
| --- | --- | --- | --- |
| GET | `/api/v1/health` | Health check. | Checks the basic availability of oGMemory, AGFS, and LLM. |
| GET | `/api/v1/token_stats` | Views token statistics. | Returns the accumulated statistics on LLM, embedding, and tool usage. |
| POST | `/api/v1/token_stats` | Views and resets token statistics. | The request can carry the reset semantics. |
| GET | `/api/v1/stats/tool-usage` | Views tool invocation statistics. | Recommended path. |
| GET | `/stats/tool-usage` | Views tool invocation statistics. | Compatible alias. |

## Management APIs

Management APIs are control plane capabilities used to manage accounts, users, roles, agents, and audit logs. Generally, they are used only after RBAC is enabled.

### Account Management

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts` | Lists accounts. |
| GET | `/api/v1/admin/accounts/{account_id}` | Views account summary. |

### User and Role Management

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts/{account_id}/users` | Lists users under an account. |
| POST | `/api/v1/admin/accounts/{account_id}/users` | Creates a user and generates a user API key. |
| DELETE | `/api/v1/admin/accounts/{account_id}/users/{user_id}` | Deletes a user. |
| PATCH | `/api/v1/admin/accounts/{account_id}/users/{user_id}/role` | Changes the role of a user. |
| GET | `/api/v1/admin/accounts/{account_id}/roles` | Lists user roles. |

### Agent Management

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts/{account_id}/agents` | Lists agents. |
| POST | `/api/v1/admin/accounts/{account_id}/agents` | Creates an agent. |
| GET | `/api/v1/admin/accounts/{account_id}/agents/{agent_id}` | Views an agent. |
| PATCH | `/api/v1/admin/accounts/{account_id}/agents/{agent_id}` | Updates an agent. |
| GET | `/api/v1/admin/config/agent-sharing` | Views the agent sharing configuration. |

### Audit Log

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts/{account_id}/audit-logs` | Lists audit logs. |
| GET | `/api/v1/admin/accounts/{account_id}/audit-logs/{log_id}` | Views a single audit log. |

## Compatibility and Extension APIs

These APIs are used for internal compatibility, plug-in adaptation, or future extension.

| Method | Path | Purpose | Remarks |
| --- | --- | --- | --- |
| POST | `/api/v1/call/{method}` | Dynamically calls the `MemoryService` method. | It is not recommended to use this method as the preferred entry for external stable APIs. |

