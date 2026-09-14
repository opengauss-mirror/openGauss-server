# oGMemory Architecture

oGMemory is a context lifecycle engine designed for agents. It manages the key boundaries of the agent loop, including what enters the context window, what is persisted, what is compressed and archived, and what can be recalled in subsequent sessions.

## Architecture Overview

The core design of oGMemory is to manage context as data with a lifecycle, rather than treating it as simple vector retrieval text. The system consists of the access layer, service orchestration layer, write link, read link, ContextFS, session layer, and asynchronous index layer.

`ContextFS` is a unified context storage abstraction for the upper layer. It can be implemented using AGFS or DB-first storage. DB-first refers to using database tables to directly store semantic nodes, content, abstracts, overviews, relationships, and metadata, which is an alternative to the primary storage form of AGFS. The upper-layer extraction, write policy, retrieval, and compression processes do not need to be aware of whether the underlying storage is AGFS or DB-first.

```text
  Agent / Plugin
        |
        |  HTTP REST / Python SDK
        v
  +------------------------------------------------------------------+
  | HTTP API                                                         |
  | Lifecycle entry, authentication, and RequestContext construction |
  +-------------------------+----------------------------------------+
                            |
                            v
  +--------------------------------------------------+
  | MemoryService                                    |
  | compose / after_turn / compact / dispose         |
  +-------------------------+------------------------+
                            |
          +-----------------+-----------------+
          |                                   |
          v                                   v
+-----------------------------------+      +-----------------------------------+
| Write link                        |      | Read link                         |
| Extraction -> Policy-based routing|      | Intent -> L0/L1/L2                |
| -> Atomic write                   |      | -> Scoring -> Assembly            |
+-------------------+---------------+      +-------------------+---------------+
            |                                   |
            +-----------------+-----------------+
                              |
                              v
  +----------------------------------------------------------------------------+
  | ContextFS                                                                  |
  | Semantic nodes, content, abstracts, overviews, relationships, and metadata |
  +-------------------------+--------------------------------------------------+
                            |
          +-----------------+-----------------+
          |                                   |
          v                                   v
+---------------------+               +----------------------------+
| Session layer       |               | Asynchronous index layer   |
| SessionBuffer       |               | OutboxWorker               |
| Archive / Compact   |               | L0/L1/L2 Index             |
+---------------------+               +----------------------------+
                            |
                            v
  +--------------------------------------------------+
  | Storage backend                                  |
  | AGFS / DB-first + Vector DB                      |
  +--------------------------------------------------+
```

### Writing to a Data Stream

The path from the end of an agent turn to the time when a conversation can be retrieved is as follows:

```text
End of agent turn
   |
   v
after_turn
   |
   v
Incremental extraction
   |
   v
CandidateMemory[]
   |
   v
PolicyRouter
Selecting a write policy based on the context type
   |
   v
ContextWriter
Atomic write to ContextFS
The underlying layer can be AGFS or DB-first.
   |
   v
OutboxEvent
   |
   v
Index Worker
Generating L0, L1, and L2 vector indexes
```

The write link focuses on the following four aspects:

| Target | Description |
| --- | --- |
| Extraction | Identifies candidate contexts such as profile, preference, event, and skill from conversations. |
| Routing | Selects the merge, aggregate, or append write policy based on the context type. |
| Persistence | Writes data to the primary storage through ContextFS. The underlying layer can be AGFS directory files or DB-first database tables. |
| Indexing | Generates L0, L1, and L2 vector indexes in the background through Outbox after the write is complete. |

### Data Stream Reading

The path from the time when a user query enters oGMemory to the time when the agent context is injected is as follows:

```text
User query
   |
   v
compose
   |
   v
Intent recognition and query planning
   |
   v
L0, L1, and L2 hybrid vector search
   |
   v
L0 and L1 hits as the directory entry
   |
   v
Hierarchical expansion to L2 content
   |
   v
Reads the content from the source ContextFS.
The underlying AGFS/DB-first is transparent to the read link
   |
   v
Sorting, deduplication, and budget control
   |
   v
Assembles into injectable context
```

The read link does not perform a single-layer top-k search. oGMemory indexes the abstract, overview, and full content simultaneously.

| Level | Content | Purpose |
| --- | --- | --- |
| L0 | Short abstract | Quickly locates the topic and serving as the directory roadmap. |
| L1 | Structured overview | Determines relevance and content scope. |
| L2 | Full content | Provides detailed evidence that can be finally injected. |

This hierarchical retrieval method can reduce the missing recall caused by the dilution of long text embedding signals. L0 and L1 are responsible for locating, and L2 is responsible for providing complete information.

Regardless of whether the primary storage uses AGFS or DB-first, the read link retrieves the content through ContextFS. The vector database is only responsible for the index view required for candidate recall and ranking.

## Lifecycle Model

oGMemory organizes context capabilities based on the agent running phase. Each phase only processes the tasks that are suitable for that phase, avoiding putting all logic into a single retrieval or extraction.

| Phase | Occasion | Main Entry | Purpose |
| --- | --- | --- | --- |
| 1. Message arrival | Before agent inference | `compose` | Parses the current question and prefetches candidate contexts. |
| 2. Inference preparation | Assembles the context window. | `compose` | Injects profiles, archives history, session status, and retrieval evidence. |
| 3. Tool use | Before and after tool invocation | tool usage collector | Collects tool invocation statistics to provide input for subsequent extraction and observation. |
| 4. End of a round | After the agent completes a round | `after_turn` | Accumulates new messages and extracts and writes them to long-term memory when the threshold is reached. |
| 5. Compression management | The context is close to the upper limit or is explicitly compacted. | `prepare_compaction` / `compact` | Extracts the unprocessed content, archives the session, and returns the compressed summary. |
| 6. Session closure | The session ends. | `dispose` | Updates the remaining messages and clears the active session. |

### Context Transfer in the Lifecycle

```text
Dialog message
  -> SessionBuffer
  -> Incremental extraction
  -> ContextFS node
     (Primary storage of AGFS or DB-first)
  -> Outbox asynchronous index
  -> Hierarchical retrieval
  -> Compose injection
  -> Compact/Dispose archiving
```

## Context Types

oGMemory distinguishes context types based on information semantics. Different types have different lifecycles, write policies, and scopes.

| Type | Purpose | Write Method | Typical Scope |
| --- | --- | --- | --- |
| profile | Stable user identity information, such as name, occupation, and location | Merging | user |
| preference | User preferences, such as coding style and tool preferences | Merging by topic | user |
| entity | Entity facts such as people, projects, organizations, and locations | Merging by entity | user |
| event | Historical events with time boundaries | Append write | user |
| case | Cases and experience of agents in solving problems | Append write | agent |
| pattern | Behavior patterns observed from interactions | Merging by topic | user |
| skill | Workflow and skills that can be reused by agents | Merging by skill | agent |
| tool | Tool usage experience, parameter suggestions, and failure modes | Merging by tool | agent |

These types are declared by the YAML schema. The schema determines the extracted fields, owner scope, catalog template, and write mode. Therefore, users can add custom context types without modifying the main process.
