# Introduction to oGMemory

When the CLI Agent is continuously used, the context quickly evolves from "the current round of conversation" to a more complex issue. The user's background, preferences, historical events, tool experience, and project status are all accumulated, but the model itself does not truly remember this information.

oGMemory is designed to address this context management issue. Instead of simply storing conversations as logs, it organizes valuable facts, preferences, events, and experiences into structured memory, and forms a complete lifecycle from writing, indexing, retrieval, compression, to archiving. This allows the Agent to reuse these long-term contexts in subsequent conversations.

## Problem: Token Budget Is a Bottleneck.

Each CLI Agent runs on a **fixed token budget**. The context window is the most expensive and scarce resource.

| Symptoms | Root Cause | Missing Lifecycle Phase |
| --- | --- | --- |
| The Agent forgets the early content of the conversation. | Old content is eliminated after the window is full and is not persisted. | 4. **afterTurn** — Missing extraction and persistence |
| Making the same mistake across sessions | No mechanism for transferring experience across sessions | 6. **session_end** archiving + 2. **bootstrap** cold start |
| A query that should have cost $0.05 ended up costing $0.50. | Flat retrieval loads the entire document, but a summary would suffice. | 2. **assemble** — Missing hierarchical retrieval |
| Multi-agent collaboration failure | Agents cannot see each other's working context. | 4. **afterTurn** cross-session sharing + multi-tenant isolation |
| Contexts keep expanding in long sessions. | There is no systematic compression. | 5. **compact** — Missing signal scoring and summary chain |

This is not a problem with the model. It is a **problem with the infrastructure**. ContextEngine provides the missing layer of infrastructure.

---

## Design Philosophy

### Core Insight: Context Has a Lifecycle.

Current RAG systems treat retrieval as a single operation: vectorizing queries, searching vector databases, and returning results. This ignores a fundamental truth: **context in an Agent system has a complete lifecycle**, just like data in a database.

```
  Birth          Structuring       Storage           Indexing         Retrieval       Compression      Archiving
  (Extract       (Classify by      (Atomic write,    (Vectorize +     (Vector search, (Summarize,      (Session end,
   from session)  type, route      order guaranteed) write to L0/L1/  layered expand, deduplicate,     archive,
                  by strategy)                       L2 IndexRecords) load by budget) compress)        state snapshot)
```

Each phase has different constraints. Extraction must be incremental. Storage must be atomic. Indexing must be asynchronous. Retrieval must be budget-aware. Compression must preserve signals. A system that only processes one or two of these phases leaves the rest to chance. The ContextEngine covers the entire lifecycle.

### Six Interception Points

The agent loop is not a black box. It has well-defined execution phases. Key design choice: **intercept at the loop boundary, rather than within model inference.** All contextual operations occur before and after inference, never during inference—zero latency impact.

```
┌────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              Agent Loop (infinite loop)                                        │
│                                                                                                │
│     ┌───────────┐     ┌─────────────────────────┐     ┌─────────────────┐     ┌────────────┐   │
│     │     ①     │     │           ②             │     │        ③        │     │    ④       │   │
│     │  Message  │────▶│  Inference preparation  │────▶│ Tool invocation │────▶│ Round end │   │
│     │           │     │                         │     │                 │     │            │   │
│     └─────┬─────┘     └─────────────────────────┘     └─────────────────┘     └─────┬──────┘   │
│           ▲                                                                         │          │
│           │           ┌─────────────────────────┐     ┌─────────────────┐           │          │
│           │           │           ⑤             │     │        ⑥        │           │          │
│           └───────────│  Compression management │◀────│  Session closed │◀─────────┘          │
│                       └─────────────────────────┘     └─────────────────┘                      │
│                                                                                                │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
```

| Phase | Moment | What to Do |
| --- | --- | --- |
| 1. Message arrival | Before agent inference | Parse intent and prefetch candidate contexts. |
| 2. Inference preparation | Assemble the context window. | Cold start injection/Topic tracking/Budget planning/Layered loading/De-duplication |
| 3. Tool invocation | Before and after tool execution | Inject tool skills/Parameter derivation/Result compression/Fact extraction |
| 4. Round end | After the agent completes a round | Incremental extraction/Relationship construction/Conflict resolution/Asynchronous indexing |
| 5. Compression management | When the context window is nearly full | Signal scoring/Elimination protection/Redundancy merging/Summary chain |
| 6. Session closure | When the session ends | Task archiving/Status snapshot/Integrity audit |

### Unequal Context Types

There are seven types of contexts, each with fundamentally different lifecycle behaviors. This is not arbitrary classification—it is determined by the semantics of the information itself. A single "store everything the same way" solution either loses variable state (if only appending) or corrupts immutable history (if overwriting).

| Type | Why this lifecycle? | Write Policy | URI Pattern |
| --- | --- | --- | --- |
| **Profile** | User status changes—"I live in Beijing" may become "I moved to Tokyo." | **Merge** — New overwrites old in case of conflict. | `.../memories/profile` |
| **Preference** | Accumulated by topic, with only one current view per topic. | **Combined by slug** | `.../memories/preferences/{slug}` |
| **Entity** | Entities accumulate facts, but "Project Alpha" is still "Project Alpha". | **Combined by slug** | `.../memories/entities/{slug}` |
| **Event** | History is immutable — "Migration completed on March 15" will never change. | **Append-only** | `.../memories/events/{event_id}` |
| **Case** | The problem-solving trajectory is a historical record. | **Append-only** | `.../memories/cases/{case_id}` |
| **Pattern** | Patterns emerge from repeated observations and evolve over time. | **Combined by slug** | `.../memories/patterns/{slug}` |
| **Skill** | Tool expertise accumulates and grows — more experience means better knowledge. | **Accumulative append** | `.../skills/{skill_name}` |

---

## Key Architectural Decisions

### Why Use YAML to Drive Schemas?

**Problem**: Schemas are hardcoded in Python, and the code needs to be modified each time a new context type is added. Each time a change is made, the extraction pipeline, policy routing, and URI parsing need to be modified.

**Solution**: Context types are declaratively defined in YAML schemas. SchemaRegistry loads all YAML files during startup, and PolicyRouter and URIResolver automatically adapt to the registered information.

**What would happen if this solution was not used**: Each time a new type (such as "Decision" or "Handoff") is added, the code needs to be modified across three modules, introducing regression risks. YAML-driven schema allows new types to be added by adding a configuration file instead of modifying three parts of code.

### Why Use the Outbox Pattern for Asynchronous Indexing?

**Problem**: Vectorization and writing vector indexes take 100–500 ms. If the synchronization is performed, each `afterTurn` will be blocked, increasing the response latency of the Agent.

**Solution**: After the write is complete, an OutboxEvent is delivered. The background worker consumes the event and performs embed and upsert. At least one delivery is guaranteed. If the maximum number of retries is exceeded, the message is moved to the dead letter queue. The Worker supports concurrent consumption of `FOR UPDATE SKIP LOCKED` multi-processes.

**What happens if this solution is not used**: Synchronous indexing means that the Agent waits for an additional 100 to 500 ms in each round. This is unacceptable in interactive scenarios. Or, if no index is created, there will be no subsequent semantic retrieval capability.

### Why Is Three-Level Retrieval (L0/L1/L2) Needed?

**Problem**: The longer the content, the lower the vector similarity may be, rather than higher. The embedding of a document with 5000 tokens must represent **each** concept in it, which dilutes the signal of any single topic. When you query "What does Alice do?", the vector generated by the L2 content that is filled with details about Go, Kubernetes, PostgreSQL, and migration policies is scattered. "Backend engineer" is just one of dozens of signals. In contrast, the vector generated by a focused L0 summary ("Alice is a backend engineer") is accurate and directly matches the query.

**Solution**: Each context node is indexed at three levels. L0 summary (~100 tokens) generates a focused vector, which is a precise topic roadmap. L1 overview (~500 tokens) has a balanced signal. L2 full content (~5000 tokens) contains the most information, but the vector is scattered. The retrieval engine performs a unified vector search on the three levels, and then uses the L0/L1 hit as the **directory entry point** to recursively expand the tree structure. When the L0 summary is matched, the searcher expands its child nodes to discover the L2 content that would be missed by flat search. Score propagation (`final = α·child + (1-α)·parent`) gives a boost to the edge L2 results under the strongly matched parent node.

```
┌───────────────────────┐    ┌──────────────────────────────┐    ┌───────────────────────────────────┐
│ L0 summary            │    │ L1 overview                  │    │ L2 full content                   │
│ ~100 token            │    │ ~500 token                   │    │ ~5000 token                       │
│ Focus vector          │    │ Balanced signal              │    │ Comprehensive information         │
│ → Roadmap positioning │    │ → Decision-making reference  │    │ But the vector is dispersed       │
└──────────┬────────────┘    └──────────────┬───────────────┘    └─────────────────┬─────────────────┘
           │                                │                                      │
           ▼                                ▼                                      ▼
      .abstract.md                     .overview.md                            content.md
```

**Advantage of recall**: Only flat vector search is performed on L2. As a result, some content whose vector score is lower than the threshold due to detail dilution is missed. The L0/L1 roadmap guides the searcher to locate the correct directory, and then the tree is expanded to discover the complete content under the directory, including chunks whose original vector score is not high but strongly match the parent topic.

### Why Use File System Abstraction?

**Problem**: Context operations require atomicity, concurrency control, permission isolation, and garbage collection. Each of them is an independent engineering challenge.

**Solution**: Reuse the mature solutions of the file system that have been used for decades. `open/read/write/link/unlink/gc` is directly mapped to context operations. Multi-tenant isolation is enforced at the file system level through `account_id + owner_space` in the path. Even if the caller has a bug, it cannot bypass the isolation.

**What if this solution is not used**: Either develop solutions for each capability (atomic write, concurrent lock, and permission check) or give up these capabilities. The file system metaphor allows us to stand on the shoulders of giants.

### Why Use Optimistic Locking for Concurrent Writes?

**Problem**: The profile node is written to by all user sessions at the same time. Concurrency control is required.

**Solution**: Optimistic locking: Read the current `.meta.json` version and write to it only when the version remains unchanged. Zero overhead when there is no contention; graceful failure and retry when there is contention. No additional infrastructure is required.

**What if this solution is not used**: Distributed locks require coordination services (etcd/ZooKeeper), which increases O&M complexity. If no lock is used, data may be lost due to "write-after-write" overwriting. Optimistic locking delivers optimal performance in common scenarios (without contention) and ensures security in rare scenarios.

### Why Use the ReAct Loop for Extraction?

**Problem**: If the system already knows that "Alice is a backend engineer", extracting the information again is a waste. However, it is impossible to make a judgment without knowing what is already known.

**Solution**: The LLM is equipped with a set of tools— `read(uri)`, `list(uri)`, `get_relations(uri)`, and `extract_*` extraction actions—and then executed in a loop. In each round, the system *reads* the existing memory node (Reason), determines which information is new, and then calls the corresponding `extract_*` tool (Act). If it is uncertain, more nodes are read and the loop continues. This is the real ReAct: inference and tool calling are performed alternately, instead of a single blind extraction.

```
  Iteration 1: read(profile_uri) → "Alice, backend engineer, London"
            → No new information, skip.

  Iteration 2: read(entities/go) → "Go expert, prefers error handling pattern X"
            → New information: "Alice is also using Rust for a side project."
            → extract_entity(slug="rust", ...)
```

**What would happen if this solution is not used**: A single blind extraction would either store known information repeatedly (wasting tokens and storage) or miss implicit new information (losing signals). The ReAct loop trades a small amount of additional cost for significantly higher extraction quality.

---
