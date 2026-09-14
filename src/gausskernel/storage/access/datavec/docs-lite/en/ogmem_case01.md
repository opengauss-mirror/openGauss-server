# Making Your Learning Assistant Remember You: A Case Study of Long-Term Learning with OpenClaw and oGMemory

It is not difficult for an agent to answer questions in a short conversation. The challenge is whether the agent can still remember the people, events, time, and preferences it has mentioned after the user has used it for days or weeks.

This is the value of long-term memory. It allows the agent to not only rely on the current context window but also to accumulate useful information from historical conversations and retrieve and use it in subsequent tasks.

## Background

In real-world use, users do not always restate the background information. A lot of information is naturally scattered across conversations at different times:

- A plan is mentioned today.
- A few days later, new progress is added.
- Then, the conversation moves on to related people, events, or preferences.
- Later, the user directly asks, "When did I mention that thing before?"

Without long-term memory, the agent can easily forget early information, confuse relationships between people, or fail to continue the previous task. oGMemory aims to solve this kind of problem: enabling the agent to remember the past, understand the present, and continue to complete subsequent tasks even after long-term use.

```text
Multi-day, multi-round historical conversations → oGMemory (accumulating long-term memory) → current question
                              │
                              └── Recalling people, events, timelines, and preferences → OpenClaw Agent (combining historical answers)
```

## Case Settings

This case uses LoCoMo Case 0 in `locomo-eval-kit` as the test scenario. LoCoMo is a benchmark dataset designed for long-term conversational memory, which is suitable for simulating the usage scenario where multiple rounds of historical conversations are conducted before Q&A.

The main characters in this case are Caroline and Melanie. The two of them talked about their life progress, family relationships, identity, and learning and career plans on multiple dates. The information is not concentrated, but naturally scattered across multiple conversations.

For example, Caroline once mentioned that she joined an LGBTQ support group, which made her feel accepted and influenced her later interest in psychological counseling, education, and supporting the trans community. In subsequent conversations, she also mentioned new progress, such as her adoption plan, support from friends and family, and participation in related workshops. Melanie's part includes life events such as family, children, charity runs, camping, self-care, and creative activities.

This scenario is ideal for illustrating the value of long-term memory: users will not provide all the background information at once, and important information will gradually emerge over time. If the agent only looks at the current conversation, it will be difficult to answer subsequent questions.

```text
Early conversations (identity, support group, and education)
    │
    ▼
Subsequent conversations (adoption plan and support from family and friends)
    │
    ▼
More progress (workshops, career motivation, and self-care)
    │
    ▼
Follow-up QA (about people, events, time, and reasons)
```

These questions typically include:

- When did something happen?
- What is a person's identity, relationship, or plan?
- What is the reason behind a decision?
- How should past events be understood from the perspective of the current date?

These questions cannot be answered by looking at a single sentence. Instead, they must rely on cross-session memory.

## Test Format

The test procedure of this case is straightforward:

1. Import multiple rounds of historical conversations.
2. Wait for oGMemory to complete memory writing, sorting, and indexing.
3. Run a group of QA questions that depend on historical information.
4. Use Judge to automatically score the answers.
5. Summarize metrics such as accuracy and token usage.

```text
Import historical conversations.
    │
    ▼
oGMemory forms long-term memory.
    │
    ▼
Run cross-session QA.
    │
    ▼
Judge automatically scores the answers.
    │
    ▼
Summarize accuracy and tokens.
```

The example run command is as follows:

```bash
./run_eval_small.sh \
  --import-mode claw \
  --gateway-url http://127.0.0.1:18789 \
  --gateway-token ogmem-default-token \
  --force-ingest
```

The test script is as follows:

- [`run_eval_small.sh`](https://gitcode.com/weixin_44204324/locomo-eval-kit/blob/main/scripts/run_eval_small.sh)
- [`locomo-eval-kit`](https://gitcode.com/weixin_44204324/locomo-eval-kit)

## Output Example

The following is a summary of the results in an actual run log:

```text
Grading completed: 28/35 correct, accuracy: 80.00%

=== Judge Result Statistics (excluding category=5) ===
Total rows: 35
Graded rows: 35
Correct: 28
Wrong: 7
Accuracy: 80.00%

Token usage (QA):
  Total no-cache tokens (input_tokens): 109,194
  Total cacheRead tokens: 448,256
  Total output tokens: 1,806
  Total input tokens (no-cache + cacheRead): 557,450
  Avg no-cache tokens: 3,119.83
  Avg cacheRead tokens: 12,807.31
  Avg output tokens: 51.60
  Avg total input tokens: 15,927.14
```

From this result, we can see that the evaluation focuses on two types of information:

| Metric | Description |
| --- | --- |
| Accuracy | Whether the agent correctly answers questions that depend on historical memory. |
| Token usage | The amount of context and output cost consumed when answering these questions. |

This helps users understand that a long-term memory system must not only "remember" information but also stably recall it in actual Q&A scenarios, and provide observable evaluation metrics.

## Summary

This case demonstrates the most fundamental and important capability of oGMemory: cross-session memory.

When users use the agent for a long time, important information is scattered across multiple days and multiple rounds of conversations. oGMemory is responsible for accumulating this information into long-term memory that can be recalled, while OpenClaw is responsible for invoking this memory in the current task and generating answers.

Therefore, the agent is no longer just a short conversation tool, but a long-term assistant that can understand the history, comprehend the context, and continuously serve users.
