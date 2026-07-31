# 让学习助手真正记住你：OpenClaw 与 oGMemory 的长期陪伴式学习案例

Agent 在一次短对话里回答问题并不难，难的是：用户连续使用几天、几周之后，它还能不能记住之前说过的人、事、时间和偏好。

这就是长期记忆的价值。它让 Agent 不只是依赖当前上下文窗口，而是能把历史对话中有用的信息沉淀下来，在后续任务中重新找回并使用。

## 使用背景

在真实使用中，用户不会每次都把背景重新说一遍。很多信息会自然地分散在不同时间的对话里：

- 今天提到一个计划。
- 几天后补充新的进展。
- 之后又聊到相关人物、事件或偏好。
- 再往后，用户直接追问：“我之前说的那件事是什么时候发生的？”

如果没有长期记忆，Agent 很容易忘记早期信息、混淆人物关系，或者无法接上之前的任务进度。oGMemory 要解决的就是这类问题：让 Agent 在长期使用中仍然能记住过去、理解现在，并继续完成后续任务。

```text
多天、多轮历史对话 ──→ oGMemory（沉淀长期记忆）──→ 当前问题
                              │
                              └── 召回人物、事件、时间线、偏好 ──→ OpenClaw Agent（结合历史回答）
```

## 案例设置

这个案例使用 `locomo-eval-kit` 中的 LoCoMo Case 0 作为测试场景。LoCoMo 是一个面向长期对话记忆的评测数据集，适合模拟“先发生多轮历史对话，之后再进行问答”的使用方式。

这个用例的对话主角是 Caroline 和 Melanie。两个人在多个日期里聊到各自的生活进展、家庭关系、身份认同、学习和职业计划。信息不是集中出现的，而是自然散落在多段会话中。

例如，Caroline 曾聊到自己参加 LGBTQ 支持小组，这段经历让她感到被接纳，也影响了她后来对心理咨询、教育方向和支持 trans 群体的兴趣。后续对话中，她又提到收养计划、朋友和家人的支持、参加相关工作坊等新进展。Melanie 的部分则包含家庭、孩子、慈善跑、露营、自我照顾和创作活动等生活事件。

这类场景很适合说明长期记忆的价值：用户不会把所有背景一次性说完，重要信息会随着时间逐步出现。Agent 如果只看当前对话，就很难回答后续问题。

```text
早期会话（身份、支持小组、教育方向）
    │
    ▼
后续会话（收养计划、家庭和朋友支持）
    │
    ▼
更多进展（工作坊、职业动机、自我照顾）
    │
    ▼
后续 QA（询问人物、事件、时间和原因）
```

这类问题通常包括：

- 某件事是什么时候发生的？
- 某个人的身份、关系或计划是什么？
- 某个决定背后的原因是什么？
- 站在当前日期回看，过去事件应该如何理解？

这些问题不是只看当前一句话就能回答的，必须依赖跨会话记忆。

## 测试形式

这个用例的测试流程很直接：

1. 导入多轮历史对话。
2. 等待 oGMemory 完成记忆写入、整理和索引。
3. 运行一组依赖历史信息的 QA 问题。
4. 使用 Judge 对回答进行自动评分。
5. 汇总准确率、Token 使用量等指标。

```text
导入历史会话
    │
    ▼
oGMemory 形成长期记忆
    │
    ▼
运行跨会话 QA
    │
    ▼
Judge 自动评分
    │
    ▼
汇总准确率和 Token
```

示例运行命令如下：

```bash
./run_eval_small.sh \
  --import-mode claw \
  --gateway-url http://127.0.0.1:18789 \
  --gateway-token ogmem-default-token \
  --force-ingest
```

测试脚本可参考：

- [`run_eval_small.sh`](https://gitcode.com/weixin_44204324/locomo-eval-kit/blob/main/scripts/run_eval_small.sh)
- [`locomo-eval-kit`](https://gitcode.com/weixin_44204324/locomo-eval-kit)

## 结果示例

下面是一段实际运行日志中的结果摘要：

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

从这个结果可以看到，评测会同时关注两类信息：

| 指标 | 说明 |
|---|---|
| 准确率 | Agent 是否正确回答了依赖历史记忆的问题 |
| Token 使用量 | 回答这些问题时消耗了多少上下文和输出成本 |

这能帮助用户理解：长期记忆系统不仅要“能记住”，还要能在实际问答中稳定召回，并且具备可观测的评测指标。

## 总结

这个案例展示的是 oGMemory 最基础也最重要的能力：跨会话记忆。

当用户长期使用 Agent 时，重要信息会分散在多天、多轮对话中。oGMemory 负责把这些信息沉淀成可召回的长期记忆，OpenClaw 负责在当前任务中调用这些记忆并生成回答。

因此，Agent 不再只是一个短对话工具，而是能够接住历史、理解上下文、持续服务用户的长期助手。
