# oGMemory API

本文档展示 oGMemory HTTP API 的顶层接口。接口由 `server/app.py` 暴露，核心业务由 `MemoryService` 编排。详细请求字段、响应结构和调用示例应以具体集成文档或代码为准。

默认服务地址：

```text
http://localhost:8090
```

## 生命周期接口

生命周期接口服务于 Agent 主循环，覆盖推理前组装、轮次后写入、压缩和会话关闭。

| 方法 | 路径 | 用途 | 备注 |
| --- | --- | --- | --- |
| POST | `/api/v1/compose` | 组装当前轮次上下文 | 推理前调用，返回注入 oGMemory 上下文后的 messages |
| POST | `/api/v1/after_turn` | 轮次结束后增量写入 | 累积消息，达到阈值后后台抽取、写入、索引和归档 |
| POST | `/api/v1/ingest` | 写入消息到 session buffer | 只进入短期会话缓冲，不直接等同长期记忆写入 |
| POST | `/api/v1/ingest_batch` | 批量写入消息到 session buffer | 当前实现等价于 `ingest` |
| POST | `/api/v1/prepare_compaction` | compact 前置准备 | 执行增量抽取准备，返回 prepare token |
| POST | `/api/v1/compact` | 压缩并归档 session | 可直接调用，也可配合 `prepare_compaction` 两阶段调用 |
| POST | `/api/v1/bootstrap` | 会话冷启动入口 | 当前为轻量占位接口 |
| POST | `/api/v1/dispose` | 会话关闭清理 | 刷新剩余消息并清理活跃 session |

## 记忆检索接口

记忆检索接口用于搜索和读取已经进入 oGMemory 的上下文。

| 方法 | 路径 | 用途 | 备注 |
| --- | --- | --- | --- |
| POST | `/api/v1/search_memory` | 语义搜索记忆 | 走查询规划、L0/L1/L2 分层召回和结果组装 |
| POST | `/api/v1/get_memory` | 按 URI 读取记忆 | 读取指定 `ctx://` 记忆节点 |

## 会话接口

会话接口用于管理活跃 session 的消息、状态、归档和上下文。

| 方法 | 路径 | 用途 | 备注 |
| --- | --- | --- | --- |
| POST | `/api/v1/sessions/{session_id}/messages` | 向 session 添加消息 | 写入活跃 SessionBuffer |
| GET | `/api/v1/sessions/{session_id}` | 获取 session 状态 | 返回 pending tokens、消息数量、提交次数等概要 |
| POST | `/api/v1/sessions/{session_id}/commit` | 提交 session 归档 | 可异步或同步提交 |
| GET | `/api/v1/sessions/{session_id}/context` | 获取 session 上下文概要 | 返回活跃消息和历史归档摘要 |

## 观测接口

观测接口用于健康检查、token 使用统计和工具调用统计。

| 方法 | 路径 | 用途 | 备注 |
| --- | --- | --- | --- |
| GET | `/api/v1/health` | 健康检查 | 检查 oGMemory、AGFS、LLM 等基础可用性 |
| GET | `/api/v1/token_stats` | 查看 token 统计 | 返回累计 LLM / embedding / tool usage 统计 |
| POST | `/api/v1/token_stats` | 查看并可重置 token 统计 | 请求中可携带 reset 语义 |
| GET | `/api/v1/stats/tool-usage` | 查看工具调用统计 | 推荐路径 |
| GET | `/stats/tool-usage` | 查看工具调用统计 | 兼容别名 |

## 管理接口

管理接口属于控制面能力，用于账号、用户、角色、Agent 和审计日志管理。通常只在启用 RBAC 后使用。

### 账号管理

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts` | 列出账号 |
| GET | `/api/v1/admin/accounts/{account_id}` | 查看账号概要 |

### 用户与角色管理

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts/{account_id}/users` | 列出账号下用户 |
| POST | `/api/v1/admin/accounts/{account_id}/users` | 创建用户并生成用户 API Key |
| DELETE | `/api/v1/admin/accounts/{account_id}/users/{user_id}` | 删除用户 |
| PATCH | `/api/v1/admin/accounts/{account_id}/users/{user_id}/role` | 修改用户角色 |
| GET | `/api/v1/admin/accounts/{account_id}/roles` | 列出用户角色 |

### Agent 管理

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts/{account_id}/agents` | 列出 Agent |
| POST | `/api/v1/admin/accounts/{account_id}/agents` | 创建 Agent |
| GET | `/api/v1/admin/accounts/{account_id}/agents/{agent_id}` | 查看 Agent |
| PATCH | `/api/v1/admin/accounts/{account_id}/agents/{agent_id}` | 更新 Agent |
| GET | `/api/v1/admin/config/agent-sharing` | 查看 Agent 共享配置 |

### 审计日志

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/admin/accounts/{account_id}/audit-logs` | 列出审计日志 |
| GET | `/api/v1/admin/accounts/{account_id}/audit-logs/{log_id}` | 查看单条审计日志 |

## 兼容与扩展接口

这类接口主要用于内部兼容、插件适配或未来扩展。

| 方法 | 路径 | 用途 | 备注 |
| --- | --- | --- | --- |
| POST | `/api/v1/call/{method}` | 动态调用 `MemoryService` 方法 | 不建议作为外部稳定 API 的首选入口 |

