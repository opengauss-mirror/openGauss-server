# oGMemory 快速安装

## 一、安装方式

| 你想做什么 | 推荐方式 | 会生成什么 | 启动方式 |
| --- | --- | --- | --- |
| 只启动 oGMemory HTTP 服务，供 SDK、脚本或已有 Agent 调用 | Headless | `config/ogmem.yaml` | `ogmem start headless` |
| 把 oGMemory 接入本机已有的 OpenClaw 或 Claude Code | Agent Plugin | `config/ogmem.yaml`，以及 OpenClaw/Claude 侧配置 | `ogmem start plugin` |
| 一次性部署 OpenClaw Gateway + oGMemory + 可选 openGauss | Docker | `deploy/deploy.env`、`deploy/ogmemory.yaml` | `bash deploy/deploy.sh` |

如果你只是验证命令链路，可以选择 `mock` provider，不需要真实模型 API Key。需要真实记忆抽取时，再选择 `openai`、`volcengine`、`dashscope` 或 `zhipu`。

## 二、准备工作

### 1. 安装基础依赖

建议环境：

- Python 3.11+
- PostgreSQL，可选；当前 `ogmem onboard` 默认会生成 SQL 存储配置
- Docker，可选；仅 Docker 一体化部署需要
- OpenClaw 或 Claude Code，可选；仅 Agent Plugin 模式需要

### 2. 获取项目源码

```bash
git clone --branch dev https://gitcode.com/opengauss/oGMemory.git
cd oGMemory
```

### 3. 创建 Python 环境并安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

如果还要运行测试：

```bash
pip install -e ".[dev]"
```

验证 CLI 已安装：

```bash
ogmem --help
```

### 4. 选择存储后端

oGMemory 的安装方式和存储后端是两层选择：Headless、Agent Plugin、Docker 决定服务怎么启动；SQL 或 AGFS 决定记忆数据怎么落盘。

| 存储后端 | 适合场景 | 配置方式 |
| --- | --- | --- |
| SQL | 推荐默认选择，使用 PostgreSQL 直连存储，便于部署和排障 | `storage.backend: sql`，填写 `storage.connection_string` |
| AGFS | 需要使用 AGFS 文件系统能力或兼容旧链路 | `storage.backend: agfs`，本地需准备 AGFS 服务或二进制 |

当前 `ogmem onboard` 默认会生成 SQL 存储配置，并在可连接 PostgreSQL 时尝试初始化 Schema。后续无论选择 Headless、Agent Plugin 还是 Docker，都可以按需要使用 SQL 或 AGFS。

## 三、方式 A：Headless 本地服务

适合只启动 oGMemory HTTP 服务，然后由 SDK、脚本或已有 Agent 访问。

交互式配置：

```bash
ogmem onboard
```

在 `Deployment Target` 中选择：

```text
Headless (CE server only)
```

配置会写入：

```text
config/ogmem.yaml
```

启动服务：

```bash
ogmem start headless
```

如果启动时报 `AGFS binary not found`，先构建 AGFS：

```bash
cd agfs
make build
cd ..
ogmem start headless
```

如果使用 SQL 存储后端，一般不需要启动 AGFS；重点确认 `config/ogmem.yaml` 中的 `storage.connection_string` 能连接到 PostgreSQL。

后台运行：

```bash
ogmem start headless --daemon
```

停止服务：

```bash
ogmem stop local
```

## 四、方式 B：接入本机 Agent

适合你已经在本机安装 OpenClaw 或 Claude Code，希望把 oGMemory 作为长期记忆能力接进去。

> 注意：`ogmem onboard --mode plugin` 不负责安装 OpenClaw 或 Claude Code 本体。它只生成 oGMemory 配置，并根据你选择的 Agent 生成对应插件配置或 hooks。

交互式配置：

```bash
ogmem onboard
```

在 `Deployment Target` 中选择：

```text
Agent Plugin (OpenClaw / Claude Code)
```

随后选择：

- `OpenClaw (context-engine plugin)`：生成 OpenClaw 侧配置文件
- `Claude Code (hooks)`：把 hooks 写入项目 `.claude/settings.json`


常见输出：

```text
config/ogmem.yaml
openclaw.plugin.json
```

启动插件模式：

```bash
ogmem start plugin
```

`ogmem start plugin` 会先确保 oGMemory ContextEngine 可用；如果检测到 OpenClaw 配置且本机有 `openclaw` 命令，会尝试按生成的配置启动 OpenClaw。OpenClaw 本体仍需要你提前安装。

停止 oGMemory 本地服务可使用：

```bash
ogmem stop local
```

OpenClaw 或 Claude Code 进程请按各自工具的方式停止。

OpenClaw 本地插件的更多变量见：

- [openclaw_context_engine_plugin/ENV.md](https://gitcode.com/opengauss/oGMemory/blob/dev/openclaw_context_engine_plugin/ENV.md)
- [openclaw_context_engine_plugin/README.md](https://gitcode.com/opengauss/oGMemory/blob/dev/openclaw_context_engine_plugin/README.md)

## 五、方式 C：Docker 一体化部署

适合一次性拉起 OpenClaw Gateway、oGMemory 和可选 openGauss。

交互式配置：

```bash
ogmem onboard
```

在 `Deployment Target` 中选择：

```text
Docker (containerized)
```

配置会写入：

```text
deploy/deploy.env
deploy/ogmemory.yaml
```

启动容器：

```bash
bash deploy/deploy.sh -password "OpenGauss@2024"
```

查看状态和日志：

```bash
bash deploy/deploy.sh --status
docker logs ogmem -f
docker logs openclaw_ogmem -f
docker logs opengauss -f
```

完整部署变量、ARM64 镜像限制、多租户部署和故障排查见 [deploy/README.md](https://gitcode.com/opengauss/oGMemory/blob/dev/deploy/README.md)。

## 六、验证服务

### 1. 环境检查

```bash
ogmem check
```

建议确认：

- Python 包导入成功
- `config/ogmem.yaml` 或关键环境变量已配置
- 使用真实模型时 API Key 已配置
- 使用 SQL 后端时 PostgreSQL 可连通

### 2. 健康检查

```bash
curl http://127.0.0.1:8090/api/v1/health
```

### 3. 写入一轮对话

如果你想让短对话立刻触发抽取，建议启动前设置：

```bash
export OGMEM_AFTER_TURN_THRESHOLD=1
```

写入示例：

```bash
curl -X POST http://localhost:8090/api/v1/after_turn \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "acct-demo",
    "userId": "user-1",
    "agentId": "main",
    "sessionId": "session-1",
    "messages": [
      {"role": "user", "content": "我叫 Alice，是一名后端工程师"},
      {"role": "assistant", "content": "很高兴认识你，Alice。"}
    ]
  }'
```

常见返回：

- `status: "accumulating"`：消息已进入缓冲区，但还没达到抽取阈值
- `status: "processing"`：已触发后台抽取、写入和索引
- `status: "completed"`：后台线程创建失败时降级为同步执行

### 4. 检索记忆

```bash
curl -X POST http://localhost:8090/api/v1/compose \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "acct-demo",
    "userId": "user-1",
    "agentId": "main",
    "sessionId": "session-2",
    "prompt": "Alice 是做什么工作的？"
  }'
```

返回结果里重点看：

- `messages`：已注入记忆上下文后的消息列表
- `identityContext`：Profile 类稳定身份信息
- `retrievedEvidence`：语义检索命中的证据
- `estimatedTokens`：本次组装后的估算 token 数
