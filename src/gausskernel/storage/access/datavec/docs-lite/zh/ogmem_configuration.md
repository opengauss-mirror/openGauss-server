# oGMemory 配置指南

首次使用先按 [安装指南](ogmem_install.md) 生成配置；本页说明已有配置怎么修改、怎样生效。以下命令在项目根目录执行，并使用安装时的 Python 虚拟环境。

LLM 是用于抽取记忆的语言模型，Embedding 将内容转换为检索用的向量；两者可以使用不同服务。Gateway 是 OpenClaw 的运行服务。

## 1. 找到要修改的文件

| 模式 | 配置入口 | 修改后如何生效 |
| --- | --- | --- |
| Local | 默认 `config/ogmem.yaml` | 停止并重新启动本地服务 |
| Docker | `.ogmem_data/onboard-docker/ogmemory.env` | 使用向导打印的部署命令重建容器 |
| Plugin | `ogmem-client-config/<适配器>/` 中的配置和 README | 合并到 Agent 实际配置，然后重启或重新加载 Agent |

**Plugin 只配置客户端连接。** LLM、Embedding、数据库仍由已部署的 oGMemory 服务配置，不会生成新的服务 YAML，也不会自动修改 Agent 配置。

Local 用 `OGMEM_CONFIG` 指定要读取的配置文件。下面使用默认文件；如需其他文件，先准备好该文件，再替换路径：

```bash
export OGMEM_CONFIG="$(pwd)/config/ogmem.yaml"
ogmem config show
```

默认先使用项目内的 `config/ogmem.yaml`；该文件不存在时，再查找 `/etc/ogmem/config.yaml`。`OGMEM_CONFIG` 也可以写在项目的 `config/.env` 中；终端已设置的同名变量优先。建议使用绝对路径，并在向导、启动和检查时使用同一文件。

## 2. Local：常用服务参数

向导默认配置 SQL 存储和 openGauss 向量库。下面是字段示例，不能直接运行：`your-...` 地址、模型名，以及数据库用户名和密码都要替换；向量维度按 Embedding 服务实际输出填写。已有配置只修改对应字段，不要整份覆盖。

```yaml
llm:
  provider: openai
  base_url: "https://your-llm-service.example/v1"
  model: "your-chat-model"
  api_key: null                  # 从 OGMEM_API_KEY 读取
  temperature: 0.1
  max_tokens: 4096

embedding:
  provider: openai
  base_url: "https://your-embedding-service.example/v1"
  model: "your-embedding-model"
  api_key: null                  # 从 OGMEM_EMBEDDING_API_KEY 读取

storage:
  backend: sql
  dialect: opengauss
  connection_string: "host=127.0.0.1 port=5432 dbname=ogmemory user=your_user password=your_password"

vector_db:
  type: opengauss
  connection_string: "host=127.0.0.1 port=5432 dbname=ogmemory user=your_user password=your_password"
  dimension: 1536                # 示例值，必须改成模型实际输出维度

service:
  http_port: 8090

memory:
  after_turn_threshold: 200
```

为上面的示例提供独立模型密钥：

```bash
export OGMEM_API_KEY='替换为LLM密钥'
export OGMEM_EMBEDDING_API_KEY='替换为Embedding密钥'
```

也可以直接在 YAML 的 `api_key` 中填写密钥。向导通常会写入实际值；若希望改为从环境变量读取，需删除该字段或设为 `null`，不能只把它改成空字符串。

`export` 只影响当前终端及其启动的进程，已运行的服务不会自动更新。换终端后需重新设置，也可将这些变量写入项目的 `config/.env`；随后重启服务。

| 参数 | 配置时注意 |
| --- | --- |
| `llm.provider` | OpenAI 兼容聊天接口使用 `openai`；`mock` 仅用于链路检查 |
| `llm.max_tokens` | 模型单次输出 token 上限，不是输入上下文长度；须符合模型限制 |
| `embedding.provider` | OpenAI 兼容向量接口明确设为 `openai`，不必与 LLM 提供方相同 |
| `vector_db.dimension` | 必须与 Embedding 输出一致；更换模型时重新确认，已有索引不能直接混用不同维度 |
| `storage.connection_string` | 记忆数据的 SQL 连接；openGauss 需使用 PG 兼容数据库并具备建表权限 |
| `vector_db.connection_string` | 向量索引的连接，可以与 SQL 存储共用；不能把任意 PostgreSQL 当作 openGauss 向量库 |
| `memory.after_turn_threshold` | 待处理内容达到设定的 token 数时触发自动抽取；token 是模型计量文本的单位，不等同于字数或轮次数，也不是全局写入开关 |

SQL 存储不需要 AGFS。需要 Chroma 或 AGFS 时，按 [完整 YAML 参考](https://atomgit.com/opengauss/oGMemory/blob/dev/config/ogmem.reference.yaml) 配置对应后端及依赖。

### 配置优先级

常规服务字段遵循：**YAML 显式值 → 普通环境变量 → 代码默认值**。例如 YAML 已写 `after_turn_threshold: 200`，仅执行 `export OGMEM_AFTER_TURN_THRESHOLD=1` 不会覆盖它。

`OGMEM_CONFIG` 用于选择文件，向导参数用于生成配置；调整已有服务字段时，直接修改实际使用的 YAML。

### 修改并检查

```bash
ogmem stop local
# 编辑实际使用的 YAML，或重新运行 ogmem onboard --mode local
ogmem config show
ogmem start local --daemon
ogmem status
```

`ogmem config show` 显示当前命令加载的配置摘要，并隐藏密钥。确认端口、模型、存储后端等值正确后，再检查服务状态；它不会验证模型或数据库连通性。

重复运行 Local 向导可选择复用或修改。额外自定义字段会保留，配置内容变化时生成 `.bak-<时间戳>` 备份；修改前须先停止服务。

## 3. Docker：修改部署环境

编辑向导生成的 `.ogmem_data/onboard-docker/ogmemory.env`。常用变量如下：

| 变量 | 用途 |
| --- | --- |
| `LLM_BASE_URL`、`LLM_MODEL`、`LLM_API_KEY` | oGMemory 和 quickstart 中 OpenClaw 使用的聊天模型 |
| `EMBEDDING_PROVIDER`、`EMBEDDING_BASE_URL`、`EMBEDDING_MODEL`、`EMBEDDING_API_KEY` | Embedding 配置；OpenAI 兼容接口选择 `openai` |
| `EMBEDDING_DIMENSION` | Embedding 实际输出维度 |
| `OPENGAUSS_HOST_PORT` | 数据库在宿主机上的端口 |
| `OGMEM_HTTP_HOST_PORT`、`OGMEM_MCP_HOST_PORT` | oGMemory HTTP / MCP 的宿主机端口 |
| `OPENCLAW_CE_HOST_PORT`、`OPENCLAW_MCP_HOST_PORT` | 两个 OpenClaw Gateway 的宿主机端口 |
| `OGMEM_AFTER_TURN_THRESHOLD` | quickstart 抽取阈值；未设置时默认 **1** |

例如，在环境文件中添加或修改：

```bash
export OGMEM_AFTER_TURN_THRESHOLD=200
```

Local 服务代码的默认阈值是 **200**；Docker quickstart 为短对话体验将默认值设为 **1**。较小阈值会更频繁触发抽取，增加模型调用。部署脚本对该变量优先使用启动环境中已设置的值，其次使用环境文件，再使用默认值。

修改后，在项目根目录执行向导打印的完整部署命令。使用上述默认目录时，也可执行：

```bash
env \
  OGMEMORY_ENV_FILE="$(pwd)/.ogmem_data/onboard-docker/ogmemory.env" \
  OGMEMORY_STACK_ROOT="$(pwd)/.ogmem_data/onboard-docker" \
  OGMEMORY_RUNTIME_ENV_FILE="$(pwd)/.ogmem_data/onboard-docker/ogmemory-runtime.env" \
  bash deploy/user-quickstart/deploy.sh
```

该命令会重建本实例的四个容器，服务会短暂中断，默认保留数据。仅修改文件或执行 `docker restart` 不能更新容器创建时的环境变量。

随后重新载入运行参数并检查：

```bash
source .ogmem_data/onboard-docker/ogmemory-runtime.env
docker ps --filter "name=${INSTANCE_NAME}_"
curl -fsS "http://${PUBLISH_IP}:${OGMEM_HTTP_HOST_PORT}/api/v1/health"
```

`ogmemory-runtime.env` 是部署生成的运行信息，包含实际端口和服务认证信息；不要将它作为主要编辑入口。重建默认保留数据；模型维度变更、OpenClaw 配置保留和数据重置见 [quickstart 指南](https://gitcode.com/opengauss/oGMemory/blob/dev/deploy/user-quickstart/README_zh.md)。

## 4. Plugin：修改 Agent 接入参数

以 OpenClaw 为例：

```bash
ogmem onboard --mode plugin --agent openclaw
```

向导生成 `ogmem-client-config/openclaw/openclaw.config.patch.json` 和 `README.md`。patch 是需要合并的配置片段，不是完整配置；按 README 合并到 OpenClaw 实际使用的 `openclaw.json`，保留原有模型、Gateway 和其他插件设置。

以下字段位于 `plugins` → `entries` → `og-memory-context-engine` → `config`：

| 字段 | 用途 |
| --- | --- |
| `mode` | 使用 `remote` 连接已部署服务 |
| `memoryApiBaseUrl` | Agent 能访问的 HTTP 基础地址，不加 `/api/v1` |
| `authApiKey`、`authAccountId` | 服务认证 Key 和对应账号；不是模型 API Key |
| `accountId`、`userId`、`agentId` | 记忆所属身份；写入与检索需使用对应身份 |
| `autoCapture` | 默认 `true`，控制自动捕获 |
| `autoRecall` | 默认 `true`，控制自动召回与上下文注入 |

正常使用时，`autoCapture` 和 `autoRecall` 保持默认 `true` 即可。

<details>
<summary>可选：评测问答（QA）阶段关闭自动捕获、保留召回</summary>

在上述 `config` 对象中添加或修改以下两个字段：

```json
{
  "autoCapture": false,
  "autoRecall": true
}
```

这是字段片段，不是完整 OpenClaw 配置。`autoCapture=false` 不是全局写入禁令：显式调用 compact 压缩会话、清理已有缓冲区等操作仍可能写入。

</details>

OpenClaw 的显式插件配置优先于对应环境变量。修改后重启 Gateway，再用相同运行用户和配置路径检查：

```bash
openclaw plugins list
openclaw config get plugins.slots.contextEngine
```

预期插件已启用，槽位为 `og-memory-context-engine`。其他 Agent 和通用 MCP 的配置格式不同，按各自导出的 README 操作；已有导出冲突时可指定新的 `--output-dir`。

## 完整参考

- [YAML 字段](https://atomgit.com/opengauss/oGMemory/blob/dev/config/ogmem.reference.yaml)
- [服务环境变量](https://atomgit.com/opengauss/oGMemory/blob/dev/docs/OGMEMORY_ENV.md)
- [OpenClaw 插件配置与环境变量](https://atomgit.com/opengauss/oGMemory/blob/dev/plugins/openclaw/ENV.md)
