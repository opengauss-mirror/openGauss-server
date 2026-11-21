# oGMemory 代码检索

代码检索让 Agent 能"读懂"你的代码仓库：用一句话描述想找的功能，或给出函数名、关键词，就能直接拿到相关代码所在的文件、行号和代码片段，不必在大型仓库里反复搜索、逐个打开文件。

**适用场景：** 在陌生或大型代码仓中定位某个功能的实现、查找某个函数在哪里定义和被调用、让编程 Agent 更快找到需要修改的位置。

使用前，先按 [安装指南](ogmem_install.md) 启动 oGMemory 服务。整体只需三步：**开启功能 → 接入代码仓 → 开始检索**。

## 1. 开启功能

代码检索默认关闭。在服务使用的配置文件（Local 默认为 `config/ogmem.yaml`）中加入以下内容，然后重启服务：

```yaml
auth:
  role_control_enabled: true   # 接入代码仓需要启用权限控制
features:
  code_resources_enabled: true
  code_sync_enabled: true
  code_retrieval_enabled: true
  code_workspace_allowed_roots: ["/home/your_user/workspace"]   # 允许接入的代码目录
```

`code_workspace_allowed_roots` 用于限定服务可以读取哪些目录，只有位于这些目录下的代码仓才能接入。请填写服务所在机器上的绝对路径；不要填写 `/` 等过大的范围。

Docker 部署时，需把代码目录挂载进容器，并按 [部署变量参考](https://atomgit.com/opengauss/oGMemory/blob/dev/config/deploy.env.reference) 在 `ogmemory.env` 中设置对应变量，路径填写容器内的路径。

## 2. 接入代码仓

接入代码仓需要管理员身份，即 Root Key 或账户 Admin Key（不是模型的 API Key）。下面沿用安装指南中设置的环境变量，将 `CODE_ROOT` 改为你的代码仓路径：

```bash
export CODE_ROOT=/home/your_user/workspace/my-project
OGMEM_HEADERS=(-H 'Content-Type: application/json' -H "X-API-Key: ${OGMEM_SERVICE_KEY}"
  -H "X-Account-ID: ${OG_ACCOUNT_ID}" -H "X-User-ID: ${OG_USER_ID}")

# 第一步：登记代码仓，记下返回结果中的 root_id
curl -sS "${OGMEM_URL}/api/v1/resources/code/sync/registrations" "${OGMEM_HEADERS[@]}" \
  -d "{\"path\": \"${CODE_ROOT}\", \"name\": \"my-project\", \"trigger_mode\": \"manual\",
       \"watch_enabled\": false, \"agentId\": \"${OG_API_AGENT_ID}\"}"

# 第二步：导入代码并等待完成（大型仓库耗时较长）
export ROOT_ID='替换为上一步返回的root_id'
curl -sS --max-time 600 "${OGMEM_URL}/api/v1/resources/code/sync" "${OGMEM_HEADERS[@]}" \
  -d "{\"root_id\": \"${ROOT_ID}\", \"wait_mode\": \"index_visible\", \"agentId\": \"${OG_API_AGENT_ID}\"}"
```

返回结果中 `index_visible` 为 `true`，表示代码已导入完成，可以开始检索。

**代码更新后：** 再执行一次第二步即可让检索结果跟上最新代码。如果希望服务自动发现代码变化，登记时将 `trigger_mode` 改为 `hybrid`、`watch_enabled` 改为 `true`。

## 3. 开始检索

### 在 Agent 中使用（推荐）

Agent 通过 MCP 接入 oGMemory 后（接入方式见 [安装指南](ogmem_install.md) 的 Plugin 模式），会自动获得 `ogm_search_code` 工具。你只需用自然语言向 Agent 提问，例如：

- "订单总价是在哪里计算的？"
- "`calculate_total` 函数在哪里定义？"
- "在 `src/service` 目录里找处理登录超时的代码。"

Agent 会调用该工具找到相关代码，再基于结果回答或修改代码。提问时说明所在目录，结果会更准确。

### 通过 HTTP 接口调用

自研应用可以直接调用检索接口：

```bash
curl -sS "${OGMEM_URL}/api/v1/resources/code/search" "${OGMEM_HEADERS[@]}" \
  -d "{\"query\": \"订单总价在哪里计算\", \"scope\": {\"paths\": [\"src\"]},
       \"budget\": {\"result_limit\": 3}, \"agentId\": \"${OG_API_AGENT_ID}\"}"
```

- `query`：要找什么，用自然语言、函数名或关键词描述均可。
- `scope`（可选）：缩小查找范围。`paths` 指定目录或文件，`glob_patterns` 指定文件类型（如 `**/*.py`），`grep_terms` 要求文件中必须包含的词。
- `budget.result_limit`（可选）：最多返回几条结果。

返回的 `evidence` 列表中，每条结果给出代码所在的文件和行号（`locator`）以及代码片段。检索只需普通成员的 Key，不要求管理员身份。

## 4. 实现原理

代码检索分为"导入建索引"和"检索"两个阶段：

```text
导入：代码仓 → 切分代码单元 → 提取符号与关系 → 生成向量 → 存入 openGauss，并建立关键词索引
检索：问题 → 语义 / 关键词 / 符号 / 关系图 四路召回 → 范围过滤 → 加权融合排序 → 截取相关片段返回
```

**导入建索引**

- **切分代码：** Python 文件按语法树切分为函数、类等独立单元，并记录签名、导入和调用关系；其他语言（C/C++、Java、Go、JavaScript 等）按行切分为带少量重叠的代码块。
- **建立索引：** 每个代码单元连同文件路径、行号等信息生成向量，存入 openGauss 向量库；服务同时维护 BM25 关键词索引。
- **增量更新：** 再次同步时按文件内容哈希比对，只重新处理新增、修改的文件，并清理已删除文件的索引。

**检索**

| 召回方式 | 擅长的问题 |
| --- | --- |
| 语义检索 | 用自然语言描述功能，如"订单总价在哪里计算"，按向量相似度匹配 |
| 关键词检索（BM25） | 包含具体标识符、报错信息、配置项等字面内容的查询 |
| 符号检索 | 函数名、类名，精确定位定义位置 |
| 关系图检索 | "谁调用了某函数"等问题，沿调用、导入关系找到相关代码（关系目前来自 Python 代码解析） |

四路结果先按 `scope` 过滤，再用加权倒数排名融合（Weighted RRF）合并排序：被多路同时召回、排名靠前的代码得分更高。最后按问题截取最相关的代码段落，并限制返回条数与字数，避免占满 Agent 的上下文。

## 5. 常见问题

| 现象 | 处理方法 |
| --- | --- |
| 检索结果的 `status` 为 `disabled` | 功能未开启：检查第 1 步的配置，并确认已重启服务 |
| 登记代码仓失败 | 确认已启用权限控制、使用的是管理员 Key，且代码仓路径位于 `code_workspace_allowed_roots` 中 |
| 检索不到结果（`zero_result`） | 先去掉 `scope` 范围限制重试；再确认第 2 步返回了 `index_visible: true` |
| 结果不是最新代码 | 重新执行第 2 步的导入，或改为自动跟踪代码变化 |

更多接口见 [API 说明](ogmem_api.md)；高级用法（检索模式、代码编辑等）见 [oGMemory 特性说明](https://atomgit.com/opengauss/oGMemory/blob/dev/docs/features.md#代码理解与编辑)。
