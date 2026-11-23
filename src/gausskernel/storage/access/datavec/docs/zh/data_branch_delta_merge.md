# 增量分支合并

## 一、 功能概述与适用场景

通过 `--incremental-oggit` 启用增量模式。该模式基于 WAL 逻辑解码生成的增量元数据比较和合并变更，不执行全量业务表扫描。适用于子分支向父分支提交开发、测试变更，以及子分支同步父分支的最新变更。

当前仅支持直接父子 timeline 之间的双向 diff/merge，不支持兄弟分支、非直接父子分支或任意 DAG 多路合并。合并方向根据祖先关系自动判断：

- `child_to_parent`：source 是子分支，target 是父分支。
- `parent_to_child`：source 是父分支，target 是子分支。

无论合并方向如何，`source` 始终表示变更来源，`target` 始终表示应用目标；`ours` 固定表示 target，`theirs` 固定表示 source。

## 二、 前置条件与启用方式

1. 两个分支位于同一 Tenant，且为直接父子 timeline。
2. 两端均有可连接的 running endpoint；存在多个 running endpoint 时，显式指定对应 endpoint。
3. 两端使用兼容的数据库，连接使用相同的 database 名和数据库用户。target 能通过 `postgres_fdw` 访问 source 元数据，并具有合并所需的对象访问和写入权限。
4. 两端均已启用 Oggit，worker 状态为 `active`。本次合并所需的增量日志必须完整可用。
5. 命令使用的数据库应与两端 worker 维护增量元数据的数据库一致。

在创建或启动 endpoint 时通过 `--enable-oggit` 启用 Oggit。以下为启动命令示例，适用于尚未启动的 endpoint；已运行的 endpoint 应按部署流程安排重启，使配置生效：

```bash
neon_local endpoint start --enable-oggit --oggit-database postgres <source_endpoint>
neon_local endpoint start --enable-oggit --oggit-database postgres <target_endpoint>
```

`--enable-oggit` 管理 `neon.oggit_enabled`，使 `OggitWorker` 随 endpoint 启动。`--oggit-database` 指定解码和维护元数据的数据库；省略时沿用 endpoint 上次保存的选择，没有历史选择时使用 `postgres`。显式指定 `postgres` 可以重置此前的数据库选择。

### 命令入口与部署模式

本文未特别标注的 `neon_local` 命令用于源码部署的本地栈。Docker 环境在部署目录的 `docker-compose/` 下使用 `bin/docker_local`，由 control plane 访问 endpoint。

| 操作 | 本地 `neon_local` | Docker `bin/docker_local` |
| --- | --- | --- |
| `branch diff/merge` 定位两端 | `--source-branch/--target-branch`，必要时显式指定 endpoint | 必须使用 `--source-endpoint/--target-endpoint`，不接受 source/target branch 参数 |
| 冲突查询和恢复命令定位目标 | 使用 `--target-branch` 或明确的 target endpoint | 建议明确使用 `--target-endpoint` |
| 数据库 | 显式指定 `--database`；冲突查询和恢复命令也必须指定 | 同样显式指定 `--database` |

Docker 增量合并示例中，`dev`、`main` 均是 endpoint ID，仍须对应直接父子 timeline：

```bash
bin/docker_local branch merge \
  --source-endpoint dev \
  --target-endpoint main \
  --source-schema public \
  --target-schema public \
  --database postgres \
  --incremental-oggit \
  --strategy manual
```

Docker 增量 diff 使用相同的 endpoint、schema、database 参数，将子命令改为 `branch diff` 并去掉 `--strategy`。增量模式要求 source/target schema 同名、两端均存在且连接用户具有 `USAGE` 权限；不支持内部 schema 或跨 schema 名称映射。

## 三、 工作机制与增量窗口

两端 `OggitWorker` 从 WAL 逻辑解码生成结构化事件，分别维护本地 `oggit` 元数据。target 通过 FDW 读取 source 的元数据，再进行差异分析和合并规划，而不是映射业务表执行全量比较。

主要内部元数据如下：

- `oggit.state`：分支身份、祖先关系、worker 状态和解码进度。
- `oggit.change_log`：DML 事件，包括行身份、旧值、新值和变化列。
- `oggit.object_change`：DDL、TRUNCATE、SEQUENCE 等对象事件。
- `oggit.merge_history`：合并方向、增量窗口、策略、状态和冲突数。
- `oggit.merge_conflict`：冲突详情及人工决议。
- `oggit.merge_write_barrier`：目标分支写屏障状态。

用户不应直接修改这些内部表，也不应手工绕过写屏障。

命令启动时记录 source 和 target 当前 LSN 作为本次窗口上界，并等待两端 `scanned_lsn` 追齐。这里冻结的是处理边界，不表示停止两端正常写入。没有历史成功合并时，从子分支 `branch_start_lsn` 对应的共同基线开始；存在成功合并时，从最近一次 `applied` 边界继续，并通过合并自身事件标记和提交边界避免重复处理。

窗口内同一行的多次变更会压缩为最终增量，再比较双方变更。因此增量合并不是把历史 SQL 逐条原样重放。合并应用在 target 的一个事务中执行。

## 四、 增量 diff 命令与结果

```bash
neon_local branch diff \
  --source-branch dev \
  --target-branch main \
  --database postgres \
  --incremental-oggit
```

该示例假设 `dev` 是 `main` 的直接子分支。交换 source 和 target 可以比较父分支同步到子分支的增量。

增量 diff 不修改业务数据，也不创建合并历史、冲突记录或写屏障。命令冻结窗口之后的新写入不进入本次结果；再次执行 diff 时会重新确定窗口。后续 merge 会独立确定窗口，不保证与之前 diff 的窗口完全相同。

行级差异类型包括：

- `theirs_only`：仅 source 存在该变更。
- `ours_only`：仅 target 存在该变更。
- `same_change`：两侧最终操作和值等价，无需重复应用。
- `mergeable`：双方变更可合并，例如对同一行不同列的修改。
- `row_conflict`：存在不能直接自动合并的行级冲突。
- `unsupported`：变更缺少稳定行身份或不支持安全处理。

对象级结果包括 `theirs_object`、`ours_object` 和 `object_conflict`。这些类型描述增量事件，不等同于全量 diff 的 `source_only`、`target_only` 或全量行内容。

## 五、 增量 merge 命令与策略

子分支合入父分支：

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --database postgres \
  --incremental-oggit \
  --strategy manual
```

父分支同步到子分支：

```bash
neon_local branch merge \
  --source-branch main \
  --target-branch dev \
  --database postgres \
  --incremental-oggit \
  --strategy manual
```

`--strategy` 默认为 `fail`，增量模式支持以下四种策略：

- `fail`：发现冲突时失败，执行阶段错误直接返回；不是人工恢复策略。
- `ours`：保留 target 的冲突变更，跳过 source 对应的冲突或不可自动处理项；其他可合并变更仍会应用。执行阶段仍可能出错。
- `theirs`：对普通行级冲突采用 source 变更；对象冲突、unsupported 或不兼容目标对象变更不能通过该策略强制覆盖，可能进入 `blocked`。
- `manual`：将需要人工处理的冲突记录下来并进入 `blocked`，等待决议。没有冲突时正常合并，不会仅因使用 `manual` 而阻塞。

`manual` 仅适用于增量模式。增量 merge 应关注返回的 `merge_id`、合并状态及冲突信息，不应将全量模式的插入、更新统计示例作为增量输出格式。

## 六、 DML、行身份与冲突

增量模式可应用 INSERT、UPDATE 和 DELETE：

- INSERT 向 target 插入 source 新增行。
- UPDATE 仅更新实际变化列，避免覆盖 source 未修改而 target 已修改的列。
- DELETE 按行身份删除目标行。与全量模式不同，增量模式能够传播 source 的显式删除事件，但不会仅因某行在 source 当前不存在就删除 target 中该行。
- 双方最终变更等价时不重复应用；双方均删除同一行也视为等价。

行身份支持范围：

- 主键：支持单列及复合主键，按键定位行。
- 唯一键：使用能稳定定位行的 replica identity 唯一索引；不能将任意 UNIQUE 约束都视为已经配置了可用行身份。
- `REPLICA IDENTITY FULL`：使用完整旧行保守匹配，只有 target 唯一匹配一行时才允许自动处理。匹配 0 行或多行时记录冲突。
- 无稳定行身份：记录 `unsupported_row`，不自动合并，`theirs` 也不能强制覆盖。

无主键表可根据实际数据约束配置行身份，例如：

```sql
CREATE TABLE public.accounts_by_code (
    account_code text NOT NULL,
    name text,
    UNIQUE (account_code)
);
ALTER TABLE public.accounts_by_code
    REPLICA IDENTITY USING INDEX accounts_by_code_account_code_key;
```

没有可用唯一键时，可考虑 `ALTER TABLE public.t REPLICA IDENTITY FULL`，但仍需满足唯一旧行匹配限制。

常见冲突包括 `same_column_update`（同一行同一列修改为不同值）、`delete_update`（一侧删除，另一侧更新或插入同一键）、`unsupported_row` 和旧行匹配数量异常。target 对同一表存在不兼容对象变更时，source 的 DML 也可能被阻断。

## 七、 DDL、TRUNCATE 与 Sequence

对象变更能否自动重放取决于安全分类、合并方向和双方是否修改同一对象。以下自动处理规则以变更仅存在于 source、且不存在同对象冲突为前提：

- `safe_additive`：如新增可空列、创建普通索引，两个方向均可自动重放。
- `requires_validation`：如新增约束、设置 NOT NULL，两个方向均先在保存点内校验，通过后重放。
- `semantic`：如重命名、类型变更，子合父需人工确认；父合子在受支持的对象类型及重放范围内可自动应用。
- `destructive`：如 DROP、TRUNCATE，父合子可自动重放，子合父进入对象冲突，需人工确认。
- `unsupported`：无法解析、安全重放或缺少重放 SQL 的事件，两个方向均进入对象冲突。

双方修改同一对象且变更不同，记录 `object_conflict`；等价对象变更不重复处理。上述规则不表示支持任意 DDL，仍受事件采集和数据库执行能力限制。

父子方向的主要差异如下，自动应用均以不存在对象冲突且数据库检查通过为前提：

| 变更情况 | 子分支合入父分支 | 父分支同步到子分支 |
| --- | --- | --- |
| `safe_additive` / `requires_validation` | 自动应用或校验后应用 | 自动应用或校验后应用 |
| source 独有的 `semantic` / `destructive` 对象变更 | 需要人工确认 | 在支持的重放范围内可自动应用 |
| 双方对同一对象作出不同变更 | `object_conflict`，需要处理对象冲突 | 同样需要处理对象冲突 |
| source 有行变更，target 对该表有不兼容对象变更 | 可能产生 `parent_object_change` | 同样可能产生 `parent_object_change` |

`parent_object_change` 表示“目标侧存在阻碍来源行变更应用的对象变化”，名称中的 parent 不代表它永远发生在父分支。例如 source 更新某表的行，而 target 已对该表作出不兼容 DDL 或 TRUNCATE 时，应检查 target 的对象变化与 source 行变更是否仍可兼容。

> 注意：父分支同步到子分支时，符合条件的 DROP、TRUNCATE 会影响目标对象或数据。执行前应查看增量差异并确认合并方向。

对于由 sequence 管理默认值的列，source 插入行应用到 target 时由目标端默认值生成新值，并维护本次合并内的键映射。后续涉及该键的 DML 根据数据库目录中的外键关系改写引用值。应用完成后修复 target sequence，使其不小于表内当前最大对应值。

键映射不等同于推导所有级联、副作用语义；复杂的级联操作、触发器和函数行为仍依赖目标数据库约束检查或冲突处理。sequence 修复失败会记录 sequence 范围的 `apply_error`。

## 八、 人工冲突处理、继续与终止

`blocked` 表示本次合并尚未完成，需要处理冲突或执行错误。此时：

- 仅 target 开启用户 DML/DDL 写屏障；source 仍可继续写入。
- source 后续新写入不属于本次已确定的窗口。
- 保留供恢复流程使用的 FDW 元数据访问对象，不应手工清理。
- 同一 target 不允许启动新的 active merge，应先完成或终止当前合并。

### 冲突范围与决议参数

`conflict_scope` 表示冲突范围，`conflict_type` 表示该范围内的具体原因，两者需要结合查看：

| conflict_scope | 含义与常见内容 |
| --- | --- |
| `row` | 行级冲突，如同列更新、删除与更新冲突或 `parent_object_change` |
| `ddl` | 表、列、索引、约束等对象事件，双方修改同一对象时可出现 `object_conflict` |
| `sequence` | sequence 对象事件或序列修复问题 |
| `truncate` | TRUNCATE 对象事件的冲突或执行问题 |
| `unsupported` | 暂不支持安全处理的事件或冲突 |

`apply_error` 是执行失败原因，可出现在不同 scope 中，不是另一个 scope。对象冲突应结合 `source_change_id`、来源 DDL、目标变更及 `reason` 分析；同表多条来源 DDL 分别决议，不共用一次对象级批准。

本地 `neon_local branch resolve` 要求在 `--resolution` 与 `--custom-sql` 中选择一个：两者都不提供会报错，同时提供会因互斥被拒绝。`--resolution` 没有默认值，可取 `ours`、`theirs`、`skip`；`--custom-sql` 保存 SQL，待 continue 时执行，resolve 本身不立即执行。

Docker `bin/docker_local branch resolve` 的当前入口行为不同：未给 `--custom-sql` 时，`--resolution` 默认是 `theirs`；给出非空 `--custom-sql` 时会提交自定义 SQL，优先于 resolution。为明确表达决议，两个入口都建议只选择一种方式并显式填写，不能将 Docker 的默认值套用到本地命令。

Docker 下的目标定位示例：

```bash
bin/docker_local branch resolve \
  --target-endpoint main \
  --merge-id <merge_id> \
  --conflict-id <conflict_id> \
  --database postgres \
  --resolution ours
```

Docker 下的 conflicts、merge-status、continue、abort 也使用对应的 `bin/docker_local branch` 子命令，并明确传入 `--target-endpoint`、`--merge-id`、`--database`。

以下示例中的 `<merge_id>` 和 `<conflict_id>` 应替换为实际命令返回的标识。

查看冲突：

```bash
neon_local branch conflicts --target-branch main --merge-id <merge_id> --database postgres
```

逐项选择决议：`ours` 保留目标变更，`theirs` 采用来源变更，`skip` 跳过对应项。以下示例选择 `ours`，其他决议按实际需要替换：

```bash
neon_local branch resolve \
  --target-branch main \
  --merge-id <merge_id> \
  --conflict-id <conflict_id> \
  --database postgres \
  --resolution ours
```

需要自定义处理时，通过 `--custom-sql` 提供 SQL，而不是在普通会话中绕过目标写屏障：

```bash
neon_local branch resolve \
  --target-branch main \
  --merge-id <merge_id> \
  --conflict-id <conflict_id> \
  --database postgres \
  --custom-sql 'UPDATE public.items SET price = 22 WHERE id = 2;'
```

该 SQL 仅适用于确实需要将示例冲突行价格设为 22 的情况；使用前应核对对象、行和业务含义。人工决议不会使不受支持的变更自动变为可安全重放，实际应用仍需通过数据库检查。

处理完所有冲突后继续：

```bash
neon_local branch continue --target-branch main --merge-id <merge_id> --database postgres
```

`continue` 按原窗口重新构造执行计划，不纳入后续 source 写入。如果检测到 target 出现窗口外用户变更，会拒绝继续；此时需要终止本次合并并重新规划。约束、唯一键、外键、对象缺失或 SQL 执行错误可能在实际应用阶段产生 `apply_error`，需根据冲突原因重新处理，不能假定 resolve 后一定成功。

放弃本次合并：

```bash
neon_local branch abort --target-branch main --merge-id <merge_id> --database postgres
```

`abort` 将合并状态置为 `aborted` 并关闭 target 写屏障，不删除历史冲突记录，也不回滚 source 后续写入。它不是撤销已成功合并数据的命令。

### 对象级冲突处置示例

假设子分支对 `public.t_uk` 先修改列类型、再删除另一列，父分支同时给该表新增一列。以 `manual` 将子合父后，`branch conflicts` 应分别列出两条来源 DDL 的冲突。核对每个 `conflict_id`、`source_change_id` 和实际 SQL 后，再逐项决定保留目标、采用来源或使用自定义 SQL。

以下示例仅适用于已确认需要在 target 执行这两条来源 DDL 的情况；`<type_conflict_id>`、`<drop_conflict_id>` 使用查询结果中的实际 ID：

```bash
neon_local branch resolve \
  --target-branch main --merge-id <merge_id> \
  --conflict-id <type_conflict_id> --database postgres --resolution theirs

neon_local branch resolve \
  --target-branch main --merge-id <merge_id> \
  --conflict-id <drop_conflict_id> --database postgres --resolution theirs

neon_local branch continue \
  --target-branch main --merge-id <merge_id> --database postgres
```

只解决第一条时，continue 应提示仍有 pending 冲突；不应将第一条决议自动用于第二条。对于旧版本遗留的 blocked 合并，continue 可能先补齐遗漏的事件冲突并返回 blocked，应重新查询并决议新增冲突，再继续。

需要改写某条来源操作时，可针对该冲突改用 `--custom-sql`。各事件按来源 LSN、ordinal、ID 的顺序构造执行计划；跳过前置 DDL 后，依赖它的后续 DDL 仍可能失败。continue 若返回 blocked，应查看新的 pending/`apply_error` 及 `reason`，修正决议或 SQL 后再重试；同次应用中已执行的事务内修改会随失败回滚。若无法确定安全处理方式，使用 abort 终止后重新规划，不手工删除冲突或绕过写屏障。

## 九、 示例：增量修改与人工决议

假设 `dev` 从 `main` 直接派生，分支点时双方 `public.items` 包含 `id = 2, name = 'banana', price = 20`，其中 `id` 为主键，`name`、`price` 分别为名称与价格列。

1. 在 `dev` 将 `id = 2` 的 `name` 改为 `banana-dev`，在 `main` 将该行 `price` 改为 25。执行本节增量 diff 后，这类不同列更新可判定为 `mergeable`。使用增量 merge 合入后，目标行同时保留 `name = 'banana-dev'` 和 `price = 25`。
2. 在上述成功合并之后，继续在 `dev` 将该行 `price` 改为 30，在 `main` 将其改为 35。再次执行增量 diff/merge 时，仅处理新的窗口；双方同列不同值会产生行级冲突。
3. 使用 `--strategy manual` 合并进入 `blocked`，记录返回的 `merge_id`，执行 `branch conflicts` 查看该行冲突。
4. 使用该冲突的实际 `conflict_id` 执行 `branch resolve --resolution theirs`，然后执行 `branch continue`。在没有其他冲突和执行错误的情况下，目标行 `price` 更新为 30。
5. 如果不希望应用这次变更，应在继续前执行 `branch abort`，结束本次合并并解除 target 写屏障。继续成功和终止是两种替代路径，不应将 abort 当作成功合并后的回滚步骤。

父分支同步到子分支时，将命令中的 source 和 target 交换，并将冲突处理命令的 `--target-branch` 改为 `dev`。

## 十、 运行维护与限制说明

增量模式依赖连续、完整的事件元数据和可恢复的逻辑解码进度。可通过 `oggit.state` 及 endpoint 日志观察 worker 状态和进度：`scanned_lsn` 表示扫描边界，`decode_lsn` 表示已解码事件提交边界，`required_lsn` 用于保护重启所需 WAL。worker 长期失败可能增加 WAL 保留量，应及时排查。

删除子分支后，系统异步回收 root parent 上不再需要的增量事件，按当前存活子分支重新计算安全边界。不要手工删除 `oggit` 历史记录，以免破坏后续 diff/merge 所需窗口。

[全量分支合并](./data_branch_full_merge.md)第七节的“目标表必须有主键”“不删除目标侧独有行或表”和源独有表复制对象清单仅描述全量模式。增量模式按本节行身份、显式删除事件及 DDL 安全分类处理，不应将全量限制直接套用到增量模式。未指定 `--incremental-oggit` 时仍执行默认全量模式。

### 手动回收增量元数据

`oggit gc` 回收 root parent 上已不需要的 `oggit.change_log`、`oggit.object_change` 事件。它不删除业务行，也不等同于物理 WAL 文件清理；执行前应确认后续合并所需的增量窗口仍完整。

```bash
# 本地栈：使用默认 tenant，也可增加 --tenant-id <tenant_id>
neon_local oggit gc --retention-lsn-distance 16777216

# Docker：在部署目录的 docker-compose/ 下执行
bin/docker_local oggit gc --retention-lsn-distance 16777216 -t 900
```

| 参数 | 说明 |
| --- | --- |
| `--retention-lsn-distance` | 无存活直接子分支时保留的 LSN 字节距离，默认 `16777216`，即 16 MiB |
| `--tenant-id` | 仅本地 `neon_local oggit gc` 支持；省略时使用默认 tenant |
| `-t/--timeout` | 仅 Docker GC 入口支持的请求超时，单位秒，默认 `900`；`-t` 不是 tenant 参数 |

Docker GC 由 control plane 枚举受管 tenant 的 root parent，不接受 `--tenant-id`。两种入口均使用对应 endpoint 的 Oggit 数据库选择，不另设 `--database`。

回收下界 `floor_lsn` 由系统计算：有存活直接子分支时取最小 `ancestor_lsn`；无子分支时取 `max(scanned_lsn, decode_lsn)` 减去保留距离，下限为 `0/0`。只清理 `commit_lsn` 严格小于下界的事件。下界为 `0/0`、缺少唯一可用的 running endpoint，或未完成合并仍需保留相关窗口等情况下会跳过。

检查输出中的 `tenant_id`、`timeline_id`、`status`、`deleted_change_log`、`deleted_object_change` 和 `skipped_reason`。本地输出的 `gc_lsn` 对应 Docker 返回的 `floor_lsn`；`skipped` 应结合原因判断，不表示已经完成回收。不要通过手工删除内部表记录代替 GC。

### 常见失败与排查入口

先区分“命令执行失败”和“已生成 merge_id 且状态为 blocked”。HTTP 500 / `branch merge failed` 是外层失败信息，应继续查看响应体、完整错误链和目标 endpoint 日志，不能仅据此判断为数据冲突。

| 现象或原因 | 检查与处理 |
| --- | --- |
| source/target schema 不存在或无权限 | 在两端核对 schema 名、database 和连接用户的 `USAGE` 权限；增量模式要求同名 schema。修正参数或权限后重新执行，不把无效 schema 当成空差异 |
| 两端不是直接父子 timeline | 核对 source/target 是否写成同一分支，以及实际 ancestor 关系；按直接父子关系选择目标 |
| TRUNCATE 合并失败 | 先核对方向：父合子在 source 独有且无冲突时可自动重放；子合父需要人工确认。再检查 conflict_scope、reason、数据库依赖和完整错误链；若报 `cannot be replayed automatically`，核对运行中的程序是否包含相应修复 |
| `object_conflict` / 同表多条 DDL | 按上述对象级示例逐事件查询、决议；确认每条事件都有对应处理，尤其是后续 DROP、类型变更等事件 |
| resolve 后 continue 仍失败或 blocked | resolve 只记录决议。重新查询 pending/`apply_error`，检查 SQL、唯一键、外键、缺失对象和前置 DDL 依赖；修正后继续，无法处理时 abort |
| target 在计划窗口后又发生用户变更 | 按提示终止当前合并，并基于最新窗口重新 diff/merge |

本地日志位于 `.neon/endpoints/<target_endpoint>/compute.log` 及其数据库日志目录。Docker 可查询 control plane 和目标 endpoint 日志，例如：

```bash
bin/docker_local logs docker_control_plane --tail 100
bin/docker_local logs <target_service> --tail 100
```

`<target_service>` 替换为目标 endpoint 实际的 Compose 服务名，可通过 endpoint 信息确认。复核状态时使用 `branch merge-status`、`branch conflicts`；所有恢复操作均使用原 merge_id、原 target 和原 database。只有状态为 `applied` 才表示合并成功，`abort` 也不用于撤销已经 applied 的合并。
