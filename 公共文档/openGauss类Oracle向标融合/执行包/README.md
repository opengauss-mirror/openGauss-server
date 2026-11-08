# DiskANN 向标融合执行包

本包用于把[权威设计](../DiskANN与UStore向标融合详细设计.md)实施为 AStore + DiskANN PRE/IN/POST，并约束同源仓迁移。共有 35 个任务、三个阶段门、10 份目标计划快照。无覆盖索引基线 `ba1e4ad2f9`；当前源码 `openGauss-vector-filter-p12`，阶段一 `415b78e6c9`、阶段二 `55c8fc61ae`，未 push。当前 Release 完整功能回归通过，G-P1/G-P2 已签收；25/35 凭据完成、无活动任务，第三阶段10项pending。相同二进制、数据、SQL 的 OFF/ON 正式对照全部结束：九组自然 AUTO、两种会话形式各四组同计划 gplan、Scan十二组和 Join四组等平均召回；Join低召回八组因质量无法匹配明确排除。四条典型SQL超过10×，500查询矩阵未普遍达到10×，强制模式劣化如实保留，不能把历史含覆盖索引报告算作新基线收益。最新断点和实际状态以[继续开发交接](继续开发交接.md)及 taskctl 为准；第三阶段未实施。

[两阶段开发与真实前后版本性能报告](../DiskANN与AStore前两阶段开发与性能测试报告.md)保留旧基线的9条典型SQL、20组/80个干净窗口、1439项Release回归和性能边界。其主基线 `8dea19a42a` 包含已移除的覆盖索引代码，所有数字仅属历史记录，不代表当前无覆盖索引基线的测试或收益。

最新报告与原始证据见 [无覆盖索引前两阶段开关对照性能报告](../DiskANN前两阶段开关对照性能报告.md)。50项最终审计与9项阶段二门通过；自有55450已正常fast停止，数据保留，Oracle/Milvus停止，swappiness恢复60。

最终汇报用文档：[前两阶段最终测试报告](../DiskANN前两阶段最终测试报告.md)，按环境、数据、SQL、模型、结果五部分组织；九组最终结果见§5.1，全部直接/gplan对照与平均召回矩阵均保留，原始SQL及来源哈希随报告归档。

唯一模型启动入口是 [MODEL_HANDOFF_PROMPT.md](MODEL_HANDOFF_PROMPT.md)。当前断点见[继续开发交接](继续开发交接.md)。权威顺序为：用户最新明确指示 → 设计文档 → 本包；发现不一致先据授权修正材料，不能选宽松说法继续。

## 包内材料

| 文件 | 用途 |
|---|---|
| [任务实施清单](任务实施清单.md) / [任务目录](specs/task_catalog.json) | 逐任务依赖、设计精读、源码入口、实施步骤、完成条件和验证编号；两份内容保持一致 |
| [阶段验收与证据规范](阶段验收与证据规范.md) | 三阶段范围、如何记录真实结果、如何完成任务 |
| [迁移前置检查](迁移前置检查.md) | 先确认目标 DiskANN 行为和存储类型，再制定方案 |
| [已知未闭合事项](已知未闭合事项.md) | PRE 空域、F-00、F-09、Oracle 对照、成本估计的责任任务 |
| [前两阶段提交整理记录](前两阶段提交整理记录.md) | 两个阶段提交与原 14 个提交的对应关系、备份分支、文件树一致性及原测试版本身份 |
| [设计基线](specs/design_baseline.json) / [计划快照](specs/plan_baselines.json) | 设计 SHA 与 §1.2 原始计划，防止包与设计脱节；快照不是实测输出 |
| [验证索引](specs/validation_cases.json) | 设计中的测试/OB/O 编号与验收原文 |
| [状态文件](state/task_state.json) | 仅由 taskctl 更新；已绑定隔离实施工作树和AStore，保留各任务实际凭据 |
| [SQL 说明](sql/README.md) | 隔离烟测数据和基础查询，不替代完整回归或 Oracle 固定数据 |
| `scripts/`、`templates/`、`evidence/` | 只读预检、完整性校验、状态工具、证据模板及包自身验证记录 |

## 现在即可执行的只读检查

```bash
P=/home/qiumc/data/code/cpp/公共文档/openGauss类Oracle向标融合/执行包
python3 "$P/scripts/validate_package.py"
python3 "$P/scripts/preflight.py" --source /home/qiumc/data/code/cpp/openGauss-vector-filter-p12 --storage astore
python3 "$P/scripts/taskctl.py" list-ready
python3 "$P/scripts/taskctl.py" show Q-01-D1
```

要求 Python 3.8+ 和 Git；状态写入锁使用 Linux `fcntl`。预检只读源码、Git、进程名和容器状态，不连库、不启动服务、不编译。符号存在不等于行为相同，实际二进制与表存储由 E-01 的新实测凭据证明。包校验通过只证明材料/依赖/证据文件完整，不证明设计缺口已解决或功能正确。

工具自测入口为 `python3 "$P/scripts/test_package_tools.py"`，只在临时目录测试状态与证据规则，不修改正式状态。跨机器时可用 `DISKANN_PACKAGE_TEST_SOURCE` 指向可只读访问的测试源码仓。交接本包时同时携带上级权威设计及 [Oracle 证据入口](Oracle证据入口.md) 引用的材料，并保持相对目录；单独复制执行包目录不能替代这些输入。

## 新目标首次实施时

本机已完成的目标不重复bind或从E-01重开；先读当前交接。下述初始化只用于尚未绑定的新目标状态目录。

先读交接入口和 E-01，完成只读预检并选定实际开发工作树（需要隔离时先建立隔离工作树），再绑定目标并开始任务。绑定的 storage 是待 E-01 实证确认的声明，不是自动验证结果：

```bash
python3 "$P/scripts/taskctl.py" bind --source /home/qiumc/data/code/cpp/openGauss-server --storage astore --mode baseline
python3 "$P/scripts/taskctl.py" start E-01 --actor implementer
python3 "$P/scripts/taskctl.py" template E-01
```

每次只保留一个活动任务，恢复上下文时先恢复它。完成证据后用 `complete --report` 关闭，再 `list-ready` 选择下一任务。仅阶段门 G-P1/G-P2/G-P3 完成才表示相应阶段交付；前两阶段不依赖 F-00/F-09。工具不运行任意 shell、SQL、构建或提交操作。

E-01 为确认基础行为而必需的隔离基线构建和最小测试，可在该任务内完成；不必等待 E-02。E-02 随后固化可复跑的构建/数据/回归入口，复用已确认的产物，避免因任务拆分重复全量构建。若绑定后确需换开发工作树，保留原状态，给新工作树使用独立状态目录重新核验，不迁走旧任务的完成凭据。

其他同源仓使用新的状态目录，例如先设置 `TARGET_STATE` 为本主题下的独立目录，再在**每条** taskctl/validate 命令中加 `--state-dir "$TARGET_STATE"`；bind 使用 `--mode migration`。不要复用源仓已完成状态。迁移 E-01 证据强制包含扫描行为、存储事实和迁移方案三个文件，后续阶段还须完成目标仓实测。

修改权威设计后，校验会提示基线过期。先核对任务、验证索引、计划快照及受影响证据，不得只改 SHA 以消除报警，也不得为适配包修改权威设计。若是 F-00 等任务内的设计细化，且任务定义、验收索引和计划快照保持不变，可用下面的命令记录影响评审后同步；受影响任务及其下游会重开，无关任务保留原始版本凭据：

```bash
# --reopen 后填写实际受影响且需重新实施/验收的任务；不涉及既有任务时省略该选项。
python3 "$P/scripts/taskctl.py" sync-design --note '填写具体设计变化、影响范围及未重开任务仍有效的依据' --reopen F-06
python3 "$P/scripts/validate_package.py"
```

当前活动任务自身按已授权范围补设计时可继续完成；如其依据或已做工作失效，也必须列入重开。该命令不批准红线变更，不接受改变计划快照、验收定义或任务目录；这些变化须按设计要求处理并单独维护包及状态映射。使用多个状态目录时，各目录分别审查和同步。**迁移到新目标仓仍须全新状态，不用 sync-design 继承源仓完成记录。**
