# 历史参考：来源与完整性清单

归档日期：2026-09-12。这里收纳已找到的两份 Top-K 历史设计，以及直接配套的限制分析、全量代码解读。列裁剪详细设计保留在上一级原路径，未另外复制一份。

## 1. 原文来源

SHA-256 是复制时的来源文件哈希，用于确认原文版本，不是加上归档提示后的副本哈希。

| 归档全文 | 原始来源（保留原位） | 来源 SHA-256 |
|---|---|---|
| [openGauss_UNION_ALL下推DiskANN_TopK详细设计.md](openGauss_UNION_ALL下推DiskANN_TopK详细设计.md) | `/home/qiumc/data/code/cpp/公共文档/openGauss_UNION_ALL下推DiskANN_TopK详细设计.md` | `c90cbb3e640e80b5454789a9841ea5eaf06397b536ed44d313dd3044ee48edef` |
| [openGauss_UNION_ALL分支过滤与DiskANN_TopK协同下推设计.md](openGauss_UNION_ALL分支过滤与DiskANN_TopK协同下推设计.md) | `/home/qiumc/data/code/cpp/openGauss-server/opengauss_master_union_all_vector_topk_pushdown_design.md` | `be01c7befb520992e3042438d378c7ef73e62cee8f8c6d98973ce303d336f045` |
| [openGauss_UNION_ALL与DiskANN_TopK下推限制分析.md](openGauss_UNION_ALL与DiskANN_TopK下推限制分析.md) | `/home/qiumc/data/code/cpp/公共文档/openGauss_UNION_ALL与DiskANN_TopK下推限制分析.md` | `7e049d07fa2728b3bd2e0b1c031a33edd9591a1a157fd3343e09e66487f3edbc` |
| [openGauss_UNION_ALL与DiskANN_TopK完整代码解读.md](openGauss_UNION_ALL与DiskANN_TopK完整代码解读.md) | `/home/qiumc/data/code/cpp/公共文档/openGauss_UNION_ALL与DiskANN_TopK完整代码解读.md` | `f4500d5d04262d095835bb40f5e8a68fa54bc688a0fa6f584e38eb5281f6587e` |

## 2. 版本关系

- 两份详细设计的基准均记录为官方 `upstream/master` 的 `33f3e1485758e2d27287b608f89f5e2515451c30`；“分支过滤与 Top-K 协同下推”一份更进一步设计 WHERE 与局部 Top-K 的先后关系，不应被最初方案覆盖。
- 限制分析和全量代码解读基于旧提交 `6df94c2ccb`。其中 SRF、WITH TIES、容器边界等已知缺口，不能当成修复后仍然存在的结论。
- 当前列裁剪与 Top-K 的三阶段编排、两个 GUC 的职责、六类修复，以[当前协同设计说明](../UNION_ALL_LIMIT下推与列裁剪协同设计说明.md)及关联证据为准。
- 原文中的 configure/make、环境、端口、服务状态和行号是历史记录。实际构建/启停遵循工作区 AGENTS.md；本次归档没有验证那些历史命令。

## 3. 复制时做了什么

1. 四份来源文件均复制完整正文，没有只摘录目录、结论或关键段落。
2. 归档副本统一增加文首“历史全文归档”提示；正文历史结论未改写成当前结论。
3. 完整代码解读中的 14 个源码/测试相对链接，从公共文档根目录的层级调整为本目录的层级；原来的同级设计文档链接现在指向本目录另一份全文。
4. 来源文件未删除、未修改，也未用本副本覆盖源码仓中的跟踪文件。

完整性核验口径：移除新增文首提示，并把完整代码解读中的 `../../../../openGauss-server/` 链接还原为来源的 `../openGauss-server/` 后，应与相应来源文件逐字节相同。只有归档说明和链接层级不同，不涉及正文遗漏。

本轮已按上述口径核验：4/4 全文一致，4/4 来源哈希未改变。设计目录内全部 Markdown 本地链接，以及公共文档索引中本次新增/调整的专题链接均已核验可达；两个既有专题 ZIP 的哈希未改变。

## 4. 不在这里重复复制的资料

源码解读、测试、缺陷修复、迁移比对包在同一专题下已有归属，通过[设计总入口](../README.md)统一关联。旧特性 patch 与后续修复包也保持原路径及原哈希；本次没有重新打包。

单表 DiskANN 的标量过滤、自适应补搜、INCLUDE，以及 HybridRecall 的其他设计不是这两个特性的设计文件，不混入本历史清单。
