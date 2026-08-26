# P3-7 HNSW 10 万行 Release 基准结果

## 范围与版本

- 日期：2026-08-26，机器时区 Asia/Shanghai。
- 本轮只验收 HNSW；DiskANN 不在需求范围内，1000 万行测试已取消，100 万行留待
  10 万行结果确认后再决定。
- 分支：`feature/hybrid-recall`，基线提交 `a8f2cb9fb1`。
- 最终安装与测试的 `gaussdb` 使用 `-O2 -g3`，源码和安装二进制 SHA-256 均为
  `264538d9d28cd25cd1ca2c6a66d224b4fcac9c7acf8a629a74397d92b3800ebb`。
- 安装仅执行 `src/gausskernel install-bin`，没有重建或重装整个 openGauss。

## 当前机器与数据

- WSL2，Linux `6.18.33.2-microsoft-standard-WSL2`，ext4 虚拟磁盘。
- Intel Core i9-12900H，20 个逻辑 CPU；内存 7.6 GiB，测试后可用约 5.5 GiB；
  swap 17 GiB，其中约 3.4 GiB 已使用。
- openGauss：`max_process_memory=4GB`、`shared_buffers=512MB`、`work_mem=64MB`。
- 表 `p37_hnsw_100k`：100,000 行，heap 15 MB，总大小 96 MB；HNSW 22 MB，
  BM25 56 MB。两个索引均为 `indisvalid=true/indisready=true`。
- HNSW 参数：`m=8`、`ef_construction=32`，查询 `hnsw_ef_search=1000`。

## 性能缺陷与修复

初版并行计划虽然启动了两个 LOCAL GATHER worker，但父 `HybridRecall` 先把文本 Stream
读到 EOF，再读取向量 Stream。每个本地 Stream 的有界 tuple buffer 填满后，未被消费的
向量 producer 会阻塞，实际墙钟时间接近串行。

修复后，本地 Stream 增加单次非阻塞 poll；并行 `HybridRecall` 在同一循环中轮询两个
输入，哪个就绪就消费哪个，两路 rank 仍分别递增。两路都暂时未就绪时休眠 50 微秒，
并在每轮检查中断。串行路径及普通单输入 Stream 的阻塞语义不变。`Fusion Time` 的起点
同步修正为两个支路较晚的结束时刻。

## 正确性与生命周期

`P3-7_hnsw_100k_validation.sql` 在最终 Release 二进制上得到：

- `text_candidates=1000`、`vector_candidates=1000` 时，串行/并行各返回 10 行，逐位置
  mismatch 为 0。
- 文本查询词完全不存在、文本支路零命中时，串行/并行仍各返回 10 行，mismatch 为 0，
  无 hang。
- 50 ms `statement_timeout` 能取消正在运行的两路查询；取消后实例立即执行 `SELECT 1`
  成功。稳定后 postmaster 线程数为 33，非 idle 用户会话为 0，其他会话已授予锁为 0。

代表性 `EXPLAIN ANALYZE`：

```text
Limit (actual time=834.138..834.141, rows=10)
  -> Hybrid Recall (actual time=834.134..834.137, rows=10)
       Strategy: Parallel RRF
       Planned Branch Workers: 2
       Actual Branch Workers: 2
       Parallel Fallback: false
       Text Candidate Rows: 1000
       Vector Candidate Rows: 1000
       Text Branch Time: 443.572
       Vector Branch Time: 820.542
       Fusion Time: 0.209
       Branch Overlap: 443.572
       -> LOCAL GATHER -> BM25 Index Scan
       -> LOCAL GATHER -> Ann Index Scan using p37_hnsw_100k_ann_idx
```

`overlap / min(text, vector)=1.00`，满足并行真实性门槛 `>=0.50`。

## 60 配对轮次结果

口径：同一 session 先执行 5 组串并 warm-up，再执行 60 组测量；奇数轮串行在先，偶数轮
并行在先。每次查询均为 `record`、tenant 1% 过滤、两路各 1000 candidates、RRF k=60、
权重 0.5/0.5、Top-10。120 次查询的结果集合只有 1 种，结果一致率 100%。

| 模式 | 次数 | Mean ms | Min ms | P50 ms | P95 ms | P99 ms | Max ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| Parallel | 60 | 664.623 | 526.102 | 642.882 | 803.964 | 1057.671 | 1088.065 |
| Serial | 60 | 939.727 | 756.406 | 899.084 | 1441.934 | 1641.037 | 1724.764 |

| 指标 | 降幅 |
|---|---:|
| P50 | 28.50% |
| P95 | 44.24% |
| P99 | 35.55% |

60 个配对轮次中并行 60 次全部获胜，平均节省 275.104 ms，逐轮降幅中位数 28.89%，
逐轮降幅 P05 为 19.38%。P95 满足“Parallel <= Serial × 0.75”的推荐通过线。

此前 30 配对校准轮次得到 P50/P95/P99 降幅 `30.51%/5.37%/20.11%`；其中 P95 只由约
两个长尾点决定。为避免选择性报告，本证据保留该结果，并把最终轮数提高到 60；最终
60 轮中 Parallel P95 为 803.964 ms，低于 30 轮中的 1212.459 ms，说明 WSL 调度长尾
确实会显著影响小样本 P95。交付结论以可复现的 60 配对脚本为准，生产机仍应重新标定。

## 可复现文件

- `P3-7_calibrate_100k.sql`：确定性 10 万行数据、BM25 与 HNSW 建表建索引。
- `P3-7_hnsw_100k_validation.sql`：结果、零文本支路、计划和 timeout 清理。
- `P3-7_hnsw_100k_benchmark.sql`：5 组 warm-up、60 配对轮次、分位数及逐轮统计。
