# MES灵衢内存语义通信优化

## 可获得性<a name="section15406143204715"></a>

本特性在 openGauss 资源池化架构中引入，仅适用于资源池化架构；集群各节点需具备灵衢超节点（UB）能力、UBS Memory 服务、ubs-atomic开源库运行环境。

## 特性简介<a name="section740615433477"></a>

openGauss 在 MES（Message Exchange Service）通信层新增 SHM（Shared Memory）传输模式，作为与 TCP、RDMA/UBC 并列的第三种节点间通信方式。SHM 模式基于灵衢超节点 UBS Memory 服务与 ub_dist_comm_queue （ubs-atomic库能力）分布式通信队列，在节点间建立共享内存环形缓冲区，实现零拷贝消息收发；互连类型、队列 CPU 绑核及传输性能统计均可通过 GUC 参数配置。

## 客户价值<a name="section13406743164715"></a>

- 为资源池化场景下 MES 节点间通信提供基于 UB 共享内存的底层传输通道，支撑小包高频场景下的低时延跨节点消息交互。
- 避免协议栈上下文切换与多次内存拷贝；相比 URMA，在小包高频场景下协议开销更低、时延更优，可显著降低节点间消息传输时延并提升小包高频场景吞吐量。
- SHM 与既有 TCP、RDMA/UBC 模式互斥可选，不改变现有传输模式行为及 MES 消息协议；提供按队列维度的传输性能统计与 CPU 绑核能力，便于性能调优与问题定位。

## 特性描述<a name="section16406154310471"></a>

- 在 CBB MES 模块实现 SHM 传输模式：各节点通过 UBS Memory API 创建共享内存 region 与 9 个 ub_queue 队列的共享内存对象（优先级 0-7 及 prio6-mirror 队列），映射集群内其他节点共享内存，并基于 ub_dist_comm_queue 完成消息收发、连接管理与断连清理。
- 在 DMS 层新增 `DMS_CONN_MODE_SHM` 连接模式及 `DMS_CS_TYPE_SHM` 管道类型，将 SHM 队列 CPU 绑核等配置从 openGauss-server 透传至 MES。
- 在 openGauss-server 层扩展 `ss_interconnect_type` 支持 SHM 取值，新增 `ss_shm_ub_comm_cpu_bind`（9 个队列的 CPU 亲和性，格式 `[id,id,...]` 或 `[lo~hi]`）与 `ss_mes_elapsed_switch`（MES 耗时统计开关）；上述 GUC 为 PGC_POSTMASTER 级别，需重启生效。
- SHM 模式下消息接收由 ub_dist_comm_queue 内部回调线程驱动，不启动 MES 独立接收线程；优先级 6 的消息在 prio6、prio7、prio6-mirror 三个队列间轮转发送，以缓解高优先级队列压力。
- 支持按 ub_queue（0–8）维度的发送时延（avg/max/min）、发送计数与接收计数统计；开启 `ss_mes_elapsed_switch` 后，按约 10 秒窗口输出至运行日志（该开关默认关闭）。

## 特性增强<a name="section1340684315478"></a>

- MES 通信层新增 SHM 传输模式，与 TCP、RDMA/UBC 并列可选。
- 基于 UB 共享内存实现跨节点零拷贝消息收发。
- 支持 9 个 ub_queue 队列的独立共享内存对象与 ub_comm_queue 句柄，以及 per-queue CPU 绑核。
- 新增 SHM 传输性能统计与节点间共享内存映射重试、断连解映射等可靠性机制。

## 特性约束<a name="section06531946143616"></a>

- 仅在 `ss_interconnect_type` 配置为 SHM 时启用，与 TCP、RDMA/UBC 互斥，不影响其他模式。
- 依赖灵衢超节点 UBS Memory 服务与 ubs-atomic 动态库，运行时通过 dlopen 加载。
- `ss_shm_ub_comm_cpu_bind`、`ss_mes_elapsed_switch` 及互连类型相关配置为静态配置（PGC_POSTMASTER），不支持动态生效，修改后需重启集群节点。
- 共享内存映射对端失败时最多重试 60 次（间隔 1 秒），以容忍节点启动时序差异；对端共享内存未就绪前对应 pipe 不可用于收发。

## 依赖关系<a name="section8406643144716"></a>

- 依赖 openGauss 资源池化架构及 MES 节点间通信能力。
- 依赖灵衢超节点 UB 硬件、UBS Memory 服务与 ubs-atomic动态库。
- 涉及 CBB、DMS、openGauss-server 三个仓库的协同改造，无独立交付件。

## 基本原理<a name="section8406643144716"></a>

- **共享内存管理层**：基于 UBS Memory API 在各节点创建命名 region（`region_mes_<user>_<index>`）及 per-queue 共享内存对象（`shm_mes_<user>_<index>_<queue_idx>`），段内划分为 Boot 区域与 Ring 区域（128MiB 对齐），并映射本节点及对端节点共享内存至本地地址空间。
- **通信队列层**：基于 ub_dist_comm_queue 为每个优先级队列初始化环形缓冲区，注册 MES 命令回调；发送时调用 `ub_comm_queue_send` 将消息写入目标节点 Ring Buffer，接收由队列内部回调线程驱动并投递至 MES 消息处理流程。

## 使用指导<a name="section8406643144716"></a>

- 将互连类型配置为 SHM：设置 GUC 参数 `ss_interconnect_type = 'SHM'`，重启后生效。
- 可选配置 SHM 队列 CPU 亲和性：设置 `ss_shm_ub_comm_cpu_bind`，格式为 `[id,id,...]` 或 `[lo~hi]`，9 个槽位分别对应 9 个 ub_queue；未配置或解析失败时 `cpu_id = -1`（不绑核）。
- 可选开启传输性能统计：设置 `ss_mes_elapsed_switch = on`，重启后生效；运行日志中可查看各队列发送时延（avg/max/min）、发送次数与接收次数（约每 10 秒输出一次）。
- SHM 模式与 TCP、RDMA/UBC 互斥，启用前需确认集群各节点已部署并可用 UBS Memory 与 ub_dist_comm_queue 运行环境。

## 使用场景<a name="section8406643144716"></a>

- 资源池化集群中 MES 节点间小包高频消息通信场景，例如对跨节点消息时延敏感、通信频次较高的 DMS/MES 协同路径。
- 已部署灵衢超节点且希望以共享内存替代 TCP 或 URMA 作为 MES 底层互连的部署场景。
