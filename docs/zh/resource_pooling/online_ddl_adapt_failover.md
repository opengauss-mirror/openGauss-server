# openGauss支持在线failover

## 可获得性<a name="section15406143204715"></a>

本特性自openGauss 7.0.0版本开始引入，仅适用于资源池化架构。

## 特性简介<a name="section740615433477"></a>

本特性面向openGauss资源池化主写备读场景。触发failover后，参与本轮failover的资源池化旧备机实例需要清理本实例上可能阻塞在线reform的backend与部分辅助线程。这些旧备机线程所在实例可能是本轮升主的新主节点，也可能是未升主、继续作为备机的节点。此前存在以下问题：

1. 这些线程如果处在`ReadBuffer_common`、`LockBuffer`或段页式metadata页读/锁流程中，页面请求失败后可能进入DMS相关retry循环。
2. 由于 `ReadBuffer_common` 期间存在 `HOLD_INTERRUPTS()`/`RESUME_INTERRUPTS()` 保护，线程不能及时响应退出信号，从而导致 `FailoverCleanBackends` 长时间等待甚至超时。
3. backend线程如果携带页面pin、IO状态、锁、read hint等资源跨越reform，也可能影响页面回放和后续一致性。
4. 原本采用直接报错方式处理被阻塞线程，由于报错点位置不统一，pin、IO等资源清理不可控，存在较大隐患。

本设计将已识别的openGauss侧可退栈阻塞点从“直接在阻塞点`ERROR`”优化为“先按资源归属逐层清理，再退到安全边界`ERROR`”。

## 客户价值<a name="section13406743164715"></a>

在线failover场景下，本特性可让backend和相关辅助线程尽快退出，推动在线reform继续执行，避免failover被阻塞，或因failover超时导致新主节点openGauss-server进程退出。同时，本特性也能降低直接在阻塞点报错导致页面资源泄漏或状态残留的风险。

## 特性描述<a name="section16406154310471"></a>

在资源池化架构的failover场景下，backend和相关辅助线程会尽快退出，避免failover被阻塞，或因failover超时导致新主节点openGauss-server进程退出。

## 特性增强<a name="section1340684315478"></a>

本特性是在资源池化架构下对在线reform特性的增强。

## 特性约束<a name="section06531946143616"></a>

1. 本特性关注会阻塞在线reform推进的线程，包括普通backend、线程池worker、autovacuum worker、statement线程、job worker、job scheduler，以及可能进入页面读路径并影响failover清理等待口径的backend-list辅助线程。
2. 本特性只处理已分析清楚的openGauss侧阻塞点，包括普通data/index page页面读retry、`LockBuffer`请求页retry、普通页buffer replacement中`DmsReleaseOwner`失败后的retry、段页式segment head/target fork head/`seg_nblocks`/`seg_get_physical_location`等metadata页路径。
3. 本特性不改变DMS/MES内部调用语义，不处理已进入`dms_request_page`、`mes_alloc_room`等单次调用后长期不返回的问题。如果发生此类问题，将进入强制退出进程流程。
4. 对未识别的阻塞点或无法确认资源归属的路径，不强行逐层退栈，保留原有直接报错逻辑。若仍有线程无法退出并阻塞在线reform，`FailoverCleanBackends`超时后打印残留backend、关键辅助线程、全线程堆栈和LWLock信息，再强制退出进程，避免reform永久卡死。

## 依赖关系<a name="section8406643144716"></a>

本特性依赖reform特性。
