## 6. 文件扫描、入库和白名单资源公平

### 6.1 扫描公平与一致准入

扫描每活动一轮分配候选数、复制字节、elapsed 时间预算，活动集合轮转；建议初始 100 个候选或 100ms 元数据扫描预算，复制 IO 另受字节预算和超时控制。这些是试验值，不意味着完整目录枚举/排序本身能被该预算打断。

对于大目录，持有有界扫描游标/候选堆，分片 enumerate，并周期完整对账。目录事件仅作提示。文件可能迟到，禁止仅按最大文件名水位跳过较小文件。历史日期继续按文件名业务时间过滤，当前日旧小时是否过期仍按已有活动窗口规则。

本地/SFTP/手工上传统一使用原子容量 reservation。现有快照先查再注册不能作为全局严格配额，process_realtime_inboxes 重新注册也需要走同一准入入口。预留登记失败/复制失败释放额度，启动对账修复遗留 reservation。原始输入未成功入库前保留在源或可恢复位置。

SFTP 是独立 poller，但多个 SFTP campaign 仍串行，应限制每活动单轮下载量并用有界 IO 池隔离网络超时；预算同时限制总连接数和临时文件字节。单个活动扫描异常只记录该活动并继续后续轮次。

### 6.2 入库并发与大文件

继续共享 MC_REALTIME_INGEST_WORKERS，当前建议实验保持 4。活动争用时限制一个活动同时处理文件数；只有该活动有工作时允许借用空闲 worker，不把每个活动固定划分线程造成闲置。

文件轮询并不等于处理时间公平：四个大文件可占住全部 worker，小活动新文件只能等待。初期限制每活动并发；需要有界最大等待时，再加可恢复 chunk quantum。仅在持久检查点安全边界让出 worker，保存文件偏移/行序和去重状态，不能把已有 pandas iterator 随意丢弃后假设可无成本续读。

CSV 继续分块和现有批量去重；每 chunk 检查 token、活动状态、日期/窗口和局部背压。零匹配文件正常完成，不能将旧零命中观测用于跳过新文件。XLSX 全文件读取的内存成本单独限制，必要时后台转换为可流式内部格式，转换输出有校验/生命周期，不让大 XLSX 占满所有资源。

### 6.3 白名单构建

将 migrate/repair/build 从实时入库 _run_maintenance 移到专用维护/构建线程；默认全局构建并发保持 1，每活动最多一个版本构建。构建队列按活动轮询，连续失败版本有退避上限，避免总是选择最老 building 记录。

已就绪且未变化的索引继续复用；暂时主库写竞争不标为索引损坏。新版本成功后原子切换绑定。文件保留已固定 whitelist_version_id；旧索引清理需要确认没有文件/reader 引用，不能把正在使用的版本删除。长构建设置截止时间/进度；仅在内存、磁盘和查询竞争测试通过后提高到 2，并计入所有独立 DuckDB index 实例的总资源。
## 7. 局部背压、总容量与不会被“零写队列”掩盖的阻塞

按活动控制 pending 文件数、文件字节、待发任务数、待发分片、在途分片、结果日志未提交量和最老年龄。默认文件 pending=100 可保留，但 100 个小文件与 100 个大文件资源完全不同，应增加每活动字节/行数预算。

局部超限暂停该活动新 intake/任务创建，允许其已创建任务继续排空。其他活动有信用时继续处理。全局磁盘/内存/结果日志硬上限优先，必要时暂停全局新发送以保住恢复信息，但响应处理、租约必要控制和结果落库继续运行。持有写锁时不得 wait_for_ingestion。

全局 task_depth 必须拆解为 queued、ready_memory、socket_inflight、response_received_uncommitted、unknown，显示 durable/source timestamps。sending 很大不等于所有任务正在网络发送。write_depth 应包含队列、当前执行批次、延迟重试和磁盘日志待重放；当前从队列取走的大批次不可从“总未完成工作”中消失。

采用高/低水位滞回和周期主动更新；状态 API 展示 calculated_at、paused_reason、恢复条件。普通状态读取不能改变调度状态。所有低频统计以 1～5 秒聚合缓存供多个页面复用，避免每个 Web 刷新都全表 COUNT 多次。

按资源估算积压变化：
- 每活动任务队列净变化 = 任务创建速率 - 任务领取/终止速率。
- 结果积压净变化 = 已接收需持久化结果速率 - 持久化完成速率。
- 队列排空时间约为 backlog / 该活动实际服务能力；分片和任务单位必须一致。
- 当到达速率持续大于服务速率，任何有限队列最终都会满；需要限制准入或增加实际容量。

因此“writes_paused=false”“message_id=0”不能证明数据库不再是瓶颈，也不能证明系统一定在正常发送。
## 8. 短信状态与“不请求回执”的处理

disabled 只关闭 DLR 请求和无用途 message_id 映射；submit_sm_resp 仍需消费，用于释放窗口、明确拒绝、响应超时和状态统计。

推荐状态合同：
- queued/ready：尚未向 socket 发送；取消可直接终止。
- sending：带 attempt_token；可能含已发送片，必须查 attempt evidence 才能恢复。
- accepted：收到完整任务所有分片的成功 submit_sm_resp，表示短信中心接收，不代表用户送达。
- failed/retrying：明确拒绝或确认没有任何发送；临时拒绝按策略退避。
- delivery_unknown：部分发出、断链、超时或崩溃后无法证实接收状态，禁止自动跨中心重发。
- delivered：真实 DLR 或明确人工确认；disabled 模式不能凭本地 socket write 标记 delivered。

当前数据库在 disabled 成功时填 delivered_at，部分统计把 accepted 与 delivered 合并；兼容阶段先将 UI 标为“短信中心接收”，分别暴露 accepted_count/delivered_count。后续是否修正历史 delivered_at 需单独制定数据迁移策略，不依据旧时间猜真实送达。

stale recovery 不能只根据 sending 超过 5 分钟重发。必须先判断 attempt 日志、进程代际、活跃内存记录、已收响应待提交与队列租约；健康排队任务延续租约，已确认成功只补库，未知状态隔离，旧 attempt 的晚响应不能覆盖新 attempt 的状态。变更恢复策略前先补齐 token 条件更新与结果日志。

### 8.1 attempt fencing、分片恢复与回执隔离

claim_token 不能只在领取时写入。发送命令、分片上下文、响应/超时/失败回调以及日志必须携带同一 attempt_token；完成状态更新使用 WHERE task_id=? AND attempt_token=?，旧token记录进入审计但不得覆盖新attempt。领取令牌与发送attempt若分别建模，持久保存其关联；只靠当前status检查不足。

明确区分 queued、准备中、已向socket发出、部分接受、全部接受、明确拒绝和未知。任何分片已经接受后，不允许因另一片临时拒绝自动重发整条任务。优先保存part-level事实，只有能确定未接受的片且SMSC/业务重组语义验证通过时才允许分片重试；其余按 partial/unknown 隔离并请求业务处理。普通网络发送无法提供跨系统exactly-once。

sending租约由活跃attempt续租；恢复先核对内存发送上下文、持久intent、结果日志及消费游标。固定5分钟超时只能触发核查，不能独立成为重发依据。进程重启无法证明无message-id即未发送；disabled模式同样不能盲目重发。

DLR映射以来源命名空间关联，至少包含config_id与smsc_message_id；message-id在同一账号重用时，还要结合有效保留期、连接/中心epoch及可验证时间关联，遇到多个候选转为歧义事件，不以最新记录覆盖。回执入口必须携带来源config，迁移历史无来源记录时不能猜测绑定；旧表迁移和迟到回执采用兼容双读及明确回退边界。

DLR也写入独立持久事件日志。协议确认应在持久接收点之后；同步fsync可能拖慢读线程，应使用有界接收队列和顺序持久线程，由持久完成回调再发送deliver_sm_resp。容量不足停止新业务准入，并按对端允许的拒绝/重发策略处理；不能先确认后丢弃。持久确认延迟和SMSC重发行为需用模拟器/对端实测，重复DLR用幂等键去重。

### 8.2 flush与毒记录的可靠性合同

flush/barrier返回成功必须代表其之前已接收事件全部持久提交或明确进入可查询的隔离终态，而不是队列task_done计数归零。它携带接收序号水位和失败汇总；毒记录可以隔离让其他活动继续，但不得被算作成功写入。采用批次拆分定位毒记录时保持事务原子性及事件幂等，不重复追加业务事件/计数。失效attempt和歧义回执需保留恢复证据，按权限人工决策。

### 8.3 表结构及状态转换约束（拟新增/迁移）

| 对象 | 关键字段/唯一性 | 约束与责任 |
| --- | --- | --- |
| sms_tasks | task_id、current_attempt_token、status、recovery_state、lease_until | unknown/partial隔离由所有claim/retry入口统一排除；发送前验证owner和可发送状态 |
| sms_send_attempts | attempt_token主键；task_id、claim_token、content_hash、source_namespace、planned_parts、intent_lsn、state、lease_until | 同一task仅一个有效发送owner；持久intent完成后才socket输出 |
| sms_send_parts | (attempt_token,part_no)唯一；sequence、connection_generation、sent_at、resp_status、smsc_message_id、state | 已接受片不退回未发送；late/重复结果幂等且保留事实 |
| smpp_message_id_bindings | binding_id主键；source_namespace、message_id、attempt_token、part_no、有效时间 | 不以message-id单独唯一覆盖；保留重用候选，歧义DLR隔离 |
| sms_result_events/consumer_state | event_id唯一；stream/LSN；处理状态、隔离原因、连续水位 | 业务变更、事件幂等记录和settle水位同事务提交 |
| writer_commands | request_id/幂等键、状态、result、expires_at | Web超时查询真实结果；权限继承原业务操作 |

source_namespace是稳定的短信中心/账号命名空间，不随普通重连改变；config_id只有在禁止删除重建复用和审计配置变更时才可充当它。连接generation/epoch用于防旧socket回调，不能独自归属跨重连迟到DLR。来源账号配置改变采用新namespace或明确迁移。

条件更新同时校验token和合法前置状态，RETURNING命中后才做相应计数/事件；重复事件只读已有处理结果。queued→准备中→sending/partial→accepted/failed/unknown由attempt事实驱动，unknown/partial不直接回到queued。尚未socket发出的过期内存任务在发送前检查token/取消状态后安全撤销；已经发出的取消只阻止未发片，不伪装网络撤回。

submit结果信用与DLR信用分开：attempt预留其最坏part结果空间，持久消费后释放；DLR配置独立保留接收字节/事件和迟到窗口配额，重复/中间状态按有界去重处理。日志总体还有控制/重传保留区，满时停止新attempt，不能因尚有submit空间就无限接收DLR。

持久完成仅向连接所属有序控制发送器投递ACK命令，不由日志线程直接写socket。ACK包含连接generation和sequence，失效连接则不发送到新连接。确认延迟目标必须小于对端约定的响应时限并留余量，需先验证当前SMPP客户端允许延迟ACK、线程安全send及重传行为；若不支持，必须改客户端控制路径或明确接受的故障风险，不声称当前库天然支持。

