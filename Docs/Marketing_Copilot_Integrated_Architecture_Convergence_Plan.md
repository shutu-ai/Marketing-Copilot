# Marketing Copilot S2/S3/S4 一体化架构收敛方案

状态：与 `Marketing_Copilot_S2_S3_S4_Integrated_With_Fast_SMSC_Simulator_Codex_Task.md` 同一实现批次维护。本文是 M1 架构审计和收敛基线，不是新的分拆任务书。

## 1. 结论摘要

本仓库保持单进程 owner、单 DuckDB owner、真实 SMPP TCP socket 和 durable result journal 的正确性边界。吞吐优化只能发生在有界队列、批量事务、物理 route 共享 TPS、每 connection PDU window 和可观测的调度点内。

本批次的主要收敛决策是：

1. SMSC simulator 分为 `fast` 和 `fault` 两个互斥运行模式。`fast` 默认 `simulator_tps=0`、response delay=0、DLR disabled，并使用有界并发 PDU handler 与异步批量审计；`fault` 保留拒绝、丢响应、断连、延迟、partial 和 DLR 注入。
2. 应用 `route_tps` 与模拟器 `simulator_tps` 是两个独立 scope。route TPS 仍是 physical route 的 shared submit_sm parts/s，上限不能乘以 pool size；模拟器不限速不等于应用 route 不限速。
3. Fair dispatch 的 group key 固定为 `(campaign_type, campaign_id, resolved_route_id)`。Claim 层和 ReadyPool/socket-admission 层分别按实际编码 parts 计费；ReadyPool 使用有界 parts-based DRR，gateway 在同一 connection generation 上按一个 bounded continuation/yield 发送 multipart。
4. 文件摄取继续使用短 DB claim、lease/token、file hash/dedupe 和 durable chunk checkpoint。CSV/TXT 使用可恢复 chunk reader；XLSX 在 pandas materialization 前经过有界内存 admission；SFTP 在独立轮询线程和 copy queue 中，不占用 SMS sender worker。
5. 结果路径保持 `Fast accumulator -> bounded WriteBuffer -> Journal -> DB event_id fence -> terminal marker`。启动顺序为 `Journal/WriteBuffer replay barrier -> ingestion workers -> SMS claim/socket admission`，replay、quarantine 或 credit failure 时 fail-closed。

## 2. 端到端守恒模型

```text
source file
  -> atomic inbox/register + file lease/token
  -> durable chunk/manifest checkpoint
  -> sms task claim + attempt/dispatch generation fence
  -> ReadyPool group DRR (parts cost)
  -> physical route shared TPS
  -> Slot shallow handoff + PDU window credit
  -> same-generation multipart continuation/yield
  -> real submit_sm socket write
  -> submit_sm_resp / unknown / partial conservative state
  -> Fast checkpoint or result event
  -> bounded WriteBuffer + Journal fsync
  -> DB event_id fence + terminal marker
```

必须同时满足：

- `planned_parts = socket submit_sm parts = response/part ledger parts`；任何编码结果与预留 cost 不一致时在 socket write 前拒绝。
- `socket_write_started` 后的连接错误不能自动整条重发；只能归类为 `delivery_unknown`，并通过 attempt/claim/sequence generation 进行恢复。
- Journal、WriteBuffer、Fast checkpoint、dispatch parent/child credit 任一 failure、overflow、replay 未完成或 quarantine 都会关闭新发送准入，但允许安全的已收结果排空。
- pause/cancel 只推进指定 campaign 的 generation/control barrier；不会 flush 或删除其他 campaign 的队列。

## 3. 瓶颈审计结果（M1）

| 链路阶段 | 原风险 | 收敛实现/观测 | 仍需门禁证明 |
|---|---|---|---|
| SMSC simulator | 每 PDU 串行 await、同步 pretty print/JSONL、全局人工节流污染应用 TPS | `backend/tools/smsc_simulator.py` fast/fault 分离；有界 handler、异步 batch audit、metrics snapshot | SIM-1..SIM-6、Windows/WSL 资源差异 |
| Task/Claim | 只按任务数会让 multipart 放大或让大 campaign 占窗口 | campaign/route group + actual parts DRR，reservation charged by parts/bytes/result | G3/G4 同 SHA |
| Physical socket | Slot FIFO + 一个 multipart 长循环会造成 socket-level starvation | ReadyPool parts DRR；同 generation bounded continuation/yield；actual write per-group telemetry | G4 真实 socket fairness |
| PDU window | task-wide window 无法准确限制 multipart | Fair 模式每 submit_sm PDU reservation/bind/release，disconnect generation retire | G3/G5 断连、late response |
| Result path | 单任务结果写放大、Journal outage 时内存/credit 漂移 | Fast bounded checkpoint、parent/child credit、batched Journal/DB transaction、terminal retry/quarantine | G1/G5 credit 守恒 |
| Realtime file | CSV 长文件、SFTP 或 whitelist 可能阻塞单一 worker | CSV chunk + checkpoint + backpressure；copy/SFTP 隔离；active whitelist index | S3 worker scaling |
| XLSX | `read_excel` 会一次 materialize，存在进程级内存尖峰 | 读取前 XLSX expansion estimate + bounded memory admission；超限回到 RESOURCE_LIMIT retry | 实测内存曲线 |
| Restart | 新 worker 可能先于 Journal replay 取得新工作 | startup replay barrier；old sender/worker 未完全停止则拒绝新 epoch | G5、G6 600s |

## 4. 关键状态与证据字段

每次验收 manifest 至少记录：

- `commit_sha`、运行环境、数据库/Journal/Watch/port 独立路径；
- `simulator_mode`、`simulator_tps`、`simulator_response_delay`、`route_tps`、`tps_scope`、`delivery_receipt_mode`；
- client submit/response parts/s 与 simulator received/response parts/s 分列；
- `pool/window`、PDU used/limit、DRR served parts、oldest eligible age、eligible→actual socket write wait P50/P95/P99/max、continuation yields；
- task key、attempt/part、submit response、Journal event/terminal、chunk/file、unknown/quarantine 的守恒对账；
- CPU/RSS、event-loop lag、DB transaction/lock、Journal fsync、WriteBuffer retry/debt 和 Fast checkpoint backlog。

Fast simulator audit 不保存原始号码和正文；使用 source/destination digest、编码、concat/flash 元数据和计数进行完整一致性核对。Fault/full audit 才保留必要的协议明细。审计队列溢出、写失败或 shutdown backlog 不得静默丢弃，直接标记 `TEST_HARNESS_FAIL/SIMULATOR_AUDIT_INCOMPLETE`。

## 5. 验收映射

- G-SIM：独立 benchmark tool 的 simulator-only load、速率对照、encoding/protocol、日志 A/B 和余量证明。
- G0/G1：本地回归与四组发送/结果守恒，独立验证 DB、Journal、Fast/可靠状态和 unknown/partial 保守语义。
- G3：最终相同 SHA 的 4 组合 × 3 次，每次 1K；性能条件为 fast simulator、simulator_tps=0、route_tps=200、DLR off。
- G4：真实 loopback socket 的 A=50K、B/C=1K 多 campaign fairness，观察 per-group parts write、wait、max gap、continuation；故障 ACK/断连另列 fault 子场景。
- G5：fault mode 慢响应、拒绝、丢响应、断连、restart/replay、Journal/DB outage、credit 守恒和 unknown/partial。
- G6：有效进展定义为 durable task/part/result/checkpoint/terminal 中至少一项在滚动窗口内推进，最长停滞必须可归因并有恢复证据；600 秒未达标不得宣称长稳通过。

任何门禁没有在本批次最终 SHA、独立运行目录和原始证据上完成，只能记为 `NOT_RUN`、`FAIL` 或 `ENV_BLOCKED`，不得借用旧 S2/S2.1 结果拼成当前通过。
