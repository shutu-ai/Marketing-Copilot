# 实时摄取恢复与 SMS TPS RCA

## 结论

本次审计确认并修复了实时文件摄取恢复路径中的四个问题：

1. 大 chunk 的请求大小会反向扩大 token bucket 的瞬时容量，绕过既定 burst 上限。
2. 压力导致速率下降后，旧的较大 permit 仍要求一次性满足，可能永久等待。
3. 每次 token 等待都重新打开 DuckDB 读连接；写锁竞争时，所有 ingestion worker 可能被读连接建立拖住。
4. 压力分数在一个大 chunk 的多个子批次之间重复衰减，正常的短写锁也会把 ramp 速率压到最小值。

修复后的实时 CSV/Excel 路径按实际 grant 分批写入，并在每个子批次事务中记录 chunk 进度；最终完成时合并持久化计数，避免重试或 duplicate 结果覆盖已提交的进度。

这证明了“摄取不会因 stale permit 或读锁放大而停滞”，但尚未证明 SMS 任务发送端能够在 4 ingestion workers / 8 task workers / 4 SMPP slots 下达到生产吞吐目标。当前 WSL 结果显示，任务创建完成后仍有独立的 task dispatch/submit 吞吐瓶颈，故不能把本次修复宣称为完整 SMS TPS 恢复。

## 证据边界

- 基线：`3d28dc47721f4be5e0fc98badad51ee655a6f9bb`。
- 验证环境：AlmaLinux-8 WSL、CPython 3.11.13、隔离 DuckDB、loopback-only SMSC。
- 依赖来源：`marketing-copilot-sles12sp5-system-python-20261013-flash-sms-regression.tar.gz` 内的 `offline/wheels` 与 `offline/requirements.txt`，安装包 SHA-256 为 `1213d8568df800a1f359dc499011dc571646a6884f94a2c70eda898e55c5d1f8`。
- 未连接生产数据库、生产 SMSC，也未使用真实 MSISDN、token 或生产短信内容。

## 已确认的问题

### 1. 大 chunk 绕过 burst 上限

旧逻辑按近似 `max(units, rate * burst_seconds)` 计算容量。请求 750、1500 或 5000 条时，`units` 会把 bucket 容量一起抬高，导致一次 admission 释放超过既定 burst 的写入量。

### 2. 速率下降后的 stale permit

旧逻辑先按较高速率计算大 request，随后压力检测把速率降到 25 TPS、容量降到 25，但等待条件仍要求原来的大 request。此时 token 上限永远无法满足，worker 会持续等待而不再缩小请求。

### 3. admission 等待路径放大 DuckDB 读竞争

`_ramp_admit_one()` 在每个 token 等待周期调用数据库深度刷新。写事务持锁时，读连接建立会把所有 ingestion worker 推入同一热点，形成“为缓解写压力而增加读压力”的反馈环。

### 4. 子批次压力采样过密

分批写入后，短写锁会在每个 25-unit 子批次上重复触发速率衰减。即使锁只短暂存在，速率也可能快速降到最小值，造成误判为持续高压。

## 最小修复

### `backend/services/backpressure_service.py`

- `_ramp_capacity_locked()` 只按当前速率和固定 burst 计算上限，与请求大小解耦。
- `_ramp_admit_one()` 每轮按当前容量重新计算 grant，并返回实际 grant；压力下降时不会等待过时的大 permit。
- `_ramp_admit()` 按实际 grant 循环，避免把大 chunk 当成不可分割事务。
- `admit_ingestion_batch()` 和 `ingestion_batch_size()` 暴露有界实际写入量。
- 文件/worker 边界负责刷新数据库深度；token 等待路径使用缓存深度和进程内 writer/journal 状态，不再每轮打开 DuckDB 读连接。
- 压力速率调整至少间隔 0.5 秒，只有压力分数大于 1 才减半；无压力时渐进恢复。
- 保留原有高低水位硬暂停、stop-aware 等待和共享 bucket。

### 实时写入与 checkpoint

- `backend/services/realtime_campaign_service.py` 的 `_enqueue_realtime_with_admission()` 统一处理 CSV/Excel 的实际 grant 和子批次写入。
- `backend/services/sms_task_service.py` 的 `enqueue_rows()` 在子批次事务中记录进度，在最终 chunk 完成时写最终计数。
- `backend/services/ingestion_job_service.py` 的 `record_chunk_progress_in_transaction()` 增量更新 chunk/file 计数；`complete_chunk_in_transaction()` 使用已持久化计数与最终计数的最大值合并。
- `backend/services/realtime_ingestion_state.py` 在更新前校验 file ownership、chunk lease 和状态，保留 fencing/dedupe 语义。

## 未改变的安全语义

- durable-before-wire、claim/attempt fencing、Journal 追加与恢复逻辑未被绕过。
- no-DLR 场景仍以 `submit_sm_resp=0` 为成功依据；reject、partial、断连和 missing response 不会被改写为成功。
- unknown 不会自动重发。
- 本修复未提高写锁超时、无限扩大 batch、绕过 Journal/attempt，也未增加 SMPP 连接数。
- WSL Flash 运行使用 `--flash-sms`；本次 3K/10K 样本为单 part，观测到 `data_coding=240`、`esm_class=0`，因此不能把它作为 multipart UDH 的完整覆盖证明。

## 验证结果摘要

- 重点测试：`27 passed`（backpressure hotpath、realtime progress 及相关兼容性测试）。
- 全量后端：`158 passed, 3 skipped`。
- 3K 当前实现：12/12 文件完成、3000/3000 task 创建、2418 次 `submit_sm` 且全部成功；无 duplicate、lost mapping、unknown submit id、misdelivery 或 DLR violation。600 秒窗口结束时仍有 `342 queued + 240 sending`，业务终态 gate 为 FAIL。
- 10K 当前实现：40/40 文件完成、10000/10000 task 创建、903 次 `submit_sm` 且全部成功；无 duplicate、lost mapping、unknown submit id、misdelivery 或 DLR violation。180 秒窗口结束时仍有 `8873 queued + 227 sending`，业务终态 gate 为 FAIL。
- 3K 基线：12/12 文件完成、3000/3000 task 创建、881 次 `submit_sm` 且全部成功；有 1 个 task 为 unknown，业务终态同样因 180 秒窗口超时。两个 3K 窗口不同，不能据此声称端到端吞吐提升。

## 仍待验证的假设

- 需要在相同观测窗口下完成高/低水位至少一次完整 HIGH → LOW → RESUME → HIGH 周期，并采集 5 分钟 steady-state。
- 需要补齐 DuckDB gate wait/hold 的 P95/P99 以及 timeout 增量样本。
- 需要在真实 SLES12SP5 主机上执行独立副本部署/升级；AlmaLinux-8 只证明了 wheelhouse 和应用运行时兼容性。
- 需要单独定位 task dispatch 的约 5 submit/s 瓶颈，不能将其归因于本次 ingestion ramp 修复。
