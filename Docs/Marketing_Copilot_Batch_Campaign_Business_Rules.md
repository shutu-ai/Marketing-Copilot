# Marketing Copilot 批量营销业务规则说明

| 项目 | 内容 |
|---|---|
| 文档状态 | Current（当前 VNext 正常流程） |
| 适用范围 | 批量营销活动、潜客清单、跨日发送、暂停/恢复、结束/重新启动 |
| 更新时间 | 2026-10-20 |
| 发送安全目标 | 同一批量活动不因跨日、暂停恢复或服务重启而重复发送已经写入 SMPP Socket 的消息 |

## 1. 核心结论

1. 批量活动的开始时间和结束时间跨越多天，不代表潜客清单每天完整重发一次。
2. 第一天已经达到 `socket_written` 的用户，后续日期不会再次发送。
3. 第一天尚未处理、尚未进入发送 spool 的批次，会在下一个满足发送条件的日期继续处理。
4. 已经进入 spool、但在业务日截止前尚未写入 Socket 的旧任务，不会在第二天自动重发；它们会按跨业务日安全规则截止，避免重复发送。
5. 潜客清单全部发送完成，不会立即把活动状态改为“已完成”。活动通常保持“进行中”，直到配置的 `end_time` 到达，或者运营人员手工结束/取消。
6. 当前文档以 VNext 正常模式为准。旧 Legacy 模式存在“每日重置批次、按允许发送日重复发送”的兼容行为，不能与当前 VNext 规则混用。
7. 活动结束后重新上传潜客清单再重启，不是断点续传，而是一次新的发送输入；新名单中的手机号（包括旧名单中已经发送过的手机号）可能再次发送。

## 2. 业务对象与统计口径

### 2.1 潜客清单用户

潜客清单中的一条有效 `msisdn` 记录视为一个业务发送对象。相同源文件、同一发送日期内的重复号码会在入队阶段去重。

### 2.2 已发送用户

当前 VNext 的“已发送”以消息事件已经写入 SMPP Socket 的 `socket_written` 证据为准，业务单位是消息/用户记录，不是 PDU 数量。

### 2.3 短信中心已接受

“短信中心已接受（PDU）”表示 SMSC 对 `submit_sm` 分片返回成功响应，统计单位是 PDU 分片。普通单段短信通常是一条消息对应一个 PDU；长短信或多段短信可能一条用户消息对应多个 PDU。

因此：

- “已发送用户数”不能直接与“短信中心已接受（PDU）”相加或比较；
- PDU 接受成功不等同于手机最终送达；
- 当前 VNext 不根据 `submit_sm_resp` 推断手机端已送达。

### 2.4 业务日

VNext 使用业务日作为发送截止边界。跨业务日后，旧业务日任务不能继续被分发，也不能通过新一天的运行自动重发。

## 3. 活动生效条件

批量活动只有同时满足以下条件，才会继续创建或处理发送任务：

1. 活动状态为“进行中”；
2. 当前时间不早于 `start_time`，且不晚于 `end_time`；
3. 当前星期在 `send_days` 中；如果 `send_days` 为空，则不限制星期；
4. 当前时间位于每日发送窗口 `send_start_time` 至 `send_end_time` 内；
5. 潜客清单、短信模板和 SMPP 路由配置有效。

星期值沿用前端约定：星期日为 `0`，星期一至星期六为 `1` 至 `6`。因此，“第二天继续”应理解为“下一个允许发送日继续”，不一定是自然日的第二天。

## 4. 当前 VNext 执行流程

```text
上传潜客清单
      |
      v
生成批次/Manifest
      |
      v
活动进入“进行中”
      |
      v
检查日期、星期和每日发送窗口
      |
      +-- 不满足 --> 保留未处理批次，等待下一个允许发送窗口
      |
      v
未处理批次进入 VNext spool
      |
      v
Worker 取得任务并完成 SMPP Socket 写入
      |
      v
记录 socket_written / PDU 响应 / 结果投影
```

活动调度器在 VNext 模式下只执行 VNext 的业务日 rollover，不执行 Legacy 的每日批次重置。因此，活动跨越多天时，已完成的批次不会被重新置为待处理。[activity_scheduler.py:159](../backend/services/activity_scheduler.py#L159)

## 5. 跨天发送规则

| 第一天的情况 | 第二个允许发送日的行为 | 是否重复第一天已发送用户 |
|---|---|---:|
| 全部用户已经 `socket_written` | 没有新的发送任务 | 否 |
| 仍有数据库中的 `pending` 批次尚未处理 | 继续处理剩余批次 | 否 |
| 任务已进入 spool，但尚未写 Socket，且业务日已经截止 | 旧任务截止/过期，不自动重建 | 否 |
| 活动当天不在 `send_days` | 不发送，等待下一个允许发送日 | 否 |
| 活动已超过 `end_time` | 活动不再继续处理 | 否 |

### 5.1 示例：第一天全部发送完成

潜客清单有 10,000 人，活动时间为周一至周三：

- 周一全部达到 `socket_written`：周二、周三不会再次发送这 10,000 人；
- 活动状态仍可能显示“进行中”；
- 到达活动 `end_time` 后，调度器才会自动将活动改为完成状态；
- 如果运营人员需要立即结束活动，应手工执行结束/完成操作。

### 5.2 示例：第一天只处理了一部分批次

潜客清单有 10,000 人，周一只完成了前 6,000 人的批次处理：

- 周二满足发送条件时，继续处理剩余未处理批次；
- 前 6,000 人不会因为周二重新运行而重复发送；
- 如果剩余任务已经进入 spool 但在周一业务日截止前没有写入 Socket，则不会在周二自动重发这些旧 spool 任务。

## 6. 活动状态与人工操作

“暂停/恢复”和“结束/重新启动”是两种不同的业务操作，不能混用。

### 6.1 暂停后恢复

适用于临时停止发送、之后还要继续当前活动的场景。

- 暂停时，已经写入 Socket 的记录保持发送证据，不会回滚或重发；
- 尚未写入 Socket 的 spool 记录保持安全状态，恢复后继续处理；
- 如果清单已经全部发送完成，恢复后不会产生新的发送任务；
- 暂停/恢复不改变 SMPP 路由的 TPS、窗口或连接数。

因此，临时停止应优先使用“暂停 → 恢复”。

### 6.2 结束活动后重新启动

批量活动页面中的“停止活动/结束活动”属于结束路径，不等同于暂停：

- 已经 `socket_written` 的用户不会重发；
- 结束时仍在 VNext spool 中的任务会在 Worker 观察到取消状态后被取消或截止；这个过程不是同步清空；
- 后续将活动状态改回“进行中”不会自动重建已经取消的旧 spool 任务；
- 仍处于数据库 `pending` 状态、尚未进入 spool 的批次，才可能在重新启动后继续处理；
- 如果 `end_time` 已经过去，即使状态改回“进行中”，也不会继续发送，必须先配置新的有效结束时间。

如果业务目标是“临时停一下再继续”，不要使用结束活动后重新启动。

### 6.3 结束后重传潜客清单再重启

这是“替换名单并发起新一轮触达”，不是“用新名单续传旧名单”：

- 活动处于“已结束”时上传文件，只会原子替换当前潜客清单并更新名单数量，不会自动入队或发送；
- 重启活动时，文件源签名变化通常会触发新的批次 Manifest，并将新 Manifest 设为当前活动 Manifest；旧 Manifest 的数据库批次不再作为当前批次处理；
- 新名单中的所有有效记录都会作为新的发送输入重新参与发送。即使手机号在旧名单中已经 `socket_written`，只要它仍在新名单中，也可能再次发送；当前没有跨名单、按手机号的历史去重；
- 即使重新上传的文件内容与旧名单相同，也不能按“恢复未完成任务”理解，业务上应视为新的触达；
- 上传新名单不会清除旧的发送证据、历史统计或已经写入的 spool 记录；
- 停止状态只会让 Worker 在处理旧 spool 记录时执行取消/截止。如果旧 spool 记录尚未被 Worker 观察并取消，就在重启前恢复为“进行中”，这些旧记录仍可能被发送。因此，当前流程不能保证“重传后只发送新名单、旧 spool 绝不再发”。

如果业务要求严格避免旧名单残留发送，应先确认该活动的旧 spool 已清空/过期，或增加旧 `campaign_generation` 的发送前隔离，再重启新名单。

### 6.4 取消或完成

取消/完成表示终止当前活动：

- 已经写入 Socket 的发送证据保留；
- 尚未安全发送的任务不应被当作可重发任务；
- 通过运营操作接口进入完成或取消状态后，不能按暂停活动的方式恢复；
- 需要再次营销时，应创建新的活动或明确上传新的名单版本，并由运营人员确认这是一次新的触达。

## 7. 活动何时进入完成状态

当前实现不会因为“潜客清单全部发送完”就自动完成活动。自动完成条件主要是：

- 活动仍处于“进行中”；并且
- 当前时间已经达到或超过活动的 `end_time`。

因此，“批次已发送完”和“活动已完成”是两个不同概念：

| 概念 | 含义 |
|---|---|
| 批次发送完成 | 当前批次没有待处理发送对象，或其发送结果已经形成 |
| 活动完成 | 活动生命周期已经结束，不再接受后续调度 |

如果需要在清单发送完后立即停止活动，应由运营人员手工结束；如果不手工结束，活动可以继续显示“进行中”，直到 `end_time` 到达。

## 8. 重复发送的明确边界

正常 VNext 流程不会因为以下操作重复发送已经写入 Socket 的用户：

- 活动跨越多个自然日；
- 活动从暂停恢复；
- 服务重启；
- VNext spool 业务日 rollover；
- 活动列表页重新打开或刷新。

以下情况会形成新的发送输入，运营人员应视为一次新的触达并重新确认：

- 重新上传潜客清单；
- 生成新的清单/Manifest；
- 新建或复制活动；
- 切换到 Legacy 流程；
- 人工修改数据库状态或绕过正常活动控制接口。

重新上传潜客清单后，即使活动 ID 不变，也要按新名单版本重新确认触达范围；当前实现不会把新名单自动限制为“旧名单未发送的手机号”。

旧 Legacy 流程的兼容逻辑会在每日没有统计记录时重置批次，并将 `task_date` 纳入去重键；在 Legacy 模式下，同一用户可能在每个允许发送日发送一次。该行为不代表当前 VNext 正常流程的业务规则。[batch_campaign_service.py:693](../backend/services/batch_campaign_service.py#L693)、[sms_task_service.py:682](../backend/services/sms_task_service.py#L682)

## 9. 运营操作建议

| 业务意图 | 推荐操作 | 不建议操作 |
|---|---|---|
| 临时暂停，稍后继续 | 暂停活动，完成确认后再恢复 | 结束活动后重新启动 |
| 第一天下发完后不再继续 | 手工结束/完成活动 | 反复暂停、重启验证 |
| 第一天下发未完，第二天继续 | 保持活动进行中，确保下一个允许发送日和窗口有效 | 重新上传同一份清单 |
| 确认是否已经发送 | 查看 `socket_written` 用户数和待处理批次 | 仅看 SMSC 接受 PDU 数 |
| 需要重新触达同一批用户 | 新建活动或明确上传新名单版本 | 直接修改数据库或重置旧批次 |

任何“重新启动”操作前，应先确认：

1. 活动当前状态是“暂停”还是“已结束”；
2. `end_time` 是否仍在未来；
3. 是否还有数据库 `pending` 批次；
4. 是否存在尚未写入 Socket 的旧 spool 任务；
5. 这次操作是否会构成新的营销触达。

## 10. 当前实现依据

- 正常启动使用 VNext 数据平面，Legacy 发送链仅在显式配置 Legacy 模式时启用：[backend/main.py:158](../backend/main.py#L158)
- VNext 调度器执行业务日 rollover，不执行 Legacy 每日批次重置：[backend/services/activity_scheduler.py:159](../backend/services/activity_scheduler.py#L159)
- 日期、允许星期和每日发送窗口的判断：[backend/services/sms_task_service.py:704](../backend/services/sms_task_service.py#L704)
- 暂停时未写 Socket 的记录保留，已写 Socket 的记录不回滚：[backend/vnext/worker.py:585](../backend/vnext/worker.py#L585)、[backend/vnext/worker.py:626](../backend/vnext/worker.py#L626)
- 结束/取消状态在 VNext 控制面映射为停止发送：[backend/services/vnext_service.py:1267](../backend/services/vnext_service.py#L1267)
- 重传名单仅在活动进行中时自动触发批次生成；结束状态上传不会自动发送：[backend/services/batch_campaign_service.py:1587](../backend/services/batch_campaign_service.py#L1587)、[backend/services/batch_campaign_service.py:1642](../backend/services/batch_campaign_service.py#L1642)
- 新名单按文件源签名生成并切换活动 Manifest：[backend/services/batch_campaign_service.py:934](../backend/services/batch_campaign_service.py#L934)、[backend/services/batch_campaign_service.py:1118](../backend/services/batch_campaign_service.py#L1118)
- VNext 事件去重基于事件 ID、来源、行序号和活动代次，不是跨名单手机号历史去重：[backend/vnext/ingress.py:46](../backend/vnext/ingress.py#L46)、[backend/services/vnext_service.py:314](../backend/services/vnext_service.py#L314)
- 结束后取消由 Worker 处理，恢复运行会清除取消状态：[backend/vnext/worker.py:118](../backend/vnext/worker.py#L118)、[backend/vnext/worker.py:625](../backend/vnext/worker.py#L625)
- 活动自动完成按 `end_time` 判断，而不是按清单是否发送完判断：[backend/services/p1_service.py:96](../backend/services/p1_service.py#L96)
- 批量页面的结束与重新启动入口：[frontend/src/views/campaign/BatchCampaign.vue:103](../frontend/src/views/campaign/BatchCampaign.vue#L103)、[frontend/src/views/campaign/BatchCampaign.vue:518](../frontend/src/views/campaign/BatchCampaign.vue#L518)

本说明描述的是当前代码中的 VNext 业务行为，不包含对发送速率、SMPP 窗口、连接池或 worker 并发参数的修改。
