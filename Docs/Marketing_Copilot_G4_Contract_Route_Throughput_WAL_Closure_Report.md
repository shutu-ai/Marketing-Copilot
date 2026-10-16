# Marketing Copilot — G4 合同、双 Route、吞吐与 WAL Closure Report

任务 ID：`MC-G4-CONTRACT-ROUTE-THROUGHPUT-WAL-CLOSURE-20261016`<br>
执行日期：2026-10-16<br>
最终状态：`INTEGRATED_NO_GO`

## 1. 结论先行

本次完成了合同矛盾审计、双 Route bind 的最小代码修复、当前源的 backend G0、双 Route 配置真实性 smoke，以及当前源 5500-task 的 OCC/serialized 隔离诊断。结果不能进入 S5：

- G4 合同仍有 52,000 与 102,000 两种活动映射，且 route TPS=200、G4 deadline=180 s 的数学下限已经超过 deadline；没有获得用户对合同的批准，`Docs/validation_contract.json` 未修改。
- 原双 Route 根因已定位并修复：`SmppGateway.connect()` 原来在第一条 enabled config 成功后立即返回。现在默认仍是 primary/backup，显式并行验收才会要求所有 enabled Route 完成真实 SMPP bind，并在任何必需 Route 不可用时 fail-closed。
- 当前 Windows 双 Route smoke 的 `config_audit` 在 `pool=1/window=1` 和 `pool=4/window=50` 均 PASS，两个 Route 均有 gateway connected 和 simulator bind events；但整体 runner 仍 FAIL，不能将配置审计扩大为 E2E 或吞吐 PASS。
- 当前源 5500-task 两轮均在严格 300 s 业务等待内未完成。OCC 记录 68 条 DuckDB OCC conflict retry，serialized 没有 OCC conflict 但记录了 database write-lock watchdog；两者均为严格 FAIL。历史 OCC WAL replay write-write conflict 也仍保留，未被本次结果覆盖。
- G0 在当前源、AlmaLinux-8 WSL native ext4、CPython 3.11.13 上为 `351 passed, 2 skipped`；前端 build PASS。G4 capacity、G4 observer、G4 native、G6 等上线门禁仍未闭合。

本报告是唯一主报告。没有执行 SLES12SP5 部署、生产切换、真实 SMSC 或真实号码测试；没有 commit 或 push。

## 2. 执行起点、工作区与冻结合同

### 2.1 Git 与工作区

执行前已 fetch 并核对：

```text
HEAD       = f5b72b8cc25e78bb5e7759b0f1ac358833187874
origin/main= f5b72b8cc25e78bb5e7759b0f1ac358833187874
```

执行前工作区已 dirty。既有用户内容包括 `AGENTS.md`、`frontend/package-lock.json`、历史 Docs/evidence、`.s0/.s1/.s11/.s2` 运行目录、cache、memory、数据库/WAL/临时目录以及其他未跟踪文件；均未 reset、clean、覆盖或删除。本次只增加/修改：

```text
M  backend/services/smpp_gateway.py
M  backend/tools/mixed_campaign_capacity_acceptance.py
?? backend/tests/test_smpp_gateway_routes.py
?? Docs/Marketing_Copilot_G4_Contract_Route_Throughput_WAL_Closure_Report.md
```

`AGENTS.md`、`frontend/package-lock.json` 和 `Docs/validation_contract.json` 均未由本次修改。

冻结合同文件当前 SHA256：

```text
Docs/validation_contract.json
66116ce46663c6bded40b6512f169f2701d7d364c9b055afe81fec79048a2c46
```

合同文件自身记录的历史 `baseline.git_head=95efa42491af5bb0aa0b08560bf293fd26a92553` 与本次实际 `HEAD/origin/main=f5b72b8c...` 不同；这属于需要在复核时显式处理的 source/contract baseline 差异，本次没有擅自改写合同。

### 2.2 被测源与 runner manifest

以下 SHA 是本次报告写入前从工作树计算的 SHA256；除 gateway、mixed runner 和新增测试外，其余为本次审计所用的未修改源。

```text
0d7c8b32fad9ebe98f6d1ee2640e99297f7bf7446ad093409cc68fed1da5aafc  backend/services/smpp_gateway.py
fbf8c09752cf975aec344199da091ee57648b3a5e7d6eaf7df0f65c3e85f0634  backend/services/task_worker.py
ce8579b1ff10442ddfd405866a3e4eb3e7985b7ae3ab306071f63253cd66bb32  backend/services/sms_task_service.py
084b321f3f61a02e646e502deb10a1852f090a348ae3d445618282777a6ff0af  backend/services/sms_write_buffer.py
c1ebb960bc35931b7cca21d620895ed866434884b1728e73fc203781793d1feb  backend/services/result_journal_service.py
967abc116fba6e80e494df99d2b151124256e32b9778a9921cd9bc677158da7a  backend/services/fast_send_service.py
60c3a566eaff49c71e5a901a113bafe256b860b769f1ccee6d7da8c3637b0c0e  backend/services/realtime_ingestion_worker.py
f11153f6c5468c160422771b489234ce743f0564dfdd7bb5992f18edbae665ce  backend/services/realtime_campaign_service.py
09956a91fba53ebd042428042943e8a60f669a2c921b8ea1192a26eb2ed62663  backend/services/activity_scheduler.py
1d7a4a984637bdf2860b6262c8a33814e31eb47dd07a8e3d265c6ca4d5c2e293  backend/tools/multi_campaign_fair_acceptance.py
baa2d87b2be117e0f813060f9d35b0b926fa6a971e3b15ebafadef13cc39768d  backend/tools/mixed_campaign_capacity_acceptance.py
41fe2fb18b37fe41ef1f7769cb4d6e7fbe2a1b11b06a1b0d30c3c1fd58a32b4c  backend/tools/local_e2e_acceptance.py
f38c2fa025fcdb3b0471fe5d4a47e1d20a25fe5c5f2f9a6d2cf7d8bb6ff2452f  backend/tests/test_smpp_gateway_routes.py
```

### 2.3 Formal environment

正式 backend G0 和 5500 诊断均使用 WSL `AlmaLinux-8` native ext4，不使用 `/mnt/d` 作为性能基线：

```text
Python: CPython 3.11.13
Filesystem: /dev/sdd ext4
df -T /tmp/mc-g4-20261016-codex/src/backend:
  /dev/sdd ext4 1055762868 71672288 930387108 8%
Full-G0 venv: /tmp/mc-venv-404d921b/bin/python
Full-G0 pip freeze SHA256: 966d47f160df6ce36765e4771291c42e345acfb997cff3236bf620b78b8a244c
App-only offline wheelhouse venv freeze SHA256: 847439f3d7d1169e87610f93f1ad7da493870c7efc4b35284af2cc6017b89686
DuckDB: 1.2.0
```

当前源副本、数据库、WAL、日志和测试临时目录位于 `/tmp/mc-g4-20261016-codex`。app-only venv 来自仓库已有的 `marketing-copilot-sles12sp5-wheelhouse-20261013-batch-upload-tolerance.tar.gz`；该 wheelhouse 没有 pytest/psutil，因此完整 G0 使用已有的正式 ext4 CPython 3.11 venv，不把测试工具误列为应用依赖。

## 3. `CONTRACT_DECISION_REQUIRED`

冻结合同中的关键矛盾如下。数学下限只用于判定合同可达性，不是实测 TPS：

| 项目 | 当前合同/runner | 单 Route、单 PDU 下限 | 待用户决定 |
|---|---:|---:|---|
| 命名 A/B/C | A=50,000，B=1,000，C=1,000，总计 52,000 | 52,000/200 = 260 s | 是否以 52K 作为正式容量映射 |
| 当前四活动 runner | realtime-A 50K + batch-A 50K + realtime-B 1K + batch-B 1K = 102,000 | 102,000/200 = 510 s | 是否接受 runner 的复制语义并以 102K 作为正式映射 |
| G4 deadline | 180 s | 两个下限均已超过 180 s | 180 s 是公平性观察窗口，还是全量完成 deadline |
| G6 长稳 | 有效窗口 600 s，历史有效运行约 631.248 s | 不替代 G4 deadline | 是否要求 600 s 内完成原容量目标 |
| 双 Route | 每物理 Route 独立 shared TPS=200 | 应计算 `max(PDU_i/200)` | 必须定义分流/归属；未有 bind 和分流证据前不得假设总 TPS=400 |

未获得用户批准。因此：

1. 保持 `Docs/validation_contract.json` 原样；
2. 不把缩小负载、延长诊断窗口或双 Route smoke 叫作正式 G4 capacity PASS；
3. 建议用户在下一轮合同版本中明确选择 52K 或 102K、180 s 的语义、双 Route 分配规则，并为全量完成期限单独设定可达值（期限必须再以真实 multipart/DB/摄取开销复核）。

当前 `G4_CAPACITY` 判定：`SPEC_INFEASIBLE_NEEDS_DECISION`。

## 4. 双 Route 根因与最小修复

### 4.1 根因

原 `SmppGateway.connect()` 按 priority 遍历 enabled configs，在第一条 `_connection_for(config)` 成功后设置 `_active_config_id` 并直接 `return`。因此主 Route 成功时，第二条 Route 只可能在后续定向发送或重连路径触发；“TCP connected”不能证明已经完成 SMPP bind。这与历史双 Route 配置真实性 FAIL 相符。

### 4.2 修复

`backend/services/smpp_gateway.py` 增加显式 `parallel`/`MC_SMPP_PARALLEL_BIND_REQUIRED` 分支：

- 默认值仍为 primary/backup：第一条可用 Route 成功后返回，故障时才尝试 backup；不强制 standby 同时 bind。
- 显式并行模式逐一创建每个 enabled Route 的目标 pool，复用现有 `_connection_for()` 和真实 `connection.connect()`，后者包含 SMPP bind 状态判定。
- 任一必需 Route 处于 cooldown、连接或 bind 失败，立即抛出 `Required parallel SMSC configurations unavailable`，不以第一条 Route 的成功掩盖缺失 Route。
- `mixed_campaign_capacity_acceptance.py` 仅在 `smsc_routes>1` 的显式多路验收中设置该开关；没有改变生产默认、TPS、window、credit、route affinity 或发送授权语义。

新增 `backend/tests/test_smpp_gateway_routes.py` 覆盖：

```text
default primary/backup: primary bind，backup 不被预先激活
explicit parallel: 两条 Route 和各自 pool 全部 bind
required Route failure: RuntimeError fail-closed，不能假 PASS
```

定向测试 `3 passed`；完整当前源 G0 也包含这 3 个测试。

### 4.3 双 Route 证据

以下是当前工作树 Windows loopback runner 的配置真实性结果。它们是诊断 smoke，不是正式 52K/102K G4：

| 证据 | Route 1 | Route 2 | config audit | 整体结果 |
|---|---|---|---|---|
| `Docs/g4-route-p1w1-20261016.json` | config 1，pool=1/window=1/TPS=1000，gateway 1/1，bind events=1 | config 2，pool=1/window=1/TPS=1000，gateway 1/1，bind events=1 | PASS，无 mismatch；`independent_shared_tps_per_physical_route` | FAIL |
| `Docs/g4-route-p4w50-20261016.json` | config 1，pool=4/window=50/TPS=1000，gateway 4/4，bind events=4 | config 2，pool=4/window=50/TPS=1000，gateway 4/4，bind events=4 | PASS，无 mismatch；`independent_shared_tps_per_physical_route` | FAIL |

两轮均使用 `time-budget=60`、`max-events=300`，整体不是正式容量测试。p1/p4 分别生成/接受 152 个任务并有 152 个 submit event，但 `mapped_submit_sm=0`，fairness/lock gate FAIL，诊断状态 `STALL_CAPTURED`，shutdown reconcile 约 30.271 s / 30.153 s。故此处只确认配置真实性修复有效，不宣称业务 E2E 已闭合。

Route 2 不可用的 fail-closed 单测已通过；没有改变任务 route affinity，也没有添加自动改投或 uncertain task 重发。

## 5. 1K 控制、5500 诊断与瓶颈归因

### 5.1 1K 控制与 G0

当前源 native ext4 G0 命令及结果：

```text
cd /tmp/mc-g4-20261016-codex/src/backend
PYTHONPATH=/tmp/mc-g4-20261016-codex/src/backend \
  /tmp/mc-venv-404d921b/bin/python -m pytest tests -q -p no:cacheprovider \
  --basetemp /tmp/mc-g4-20261016-codex/pytest-basetemp

351 passed, 2 skipped, 6 warnings, 240.38 s
```

历史 formal ext4 1K evidence 仍保留且不能被新失败覆盖：

- `/tmp/mc-final-closure-20261016-c/evidence/sig-target-occ-fast-1k-full-pool4-window50.json`：OCC+Fast full `PASS`，pool=4/window=50/TPS=200，4/4 bind。
- `/tmp/mc-final-closure-20261016-c/evidence/sender-only-stress1k-serialized-reliable-pool1-window1.json`：serialized/reliable sender-only `PASS`。

这些是历史控制/分解证据，不宣称已覆盖 G4 52K/102K，也不宣称 native 高并发风险已消失。

### 5.2 当前源 5500-task OCC

固定参数：`large_rows=2500`、`small_rows=250`、四活动总计 5500、`dispatch=fair`、`send=reliable`、`db-concurrency=occ`、`pool=4`、`window=50`、`route-tps=200`、simulator fast/audit fast、业务等待 300 s。独立证据：

```text
JSON: /tmp/mc-g4-20261016-codex/evidence-5500/multi-sig-occ-current.json
run : /tmp/mc-g4-20261016-codex/sig-runs-occ/marketing-copilot-multi_campaign_fairness-full-b0477f55
```

结果：`FAIL`，四活动在 300 s 内未全部 accepted；`expected=5500`、`generated=3510`、`submit_sm=3134`，`SMS_DISPATCH_FAIRNESS=FAIL`、`CROSS_TYPE_E2E=FAIL`。配置审计独立 PASS：pool=4、window=50、route TPS=200、gateway 4/4 connected、simulator bind events=4。以业务等待窗口粗算的 `3134/300≈10.45 submit/s` 仅是诊断比值，不是完成态正式吞吐。

当前 OCC 日志记录了 68 条 `DuckDB OCC conflict; retrying complete transaction`，涉及 batch manifest、realtime ingestion state/finish_file 和 batch complete；没有在本轮 JSON 中出现历史那种 WAL replay write-write conflict 文本，也没有 SIGSEGV/SIGABRT。故结论是“冲突压力和超时仍存在，特定 replay 冲突本轮未重现”，不是“WAL 已修复”。

observer 量化：`GET /api/metrics/live` 1,135 次、估算响应 3,943,655 bytes、累计请求时间约 19.604 s。它是有界 live counter 采样，但仍须在下一轮观察器 A/B 中确认是否对高负载有显著影响。

### 5.3 当前源 5500-task serialized 对照

参数与 OCC 完全相同，仅 `db-concurrency=serialized`，使用独立目录：

```text
JSON: /tmp/mc-g4-20261016-codex/evidence-5500/multi-sig-serialized-current.json
run : /tmp/mc-g4-20261016-codex/sig-runs-serialized/marketing-copilot-multi_campaign_fairness-full-e808a79a
```

结果：`FAIL`，`expected=5500`、`generated=3500`、`submit_sm=1829`，`SMS_DISPATCH_FAIRNESS=FAIL`、`CROSS_TYPE_E2E=FAIL`；配置审计 pool=4/window=50/TPS=200，gateway 4/4、bind events=4。业务等待窗口粗算 `1829/300≈6.10 submit/s`，同样只是诊断比值。

serialized 日志没有 OCC conflict、WAL replay error 或 native fault，但 database stall watchdog 捕获一次 `batch.process.complete` write lock。observer 的 `GET /api/metrics/live` 为 1,122 次、约 4,125,230 bytes、累计请求时间约 22.661 s。

### 5.4 归因边界

现有对照支持以下有限结论：

| 候选类别 | 证据 | 判定 |
|---|---|---|
| `APP_DB_TRANSACTION_OR_LOCK` | 当前 OCC 68 条 conflict retry；serialized write-lock watchdog；历史 OCC WAL replay conflict | 已证实有贡献，但具体冲突 owner/key 的完整修复尚未完成 |
| `APP_SCHEDULING_OR_SYNC_WAIT` | 两种 DB 模式都在相同 5500 目标下未完成，live backlog/blocked campaign 保留在 artifact | 可能共同作用，不能仅凭低整机 CPU 排除 |
| `HARNESS_OR_OBSERVER` | 当前 live counter 约 3.94/4.13 MB；Windows 双 Route smoke 有约 30 s reconcile 和 `mapped_submit_sm=0` | 对 smoke 有影响；不是 5500 业务失败的唯一已证实原因 |
| `SMPP_OR_ROUTE_LIMIT` | pool/window/TPS 配置均被审计，route shared TPS=200；但两路均 bind 的正式分流/完成态证据不足 | 约束存在，非单独根因结论 |
| `HOST_OR_WSL_RESOURCE_BOUND` | native ext4 可用空间充足；本轮未取得足以证明宿主资源饱和的证据 | 未证实 |
| `UNKNOWN` | G4 仍有 mapping/deadline 合同歧义，且当前业务未完成 | 保留 |

历史同参数 evidence 也仍保留：

- `/tmp/mc-final-closure-20261016-c/evidence/multi-sig-occ-pool4-window50-2500-250.json`：300 s 超时，并记录 `write-write conflict on key: "realtime, 1, 1"` 的 WAL replay error。
- `/tmp/mc-final-closure-20261016-c/evidence/multi-sig-serialized-pool4-window50-2500-250.json`：300 s 超时 control，没有把 timeout 误归因成 SIGSEGV。

本次没有对 OCC retry、WAL、unknown/partial、发送前 durable authorization 或 serialized 默认进行猜测性修改；因此没有把未确定根因包装成 closure。

## 6. 性能改动与保留决策

本次唯一生产代码改动是 Route admission 语义修复，不是吞吐调参。没有改动 `fast_send_service.py`、WriteBuffer、Result Journal、credit、hard-byte、checkpoint 或正式阈值。

`MC_FAST_SEND_FAIR_CHECKPOINT_MS` 默认仍为 `300 ms`。历史 300/500/1000 ms A/B 已表明 DB write/batch 数量下降不等于端到端吞吐提升，因此不提升默认值、不把可选 profile 的结果写成正式 PASS。

修复保留：

- 发送前 durable authorization、Journal fsync、attempt/part identity、unknown/partial、pause/cancel fence、route affinity、route-shared TPS、PDU window、fail-closed。
- serialized 默认与回滚能力。
- 所有 timeout、OCC conflict、WAL、SIGSEGV、observer 和 G6 失败 evidence。

## 7. Gate 状态矩阵

| Gate | 状态 | 证据与边界 |
|---|---|---|
| G0 | **PASS** | 当前源 native ext4 backend `351 passed/2 skipped`；frontend `npm run build` PASS，1794 modules，只有既有 Rolldown annotation/chunk warnings |
| G1 | **PASS（控制范围）** | 当前源 Route 单测 3/3；历史 ext4 1K OCC+Fast、serialized/reliable 控制 PASS；不外推到 G4 |
| G3 | **PASS（历史 matrix；当前未重跑全 12 组合）** | `.s2-evidence-20261015/g3/` 历史 formal evidence 保留；本次未冒充同一 SHA 的新 12/12 |
| G4_CONFIG | **PASS（当前 Windows 双 Route smoke）** | p1/w1 与 p4/w50 两条 Route 均 DB/gateway/bind 对账通过；WSL 当前双路 runner 在 API ready 前失败，未宣称 native full config gate |
| G4_OBSERVER | **FAIL/未闭合** | 当前 p1/p4 整体 `STALL_CAPTURED`，`mapped_submit_sm=0`；5500 有界 observer 数据已记录但尚未证明无显著 observer effect |
| G4_FAIRNESS | **FAIL** | 当前源 5500 OCC/serialized 均未完成，dispatch/cross-type FAIL |
| G4_CAPACITY | **SPEC_INFEASIBLE_NEEDS_DECISION** | 52K/102K 与 180 s 数学矛盾，用户未批准新合同；未执行正式 G4 capacity |
| G4_NATIVE | **FAIL** | 当前 5500 无 SIGSEGV，但两种 DB 模式 timeout；历史 OCC WAL conflict 和高并发失败仍未闭合 |
| G5 | **PASS（历史 fault/recovery 范围）** | 既有 reject/drop/partial/disconnect/restart/replay/pause/cancel evidence 保留；本次未把历史范围外推为 G4 closure |
| G6 | **FAIL** | `/tmp/mc-g6-ext4-safe-p4-w50.json`：600 s 目标运行有效约 631.248 s，expected 52,000，generated 11,270，accepted 5,283，严格容量失败，max RSS 约 382 MB |
| G-SIM | **PASS（历史 simulator contract）** | simulator 的 bind/multipart/fault/audit 历史 contract 保留；本次 WSL runner 可启动两个 simulator，但 API readiness 失败，不能当成完整 E2E PASS |

## 8. 新增与历史失败 evidence 索引

### 8.1 本次新增

```text
Windows route smoke:
  Docs/g4-route-p1w1-20261016.json
  Docs/g4-route-p4w50-20261016.json
  .g4-evidence-20261016-route-p1w1/
  .g4-evidence-20261016-route-p4w50/

Native ext4 setup and G0:
  /tmp/mc-g4-20261016-codex/pip-freeze-formal.txt
  /tmp/mc-g4-20261016-codex/pytest-basetemp/

Native ext4 two-route API-ready failure:
  /tmp/mc-g4-20261016-codex/mixed-fairness-p1w1.json
  /tmp/mc-g4-20261016-codex/mixed-fairness-p1w1.log
  /tmp/mc-g4-20261016-codex/run-fairness-p1w1/

Current-source 5500:
  /tmp/mc-g4-20261016-codex/evidence-5500/multi-sig-occ-current.json
  /tmp/mc-g4-20261016-codex/evidence-5500/multi-sig-occ-current.stdout.log
  /tmp/mc-g4-20261016-codex/sig-runs-occ/
  /tmp/mc-g4-20261016-codex/evidence-5500/multi-sig-serialized-current.json
  /tmp/mc-g4-20261016-codex/evidence-5500/multi-sig-serialized-current.stdout.log
  /tmp/mc-g4-20261016-codex/sig-runs-serialized/
```

这些运行产物只作为隔离诊断 evidence，不应加入部署包或提交；失败文件没有删除。

### 8.2 历史 evidence（不可覆盖）

```text
Docs/Marketing_Copilot_Native_Stability_Throughput_Final_Closure_Report.md
/tmp/mc-final-closure-20261016-c/evidence/multi-sig-occ-pool4-window50-2500-250.json
/tmp/mc-final-closure-20261016-c/evidence/multi-sig-serialized-pool4-window50-2500-250.json
/tmp/mc-final-closure-20261016-c/evidence/sig-target-occ-fast-1k-full-pool4-window50.json
/tmp/mc-final-closure-20261016-c/evidence/sender-only-stress1k-serialized-reliable-pool1-window1.json
/tmp/mc-g6-ext4-safe-p4-w50.json
.s2-evidence-20261015/g3/
.s0-evidence-20261015/、.s1-evidence-20261015/、.s11-evidence-20261015/
```

旧 SIGSEGV、WAL replay conflict、G6 fail、观察器过载和双 Route 失败均继续保留；本次没有用一个局部 PASS 覆盖它们。

## 9. 未闭环项目与最终判定

进入 S5 前仍需：

1. 用户明确 52K/102K、180 s 语义、G6 600 s 完成义务及双 Route 分流合同，并形成新冻结合同版本；
2. 在统一新合同下完成正式 G4 capacity/fairness/observer matrix，不用缩小 profile 代替；
3. 继续追踪当前 OCC conflict 与历史 WAL replay write-write conflict 的 owner/key/事务 fencing，验证 serialized 对照、unknown/partial 和无重复外发；
4. 关闭当前 5500 timeout、双 Route E2E mapping/observer 问题并保留完整 shutdown audit；
5. 重新完成原 G6 strict capacity；历史约 631 s / 5,283 accepted 的 FAIL 仍有效。

因此最终只能是：

```text
INTEGRATED_NO_GO
```

这是本次诊断/最小修复按范围完成后的真实 gate 状态，不是 S5 ready。当前没有 commit SHA，也没有 push；后续提交应只包含本任务的 gateway 修复、必要测试和本唯一主报告，不应包含 DB/WAL/Journal、cache、memory、日志或运行 evidence。
