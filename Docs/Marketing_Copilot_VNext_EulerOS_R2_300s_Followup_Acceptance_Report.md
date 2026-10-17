# Marketing Copilot VNext EulerOS R2/P5500 300 秒客户端超时追加验收报告

## 结论

本报告是 `Marketing_Copilot_VNext_EulerOS_Targeted_Native_Crash_Verification` 的补充记录，不覆盖或改写历史 D7/R2 `SIGSEGV (-11)` 失败结论。

在用户追加授权下，测试工具 HTTP 客户端超时由 30 秒调整为 300 秒，并使用全新的隔离 Run 在 EulerOS 2.0 SP15 服务器上完成一次 R2/P5500 确认执行：

```text
R2_RESULT=PASS
R2_EXPECTED=5500
R2_ACCEPTED=5500
PROJECTION_SOCKET_WRITTEN=5500
PROJECTION_TAKEN=5500
PROJECTION_UNIQUE_EVENTS=5500
```

该结果是当前测试条件下的一次 bounded recovery indicator，不等同于生产就绪，也不证明历史 native crash 已从所有场景消除。

## 测试工具变更

原测试工具在 `urllib` 客户端中固定使用 30 秒超时，无法覆盖 `/api/vnext/idle?timeout=300` 的长轮询窗口。本次仅修改测试工具：

```text
CANARY_HTTP_TIMEOUT_SECONDS = 300.0
```

变更提交为 `a3ea4ab617c39bd3d0c5740c2396ef047f8ebc61`，正式应用业务代码和正式部署包未修改。配套单测验证实际传递给 urllib 的 timeout 为 300 秒。

## 环境与隔离

| 项目 | 值 |
|---|---|
| 服务器 | `10.202.245.70` |
| OS | EulerOS 2.0 SP15 x86_64 |
| 账号 | 非 root `paas` |
| 文件系统 | `/dev/sda2 ext4` |
| Run Root | `/home/paas/marketing-copilot-test/runs/vnext-targeted-native-r2-20261017-retest300` |
| Python | standalone CPython 3.11.5 |
| 网络边界 | loopback-only；未连接真实 SMSC 或生产服务 |
| 资源限制 | CPU `0,1,2,3`；`nice +10`；`ionice -c3`；RSS 2 GiB；Run 磁盘 5 GiB |

本次 Run 使用独立的 `HOME`、`TMPDIR`、`XDG_CACHE_HOME`、数据库、WAL、Spool、Journal、日志和诊断目录。历史 Run 未修改。

## 精确执行参数

```text
--gates r2
--authorize-p5500
--route-tps 80
--response-delay-ms 2
--workers 4
--partitions 64
--projection-queue 128
--max-inflight 32
--prefetch 16
--watchdog enabled
--cpu-list 0,1,2,3
--timeout-seconds 420
```

R2 合同为 `2500 + 2500 + 250 + 250 = 5500` 条。请求门禁与实际执行门禁均为 `R2`。

## 结果与资源审计

| 检查项 | 结果 |
|---|---|
| R2/P5500 | `PASS` |
| accepted / expected | `5500 / 5500` |
| Projection socket_written / taken / unique | `5500 / 5500 / 5500` |
| Perf guard | `PASS` |
| Guard exit code | `0` |
| Guard duration | `85.542 s` |
| 最大 aggregate RSS | `585,560,064 B` |
| 最大 Run 磁盘增量 | `0 B` |
| all recorded PIDs gone | `true` |
| post-run process audit | `PASS` |
| native crash faulthandler log | `0 B`；未发现本轮 SIGSEGV |

服务器测试包回归为 `4 passed`；本地针对性回归为 `16 passed, 1 skipped`，Python 语法检查通过，runtime contract、离线安装和 `pip check` 均通过。

## 离线包哈希

正式应用包和 standalone runtime 沿用已验证归档；本次变化仅在测试包：

```text
formal application package
bce924b870fc01e4096ceb126c19c3f3c25327a91a38cf00d6a2762b1eaa3c44

standalone Python runtime
11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97

test-only bundle: marketing-copilot-vnext-runtime-canary-test-20261017-vnext-euleros-targeted-r2-300s.tar.gz
f1d59bf5e6817c8915a754eca53d73a4cb8c8cbce175f666a8a191d35d5f6e9a

sanitized evidence: r2-evidence-sanitized-300s.tar.gz
39e918deb75f4b5659d20976c6f17816e0299389c3fb36168e7c8bec52521013
```

精简证据包仅保留 manifest、环境、参数、guard、阶段标记、诊断和汇总结果，不包含数据库、WAL、Spool、上传内容、凭据或完整运行日志；证据归档保留在本地 `test-artifacts/vnext-targeted-native-20261017-r3f7c2/` 和服务器当前 Run 的 `transfer/` 下，未纳入 Git。

## RCA 边界

本轮 300 秒客户端超时修复解决了测试工具的长轮询超时缺陷，并使本次完整 5500 条合同验收通过。它不能删除历史 D7/R2 约 34 秒处的 native crash 证据，也没有提供新的 native faulting frame。因此：

```text
历史 D7/R2 结论：保留
RCA_LEVEL：RCA_STRONGLY_INDICATED
本轮结论：R2_PASS_BOUNDED_RECOVERY_INDICATOR
生产放行：未授权
```
