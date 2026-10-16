# Marketing Copilot VNext 性能对照与阶段 C 报告

状态：VNEXT_PIPELINE_READY_FOR_CANARY（仅表示合成号码、loopback SMSC 的 VNext 门禁通过；不表示真实 SMSC 或生产发布通过）。

旧的历史集成结论 INTEGRATED_NO_GO 保持不变。本轮没有提交或推送。

## 1. 基线与交付物

- HEAD 与 origin/main：5650b8fc6bcdd620858258db3937cd5c5ad59d14
- 测试服务器：EulerOS 2.0 SP15 x86_64；本轮按约定作为 SLES12SP5 性能代理，不伪称原生 SLES。
- 服务器 Run：/home/paas/marketing-copilot-test/runs/vnext-20261016-b-184938/
- runtime：CPython 3.11.5 standalone；SHA256 11f9ec8dc1e957ef3ee14f3a3bfede048fba159578a039c34d78b6ab3f441d97
- app package：marketing-copilot-sles12sp5-system-python-20261016-vnext-dirty.tar.gz
- app SHA256：4229425bbf7af893d835ecddabafe677522eaf768088494b76703582c27d22fe
- 服务器证据已复制到：test-artifacts/vnext-server-b-20261016-b-184938/evidence/

服务器安装使用独立 runtime、Run 内 venv、pip --no-index；pip check、Python 版本记录和 backend 编译通过。没有使用 root、systemd、全局 pip、生产目录或外网依赖。

## 2. EulerOS 服务器 B 阶段

执行约束：4 worker、256 virtual partitions、CPU affinity 0-3、nice +10、ionice -c 3、真实 TCP loopback simulator。所有 acceptance 进程结束后均无残留。

| Gate | generated | socket_written | projection.applied | submit_sm_received | submit_sm_resp_success | pending | missing/duplicate | duration | 吞吐 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| P10 | 10 | 10 | 10 | 10 | 10 | 0 | 0/0 | 0.526s | 19.0/s |
| P100 | 100 | 100 | 100 | 100 | 100 | 0 | 0/0 | 0.803s | 124.5/s |
| P1K | 1000 | 1000 | 1000 | 1000 | 1000 | 0 | 0/0 | 3.925s | 254.8/s |
| P5500 | 5500 | 5500 | 5500 | 5500 | 5500 | 0 | 0/0 | 17.416s | 315.8/s |

P5500 四个 Campaign 对账：

| Campaign | expected | socket_written | submit_sm_received |
| --- | ---: | ---: | ---: |
| 1 | 2500 | 2500 | 2500 |
| 2 | 2500 | 2500 | 2500 |
| 3 | 250 | 250 | 250 |
| 4 | 250 | 250 | 250 |

P5500 的 projection_lag_seconds=0、projection queue depth 为 0、projection duplicates 为 0，spool pending_records=0，SMPP binds 为 4 且 errors 为空；loopback 运行的 route TPS 配置为 0.0，因此本次不把它解释为真实 Route TPS 上限测试。

## 3. 资源与文件系统证据

- nproc=64；实际 acceptance 进程被限制在 CPU 0-3。
- Run 所在文件系统为本地 ext4 /dev/sda2。
- Run 目录大小：P5500 前约 792M，结束后约 803M。
- P5500 后磁盘剩余 7,529,912 KiB（62% used）。
- P5500 后系统 available memory 241,941 MiB。
- 已记录 df -T、Python 版本、pip freeze、安装日志和每门 JSON。
- 本轮没有采集运行期间 RSS 峰值时间序列，因此不虚报“已实测低于 2 GiB”；2 GiB RSS 只作为命令执行约束/预警要求，正式生产 canary 前仍应补充持续 RSS 采样。

服务器端不是原生 SLES12SP5；原生 SLES 兼容性仍以 standalone runtime 的 manylinux2014/glibc2.17 合同和既有包检查为依据，不能写成原生 SLES 实测。

## 4. WSL、Windows 与 EulerOS 对照

吞吐为 generated / total duration，包含进程启动、loopback 和 drain，不能当作稳定生产 TPS；不同环境硬件、文件系统和调度约束不同，不作跨环境 SLA 承诺。

| 环境 | P10 | P100 | P1K | P5500 |
| --- | ---: | ---: | ---: | ---: |
| Windows quick（/mnt/NTFS 类环境，不作 Linux 基线） | 1.243s / 8.0/s | 2.964s / 33.7/s | 11.085s / 90.2/s | 43.576s / 126.2/s |
| WSL native ext4 formal | 1.245s / 8.0/s | 2.117s / 47.2/s | 13.360s / 74.9/s | 62.401s / 88.1/s |
| EulerOS 2.0 SP15 server ext4 proxy | 0.526s / 19.0/s | 0.803s / 124.5/s | 3.925s / 254.8/s | 17.416s / 315.8/s |

EulerOS 结果仅说明本次受限 loopback acceptance 在该服务器上的端到端表现；不能推导真实 SMSC 网络吞吐，也不能覆盖旧的 G4/G6 历史失败门禁。

## 5. 本地回归与恢复证据

- VNext 专项测试：23 passed, 4 warnings
- backend 全量回归：376 passed, 4 skipped, 4 warnings
- frontend npm run build：PASS
- worker restart：crash exit 17，crashed 5，recovered 15，unique 20，pending 0
- ProjectionWriter pause/resume：恢复后 applied 20，duplicates 0，queue depth 0
- pause control fence：暂停期间 writes 0，恢复后 writes 5

上述故障/背压/控制测试在本地隔离环境完成；共享服务器本轮只运行 loopback 规模门禁，未运行真实 SMSC、52K/102K、G4/G6 或生产流量。

## 6. 已导入证据的 SHA256

| 文件 | SHA256 |
| --- | --- |
| evidence/p10.json | 88fa49d1e67773038d4be9dbf2f19e2a17b4332d3df64648d14f501626897670 |
| evidence/p100.json | 4deb96edb51c7152a993a3248f28a2a2f3b71142f78ff64336ef4bd4c5125038 |
| evidence/p1k.json | acdc7c3d7e72cae3f3b4ff76b9189a70dde23acb8124242799109f6064624b56 |
| evidence/p5500.json | fff66af69164650bb6f2da3b9005c42c31c3f28a3f66f8b9fcb53ccafbad62c5 |
| evidence/python-version.txt | 0fcf2053b9b445f8fd79372f7ba1f4ea09b5d00c63a32f602c84ae5973b4cd19 |
| evidence/pip-freeze.txt | 847439f3d7d1169e87610f93f1ad7da493870c7efc4b35284af2cc6017b89686 |

证据扫描未发现密码、服务器凭据或真实手机号；运行数据、数据库、spool、日志和证据均未放入正式部署包。

## 7. 最终判定

V0–V5 的代码、回归、loopback 对账和离线交付均已完成；VNext 新门禁 P10/P100/P1K/P5500 在 WSL ext4 与 EulerOS 2.0 SP15 代理服务器通过，判定为 VNEXT_PIPELINE_READY_FOR_CANARY。

该判定仅允许进入合成/loopback 约束下的下一步 canary 评审。真实 SMSC、原生 SLES 实机、生产发布、旧 INTEGRATED_NO_GO 和历史 G4/G6 结论均未被改写。
