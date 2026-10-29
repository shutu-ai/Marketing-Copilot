# VNext 实时投影：单库事务方案与切换门槛

## 现场证据与根因边界

2026-10-29 远程 DuckDB 1.2.2 的三个 writer 分别在 09:23:49、09:37:48、09:51:18
发生 SIGBUS，Python 栈均停在 projection_writer.py 的 COMMIT。第四个进程启动约
13 分半后恢复 journal 消费。WAL 约 788 MB；自动 checkpoint 阈值 1 TB，显式
checkpoint 默认关闭。没有证据证明是普通写锁冲突或非法数据，SIGBUS 的内部触发条件
仍需独立副本复现和 native 栈验证。不能承诺只改超时、线程数或 checkpoint 就能治愈。

## 已实现的候选路径

- 一个 projection 子进程、一个 SQLite WAL 数据库：任务、事件去重、业务日汇总、
  journal 字节位置在同一事务提交，synchronous=FULL；不调用发送器。
- 复用有界队列、批次、公平 journal 消费、短读事务和现有任务索引。批内汇总合并后
  upsert，避免每个事件更新汇总；移除实时路径中的 DuckDB 临时表和索引提交。
- 消费位置以 SQLite 提交记录为准。JSON 保留为兼容诊断镜像，journal 仅在数据库
  提交之后回收。提交后、JSON 更新前进程退出，重启直接读取已提交位置。
- 普通锁竞争由 busy_timeout 和现有批次重试处理；事务失败恢复队列和计数。
  SQLite CORRUPT/NOTADB/IOERR/FULL/READONLY 触发现有恢复熔断，避免无限重试。
- 默认约 1000 页自动 checkpoint；WAL 达 256 MiB 且读者阻止推进时暂停写入并重试，
  防止无限增长。该上限允许最后一个有界批次超出；journal 仍可能积压，应监控磁盘
  和持续输入是否超过消费能力。此策略不保证超载时无限接收。
- 不对存活进程按固定 30 秒盲目强杀：原现场有实际长恢复，误杀会重复恢复。

## 迁移与运行

现有部署默认仍为 DuckDB；候选路径通过 `MC_VNEXT_PROJECTION_ENGINE=sqlite` 启用。
必须先在服务停止、所有子进程退出后执行（root 为 projection 目录）：

```sh
cd backend
python -m tools.migrate_sqlite_projection --root /path/to/data/vnext/projection
```

迁移持有旧库和目标库 owner lock，将旧 DuckDB 的三张权威表与 journal-offsets.json
复制到独立 staging SQLite。逐表按主键顺序校验行数、内容 SHA256，执行 integrity_check
和 checkpoint 后才重命名为 vnext-projection.sqlite3。保留旧 DB/WAL、侧车、journal、
白名单和上传文件；失败 staging 保留供诊断，禁止覆盖重试。

存在迁移库后自动选用 SQLite；禁止配置回 DuckDB，因为旧库可能已落后，journal
也可能已回收。回退必须先停机、对齐消费位置和所有新增事件，不能仅改环境变量。
旧 DuckDB recovery/maintenance 工具对迁移目录显式拒绝执行，避免替换整目录丢数据。
SQLite 硬损坏的恢复需有验证过的快照及快照位置以来的完整 journal；不能承诺从
已回收 journal 重建历史。当前候选未提供全自动的损坏库替换。

## 性能和验收

发送路径无新增同步调用；投影性能仍会间接影响积压和磁盘，不能据此宣称完全无影响。
事件数不是短信人数：一个短信可产生多个任务/发送/响应事件，验收必须按实际事件倍率。

`backend/tools/validate_projection_ext4.sh` 将源码、解释器、DB、WAL、结果全部置于
WSL 原生 ext4，记录 df -T、Python 与 pip freeze；比较相同 512 事件批次、四个并发
分页读取者、100000 个多语言事件。该微基准不包含发送器、完整 journal/offset fsync、
导出、老化、生产历史规模和多日稳定性，不能当成业务端到端验收。

切换前必须完成：实际峰值和至少 2 倍事件输入的追平测试；24–72 小时持续运行；
生产等量历史索引；分页、导出、老化并发；长读事务、锁冲突、提交边界进程退出、
磁盘满故障；迁移前后统计和任务一致性；目标 SLES12SP5 验证。
必须观察输入/消费速率、积压斜率、批次 P99、端到端统计延迟、WAL、RSS、业务处理
吞吐和延迟。吞吐低于实际峰值、积压持续增长或业务性能退化均禁止切换。
若单机 SQLite 无法满足这些门槛，应改用独立 PostgreSQL 投影服务，而非降低持久性
或扩大队列来掩盖问题。当前提交是候选实现，不是生产稳定性认证。

## 本次验证记录（2026-10-29）

Windows：相关测试 60 passed、2 skipped、1 deselected，P1 BOUNDARY PASS。
新路径 9 项用例包含真实子进程提交前/后退出、重放去重、原子回滚、读写并发、
写锁冲突重试、WAL 长读背压、离线迁移内容校验和迁移旁路保护。
明确排除旧用例 test_projection_process_materializes_task_index_batch_without_journals：
其固定业务日 2026-10-20 已超出默认 7 天保留期，单独运行仍失败；未修改该既有数据。

AlmaLinux-8 原生 ext4：run=/tmp/mc-sqlite-projection-MLbb0rW5，Python 3.11.13，
DuckDB 1.2.2、NumPy 2.2.6、pandas 2.3.2；source、venv、DB、WAL 均在该目录。
1 万事件、512 一批、4 个并发分页读取线程，完整结果：

| 路径 | 事件/秒 | 批次 P50 ms | 批次 P99 ms | 测试结束时 WAL bytes |
|---|---:|---:|---:|---:|
| SQLite FULL | 1843 | 267.847 | 516.895 | 6266552 |
| DuckDB + 侧车 | 316 | 1522.975 | 2319.020 | 17582631 |

两条路径均校验事件数和汇总一致、并发读取无异常。该主机有其他服务负载，且两组
顺序执行，绝对数值及约 5.8 倍差异只能作为微基准观察，不能外推生产容量。
较早仅安装 DuckDB 的探索轮次：SQLite 10 万事件约 2527 事件/秒；旧路径未完成即
主动停止，该轮不用于完整对比。完整 journal/fsync、老化、生产历史规模、SLES 和
24–72 小时稳定性验收尚未执行，也未切换远程生产引擎。
