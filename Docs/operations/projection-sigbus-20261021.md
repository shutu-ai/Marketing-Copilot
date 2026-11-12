# Projection writer SIGBUS：诊断与程序修复

## 生产证据与判断边界

2026-10-21，projection writer PID 55056 在 13:40:34 EAT 收到 SIGBUS。
faulthandler 指向 `ProjectionWriter.flush()` 的 DuckDB `COMMIT`；运行版本为 DuckDB 1.2.0。
此前 PID 54163 在同一路径记录了
`Failed to commit: Cannot perform IO in in-memory database - CreateBlock!`。

崩溃进程的内核栈为 `do_coredump → elf_core_dump → pipe_write → pipe_wait`，
其余线程处于退出路径。系统使用管道形式的 systemd-coredump，处理 core 期间
`multiprocessing.is_alive()` 仍返回真，ready event 和状态 JSON 仍保留崩溃前的数据。
因此旧版健康接口继续显示 ready，监督器等待进程退出而无法及时重新拉起。

连续采样中投影计数和 journal offset 停止推进，发送 worker 的 journal 仍增长。
这能证明投影落后及发送事件继续产生；不能据此直接推断当前 SMSC 接收速率或送达率。

确定的问题是提交阶段 native 崩溃，以及 core 收集、陈旧状态放大了恢复延迟。
自动 checkpoint 与临时表/持久表混合事务的存储路径是本次规避对象；
尚未取得可定位具体 C++ 缺陷的完整 native 回溯，不能将其表述为已证明的引擎根因。

## 变更

- 保持 DuckDB 1.2.0、现有表结构、批大小、向量化 staging 和 journal 处理顺序。
- 关闭提交时自动 checkpoint，保留每批事务的 WAL 持久提交。
  WAL 达到 16 MiB 后，在已提交的事务之外清除 writer 的临时表，再显式 checkpoint。
  journal cursor 仍只在事件事务成功提交后推进；checkpoint 前意外退出由 WAL 和既有去重机制恢复。
- checkpoint 的可重试错误不重新入队已提交事件；不可恢复的内部存储错误退出子进程，
  由既有监督器重新打开连接，避免无限重试失效连接。
- 提交失败时恢复应用累计计数与 staging 创建状态，避免重试后重复计数。
- Linux projection 子进程通过 PR_SET_DUMPABLE 关闭内核 core，保留 Python faulthandler。
  仅设置 RLIMIT_CORE 无法阻止管道式 core collector。需要完整 core 时可显式设置
  `MC_VNEXT_PROJECTION_ALLOW_CORE_DUMP=1`，此时会重新承担 core 收集延迟。
- faulthandler 文件名包含 PID，后继进程不会覆盖前一进程的崩溃日志。
- 状态包含写入时间；超过 30 秒、PID 不匹配、无有效状态文件或 Linux PF_DUMPCORE
  置位时，不再返回 ready。健康接口及运行时降级信息反映投影不可用。
- 常规进度诊断和状态文件最多每 250 ms 发布一次，启动、异常、最终状态仍及时记录。
  IPC 排空等待 FIFO sentinel/EOF，避免 Queue feeder 尚未交付完时提前结束。

短信 socket、SMPP 窗口、限速和活动调度路径未改动。

## 验证方式

新增 `backend/tools/projection_commit_probe.py`，使用固定随机种子的多语言合成长短信，
同时检查事件记录、聚合计数、完整文本摘要和进程不正常关闭后的 WAL 恢复。
每次必须指定新的空测试目录，不读写生产数据库。

原生 Linux 测试目录：`/tmp/mc-projection-commit-20261021.Sr9Iic`，AlmaLinux-8 WSL，
`/dev/sdd ext4`，CPython 3.11.13，DuckDB 1.2.0。
源码、数据库、WAL、虚拟环境和测试临时目录均在原生 Linux 文件系统。
目录中的 `dependency-freeze.txt` 和 `test-dependency-freeze.txt` 分别记录探针及 pytest 依赖。

首轮相同 16 MiB checkpoint 阈值、16,384 条事件顺序对照：

| 版本 | 时间 | 事件/秒 | 重开后校验 |
| --- | ---: | ---: | --- |
| 原版 | 39.345 s | 416.42 | 通过 |
| 修复版 | 39.168 s | 418.30 | 通过 |

两者记录摘要一致：`00cd160d1e5556e006e41b6498e19daea7f1286a7cb76cefad27c1f66073d472`。
该轮差异约 0.45%，应视为接近，不能宣称生产吞吐提升。
1 MiB 强制 checkpoint 的探索负载显示频繁全量 checkpoint 开销较大，未用于默认性能结论。

最终代码第二轮同参数顺序对照：原版 34.132 s / 480.02 事件每秒，
修复版 29.871 s / 548.49 事件每秒，重开后摘要仍一致。
两轮均未见吞吐下降；存在后台回归任务及系统调度差异，不能把单轮加速比例外推到生产。

默认阈值下的 65,536 事件连续恢复探针通过，观察到 4 次 checkpoint，
151.530 s / 432.50 事件每秒；重开后摘要与同种子的原版 65,536 事件探针一致：
`2ee6242d70d333438debecd13c2a87670385a560aaf969d425534f16396728a8`。

Linux 回归还实际注入 SIGBUS，验证子进程在 5 秒内退出、保留故障日志、
重新拉起并恢复崩溃前后两条事件，数据库记录与累计计数均为 2。

Windows 后端全量回归：531 passed，7 skipped（包含 Linux 专属故障注入检查），
无失败，耗时 267.46 s。最终代码的 Linux 定向回归 42 passed，耗时 15.48 s，
包含真实 SIGBUS 故障注入；两类结果分开记录。

以上不替代 SLES12SP5 目标机的长时间生产负载验证。程序修复完成并不代表现有生产
WAL 已修复；不得据此删除 WAL、重置 journal offset 或直接以新库覆盖旧库。
