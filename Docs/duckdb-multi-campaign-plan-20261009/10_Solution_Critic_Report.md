# 独立评审与设计闭环

独立审计者：Kuhn。工作范围为CodeGraph/当前源码、DuckDB1.2.0内存故障注入、假SMPP客户端及主文的两轮设计复审；未访问生产业务库、未改业务代码。

最终结论：研究方案复审通过，无剩余设计阻断项。该结论不代表已完成代码修复、详细实现或线上性能验收。

| 发现 | 修订合同 | 状态 |
| --- | --- | --- |
| 设计需补短信token完成写回和部分分片接受的恢复边界 | 第8.1及TST015补齐tokenfence、续租、partial/unknown隔离，禁止整条盲目重发 | RESOLVED（设计） |
| 设计需补SMSC来源ID隔离、ID重用歧义和DLR持久确认 | 第8.1及TST016补齐来源隔离、兼容迁移、持久完成后确认，验证对端重发 | RESOLVED（设计） |
| flush不能只以task_done归零声称此前批次成功 | 第8.2及TST017补齐水位和前序失败报告 | RESOLVED（设计） |
| 停机后迟到结果和日志连续水位、损坏恢复合同缺失 | 4.4定义接收截止、回调排空双屏障、连续settle与CRC隔离启动门禁 | RESOLVED（设计） |
| 多中心可能重复分配共享writer预算；DRR权重不保证硬保底 | 5.4定义共享成本向量准入、先保底再DRR、多route合账和最大合法parts信用 | RESOLVED（设计） |
| 5/30秒SLO未约束不可抢占工作量、DLR独立额度和ACK执行线程 | 5.4及8.3限定SLO适用输入与chunk必做；DLR独立信用、连接generation和有序ACK | RESOLVED（设计） |
| 状态更新和SMSC来源隔离缺少schema合同 | 8.3给表结构、合法前置状态、命中后才计数、稳定namespace和unknown排除入口 | RESOLVED（设计） |
| 全体停滞与writer age增长不能单独确定数据库本身阻塞 | 第9节改为疑似共享写入链路阻塞，结合线程栈和SQL/IO确认 | RESOLVED（设计） |

机器明细：model/critic_findings.json；修订追溯：model/revision_log.json。RESOLVED只表示方案已补相应合同，当前源码审计发现的缺陷仍待阶段A等实施修复。
