# message-log-replica-repair-planner

当消息日志的两个副本出现不同的 log end offset（LEO）、leader epoch 也不一致时，
用 Node.js v24.16.0 计算每个副本的安全修复方案：截断范围、复制范围、修复顺序、
目标 offset 以及不可修复范围。

> **模拟声明：本项目不连接 Kafka / BookKeeper 或任何真实存储。**
> README、`examples/` 中的所有日志状态（副本 offset、leader epoch、leader 历史、
> 提交水位 high watermark、分段摘要 checksum、故障事件）**均为模拟数据**。
> CLI 只读取 JSON 并输出 JSON 修复计划，不会真正删除、复制任何日志文件。

## 安全规则

规划器把三条硬约束编码为可测试的判定：

1. **只删除未提交尾部**：安全分叉点 `targetOffset` 必须 `>= committedWatermark`。
   若 epoch/分段协议算出的截断点低于水位，自动截断会删掉已提交记录，
   该范围进入 `unrepairableRanges`（code = `COMMITTED_DIVERGENCE`），
   只建议人工批准的整副本重建，不生成任何变更步骤。
2. **旧 epoch 不能覆盖新记录**：若被指定为复制源的副本 epoch 低于目标副本，
   且目标副本存在更高 epoch 的记录，返回 `EPOCH_ROLLBACK` 不可修复范围；
   乱序到达的旧 epoch leader 事件会被标记 `EPOCH_ROLLBACK_FENCED` 并隔离（fence）。
3. **复制期间消费者不能看到部分段**：先
   `freeze_consumer_visibility`（消费者可见 offset 冻结在水位），
   复制内容一律写入 `*.staging`，整段完成后通过 `publish_segment`
   原子 `rename` 进入消费者可见的分段注册表；后缀复制会与目标保留前缀合并为
   `stage_merged_segment` 后原子替换。最后才推进水位并 `resume_consumers`。

此外，`riskChecks` 会直接评估两种朴素操作的风险：

- 把较长副本直接截断到较短副本 LEO：若区间与 `[0, committedWatermark)` 相交，
  给出会被误删的已提交范围 `deletedCommittedRange`。
- 保留较短副本并让它当 leader：消费者已提交 offset 可能超过其 LEO，
  触发 OffsetOutOfRange 与 reset，造成重复消费，输出
  `outOfRangeCommittedOffsetRange`。

## 输入格式

参见 `examples/`。所有 offset 区间均为 `[startOffset, endOffsetExclusive)`。

| 字段 | 含义 |
| --- | --- |
| `committedWatermark` | 提交水位 HW；offset `< HW` 视为已提交 |
| `leaderHistory[]` | `{ epoch, leader, startOffset }` leader epoch 历史 |
| `replicas.<id>.logEndOffset` | 副本 LEO |
| `replicas.<id>.leaderEpoch` / `epochCheckpoint[]` | 副本最新 epoch 或完整 `{epoch,startOffset}` 检查点 |
| `replicas.<id>.segments[]` | 分段摘要：`baseOffset`、`endOffsetExclusive`（或 `lastOffset`）、可选 `checksum` |
| `events[]` | 模拟故障/选举事件，如 `broker_failure`、`leader_elected`、延迟的 `leader_claim` |
| `authoritativeReplica`（可选） | 强制指定权威源（验证错误指定旧 epoch 源时会被拒绝） |

## 规划算法

1. 重放故障事件（按 `sequence`/`at` 排序），识别并 fence 回退 epoch 事件，
   结合 `leaderHistory` 解析权威 leader。
2. 对每个非权威副本执行 Kafka 风格的 `OffsetsForLeaderEpoch` 查询：
   旧 epoch 的安全点是权威侧“下一个 epoch 的 startOffset”，当前 epoch 为权威 LEO。
3. 用分段 checksum 摘要细化分叉点：同 base offset 但 checksum 不同的首个分段即尾部冲突点。
4. `targetOffset = min(副本 LEO, epoch 返回点, 分段冲突点)`。
5. epoch 回退闸与水位闸：不满足则写入 `unrepairable`，不生成变更动作。
6. 生成 `truncateRange = [targetOffset, leo)` 与
   `copyRanges = [{ startOffset: targetOffset, endOffsetExclusive: authorityLeo }]`，
   并编排分段删除、stage、原子 publish。
7. 输出带序号的 `order`（同时以相同内容提供 `repairOrder`）、
   `targetOffsets` 与 `unrepairableRanges`。

## 运行

需要 Node.js `24.16.0`（见 `.nvmrc`，`nvm use` 即可），零第三方依赖。

```bash
node --test
# 或 npm test

node src/cli.js examples/leader-switch.json
node src/cli.js examples/tail-conflict.json
node src/cli.js examples/epoch-rollback.json

# 强制把旧 epoch 副本指定为权威源：计划失败并给出 EPOCH_ROLLBACK
node src/cli.js examples/epoch-rollback.json --authority r1
```

CLI 退出码：`0` 全部可修复；`3` 存在不可修复范围（仍输出完整 JSON）；
`2` 输入无效；`1` 意外错误。也可从 stdin 读取：`cat input.json | node src/cli.js`。

## 示例场景

- `examples/tail-conflict.json`：同 epoch 两个尾部在 offset 100 处分段 checksum 冲突，
  HW=100。`r2` 截断 `[100,170)`、复制 `[100,180)`；若把 HW 改成 120，
  同一冲突立即变成不可修复的 `COMMITTED_DIVERGENCE [100,120)`。
- `examples/leader-switch.json`：r1（epoch 1）故障后 r2（epoch 2）当选，
  r1 恢复时携带僵尸 `leader_claim epoch 1`。顺序为 fence r1 → 冻结消费者 →
  r1 截断到 120 并复制到 160 → r3 仅复制缺口 `[140,160)`（合并活动段原子发布）→
  推进水位/恢复消费。
- `examples/epoch-rollback.json`：权威为 r2@epoch3。默认计划安全修复旧 epoch 的 r1；
  若用 `--authority r1` 强制旧 epoch 当源，则 r2 的 `EPOCH_ROLLBACK [120,170)`
  被标记不可修复，且不会生成任何改动 r2 的步骤。

## 项目结构

- `src/planner.js`：纯函数核心（校验、权威判定、epoch 查询、冲突检测、计划编排）
- `src/cli.js`：JSON CLI
- `index.js`：库出口
- `test/planner.test.js`：`node:test` 测试，覆盖 epoch 回退、尾部冲突、leader 切换、
  已提交尾部保护、消费者可见性协议与 CLI 退出码
- `examples/*.json`：模拟输入
