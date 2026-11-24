# 滑雪场调用链尾部采样服务（Go）

造雪调度、缆车闸机、雪票、支付、IoT 等 36 个服务的调用链尾部采样 Demo。服务和上报流量均在程序内模拟，并提供一个中文实时决策页面。

## 快速开始

```bash
# 启动模拟服务 + 决策页面，默认 http://localhost:8080
make run

# 或指定地址/种子
go run ./cmd/tailsampling -addr :18080 -seed 42
```

```bash
# 固定随机种子，虚拟运行 3 小时流量并逐项核对
make verify

# 可调整时长和允许偏差
go run ./cmd/verify -seed 15833 -duration 3h -max-deviation 0.15

# 单元测试 + 固定种子的 20 分钟集成模拟
make test

# 核心包竞态检查
make race
```

页面接口：`GET /api/state`，页面本身由 `internal/web/index.html` 通过 `go:embed` 编入二进制。

## 页面包含的信息

- **缓冲链路数与内存**：正在聚合的 Trace 数、估算缓冲内存、峰值、配置上限、Go Heap、Goroutine。
- **各策略命中数**：出错、总耗时、重点服务/接口、标签、自适应概率。
- **每服务速率**：目标保留 RPS、近窗实际保留 RPS、累计实际 RPS、偏差、当前概率、到达速率、强制保留速率。
- **迟到片段记录**：缓存保留/丢弃、LRU 淘汰后独立保留/丢弃，以及最近 80 条处理明细。
- 另外展示总决策数、总保留率、分批数、静默顺延次数等。

## 目录结构

```text
cmd/tailsampling/   启动实时模拟流量、Sweeper、HTTP 页面/API
cmd/verify/         三小时固定种子流量核对程序
internal/model/     Span、Decision、LateRecord、Batch
internal/sampling/  TraceID 低位概率阈值
internal/policy/    出错/耗时/服务接口/标签/概率策略
internal/controller/ 每服务近期速率 + 累计预算反馈控制器
internal/pipeline/  聚合、静默、顺延、分批、LRU 决策缓存、内存保护
internal/stats/     分服务滑动窗口和页面快照
internal/sink/      导出接口与校验用紧凑 Manifest
internal/sim/       36 个服务、固定种子、三小时虚拟流量模拟器
internal/demo/      页面使用的真实时钟流量模拟器
internal/web/       HTTP API 和单页控制台
```

## 关键语义

### 1. 按 TraceID 聚合与静默期

每个 `Span` 是一个调用片段。Pipeline 按 `TraceID` 缓存，并在每次收到片段后刷新：

```text
deadline = lastArrival + silence[service]
```

普通服务静默期为 2s，`event-bus`、`notification-worker`、`iot-mqtt-gateway`、`analytics-pipeline` 等异步服务为 8s（实时 Demo 为加快页面观察使用 6s）。

Sweeper 在静默到期后先进入一个很短的到达宽限时间；宽限结束仍无新片段才执行决策。若到期后、最终决策前又收到片段，则该链路顺延一个完整静默期；`MaxExtensions=1`，顺延次数有上限。

### 2. 策略优先级

优先级数字越小越高：

1. `error=10`：任一片段出错，强制保留。
2. `duration=20`：`max(End)-min(Start) >= 1s`，强制保留。
3. `service_operation=30`：命中 `payment-service` 或接口 `Refund`，强制保留。
4. `tag=40`：标签 `ticket.class=vip`，强制保留。
5. `probability=100`：每服务自适应概率。

所有策略都会统计命中；任意非概率策略命中即 `MandatoryKeep=true`。如果没有强制命中，再由概率决定。主原因取优先级最高的命中策略。

### 3. TraceID 低位概率

TraceID 是随机 `uint64`。概率判断不使用每次随机数，而是直接比较低 20 位：

```go
bucket := traceID & ((1 << 20) - 1)
limit  := uint64(probability * (1 << 20))
hit    := bucket < limit
```

因此同一个 TraceID 在相同概率阈值、任何处理环节都会得到相同结论。

### 4. 每服务目标与自适应概率

每个根服务有独立目标保留 RPS。控制器每 10s（验证程序）读取近 120s 滑动窗口：

```text
targetRPS = mandatoryRPS + probability * eligibleRPS
probability = (targetRPS - mandatoryRPS) / eligibleRPS
```

其中 `eligibleRPS` 是未被强制策略命中的链路决策速率，避免把强制保留流量错误地当作概率杠杆。

故障期错误过多时，强制保留可能超过目标；此时概率降为 `0`。控制器还维护一个累计预算修正项：超目标后会在修正窗口内临时低于前馈比例，用后续余量偿还故障尖峰造成的累计偏差。

> 目标低于错误/重点策略的强制下限时，任何采样器都不可能同时满足“错误一条不丢”和“总速率不超目标”。`payment-service` 本身就是重点服务策略对象，因此它的目标按其强制保留下限和模拟突增负载设置为 1.35 RPS；这是策略优先级的真实反映。

### 5. 迟到片段与决策缓存

决策完成后放入容量受限的 LRU：

- LRU 仍有决策：迟到片段直接按缓存的 `Keep/Drop` 处理。
- LRU 已淘汰：若片段时间明显旧于当前时间，则识别为“无决策缓存的迟到片段”，只用该片段重新跑一遍策略。
- 独立判定中的概率仍使用同一 TraceID 低位算法。

验证程序的 LRU 容量为 4096，故意让一部分 25–240s 后的迟到片段遇到淘汰路径。

### 6. 内存上限保护

当估算缓冲内存超过 `MaxBufferBytes` 时，Pipeline 从 FIFO 最老链路开始立即提前决策，直到回到上限以内。提前决策会标记 `forced_oldest=true`。

三小时常规流量的估算缓冲峰值约 5.6 MiB，低于 128 MiB 配置上限；单元测试用极小上限专门验证最老链路淘汰。

### 7. 超大链路分批

片段数达到 `BatchSize=64` 时，当前片段会立即作为临时批次送给 `Sink`，原始片段从聚合内存移除，但保留：

- 策略投影（错误、服务、接口、标签、时间范围）；
- 片段总数；
- SpanID 校验和（Sum/XOR）。

最终决策时发送最后一批和 `Decision`。接收端可在 `Keep=true` 后提交临时批次，在 `Keep=false` 后丢弃。校验 Sink 会核对：

```text
所有 Batch 中的片段数之和 == Decision.SpanCount
所有 Batch 的 Sum/XOR 校验和 == Decision 校验和
```

决策后到达的片段进入迟到处理路径，不计入决策时校验和。

## 三小时固定种子核对结果

最近一次 `make verify`（Go 1.26，macOS arm64）结果：

| 指标 | 数值 |
|---|---:|
| 虚拟时长 / 服务数 | 3h / 36 |
| 固定种子 | 15833 |
| 生成链路 | 452,322 |
| 生成片段 | 5,123,732 |
| 故障错误链路 | 17,622 |
| 慢链路 | 15,634 |
| 超大链路 | 8,177 |
| 决策数 | 452,322 |
| 保留 / 丢弃 | 177,009 / 275,313 |
| 总保留率 | 39.13% |
| 缓冲峰值 | 987 条 / 5.62 MiB 估算内存 |
| 策略命中 | error 17,622；duration 15,634；service/op 52,686；tag 15,675；probability 99,751 |
| 静默顺延 | 5,868 次 |
| 提前批次 / 片段 | 16,413 批 / 5,096,770 片段 |
| 迟到片段 | 26,962 |
| 缓存保留 / 丢弃 | 4,411 / 5,524 |
| 淘汰后独立保留 / 丢弃 | 3,635 / 13,392 |
| 各服务累计速率最大偏差 | **4.68%**（`payment-service`，允许 15%） |

核对项全部通过：

```text
[PASS] 错误和超耗时链路零漏采 — 必须保留 32955 条，漏采 0 条
[PASS] 保留/决策批次片段齐全 — 不一致 0
[PASS] 各服务累计保留速率偏差 — 最大绝对偏差 4.68%，允许 15%
[PASS] 静默到期后顺延至少发生
[PASS] 迟到片段覆盖缓存和淘汰路径
[PASS] 超大链路分批送出
[PASS] 缓冲估算内存未超过配置上限
```

说明：故障窗口的大量错误会造成短时强制保留尖峰；累计预算控制器会在后续低峰降低概率来补偿，因此三小时累计目标偏差很小。近窗速率在故障/突增期间仍可能明显偏离目标，这是“错误一条不丢”优先级的预期结果，页面同时展示近窗和累计速率。

## 已覆盖的自动化测试

```text
internal/sampling/sampling_test.go
  - 同一 TraceID 判定稳定
  - p=0/p=1 边界
  - 20 万 ID 的概率分布

internal/pipeline/pipeline_test.go
  - 错误链路聚合后保留
  - 总耗时策略使用完整 Trace 时间窗
  - 缓存决策处理迟到片段
  - LRU 淘汰后错误片段独立保留
  - 内存超限时优先提前决策最老 Trace
  - 静默顺延一次且有上限
  - 大 Trace 分批、数量和校验和一致

internal/sim/simulation_test.go
  - 固定种子 20 分钟集成模拟和关键验收项
```

`go test -race ./internal/pipeline ./internal/sim ./internal/sampling` 已通过。
