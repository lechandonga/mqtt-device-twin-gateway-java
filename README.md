# 本地设备接入与状态同步网关（内存 MQTT 模拟器）

纯内存实现的设备接入网关，用 Spring Bean 模拟 MQTT 风格的连接、会话与消息投递，
并在其上构建设备影子、指令下发、遥测处理与固件 OTA 升级能力。
**不依赖任何外部 broker、云服务或真实凭据**，所有状态保存在 JVM 内存中。

## 模块总览

| 包 | 组件 | 职责 |
| --- | --- | --- |
| `mqtt` | `InMemoryMqttBroker` | 连接/接管、遗嘱、持久会话、QoS0/1、通配符、retained |
| `twin` | `DeviceTwinService` | 期望态/上报态字段级版本合并、并发冲突拒绝 |
| `command` | `CommandService` | 指令下发、离线 FIFO 队列、去重、超时、不支持指令 |
| `telemetry` | `TelemetryService` | 单位归一化、样本去重、时序校验、阈值边沿告警 |
| `firmware` | `FirmwareUpgradeService` | OTA 状态机、幂等进度、失败回滚、重试 |

## 1. 会话与投递语义

### 连接接管与遗嘱
- 同一 `clientId` 再次 `connect`：旧连接立即被置为离线（接管），**旧连接的遗嘱消息发布一次**，
  然后新连接建立；新连接正常 `disconnect` 不再发布遗嘱。
- `connectionLost`（异常掉线）同样触发遗嘱；主动 `disconnect`（MQTT DISCONNECT）不触发遗嘱。

### 持久会话（cleanSession=false）
- 掉线后会话保留：**订阅关系、未确认（in-flight）消息、离线队列**均不丢。
- 重连返回 `sessionPresent=true`；`flushPending(clientId)` 先按 packetId 升序排空离线队列，
  再重投 in-flight 未确认消息。
- `cleanSession=true` 时掉线/断开即清除会话全部数据。

### 投递语义
- **QoS0 至多一次**：设备在线才同步投递；离线消息直接丢弃，不排队、不重投。
- **QoS1 至少一次**：离线时进入持久会话的离线队列，重连后重投直到收到 `ack(packetId)`；
  因此可能重复。每次重投回调携带 `duplicate=true`，broker 同时按
  `topic#payload` 记录消费指纹，业务端据此去重，重复消息不会被重复消费。
- 重复或未知的 `PUBACK` 为无副作用幂等操作，返回 `false`，不会误删其他消息。

### 主题匹配
- `+` 匹配单层，`#` 只能在末尾并匹配零到多层；以 `/` 精确分层，
  例如 `a/+` 不匹配 `a`，`a/b` 不匹配 `a/b/`，规则集中在 `TopicMatcher`。
- retained 消息在新订阅建立时投递一次；空 payload 的 retained 发布等价于清除。

## 2. 设备影子：版本与冲突规则

- 期望态（desired）与上报态（reported）分开存储，版本按字段记录、写入时整体推进。
- 写入必须携带调用方持有的基准版本 `expectedVersion`：
  - `expectedVersion < 当前版本` → 拒绝，原因 **`VERSION_CONFLICT`**，不覆盖、不推进版本（无丢失更新）。
  - 相同版本、相同内容的重复上报 → **`STALE_DUPLICATE`**（幂等跳过）。
  - 相同版本、不同内容 → 同样拒绝（`VERSION_CONFLICT`），旧数据绝不回退污染新状态。
- 并发安全：每台设备一把内置锁 + 乐观版本检查，并发写入只有持最新版本者成功，
  其余都拿到可区分的冲突原因；持最新版本重试即可成功。
- `getMerged(deviceId)` 按字段合并期望/上报并标注 `inSync`。

## 3. 指令队列

指令经 broker 的 QoS1 主题 `cmd/{deviceId}` 下发，状态机：

```
QUEUED ──设备重连/上线，FIFO 送达──▶ DELIVERED ──acknowledge──▶ ACKED
   │                                 │
   └──超时──▶ FAILED_TIMEOUT ◀───────┘
不支持指令（下发前能力校验）──▶ FAILED_UNSUPPORTED
```

- **去重**：`commandId` 全局幂等，重复 `sendCommand` 返回 `duplicate=true`，不重复排队/下发。
- **离线队列**：离线期间指令 `QUEUED`；设备上线（连接监听器自动触发 `drainQueue`）后
  严格按入队顺序 FIFO 送达。
- **超时**：每条指令独立调度，默认 2000ms（可按指令覆盖）；超时后 `FAILED_TIMEOUT` 为终态，
  排队中超时的指令在重放时直接剔除，迟到的确认不能翻案。
- **不支持**：设备能力表不含该指令时立即 `FAILED_UNSUPPORTED`，不入队，结论明确。

## 4. 遥测处理

摄入顺序：**归一化 → 去重 → 时序校验 → 更新最新状态 → 阈值评估**。
- 归一化：温度 °F→°C、速度 km/h→m/s（统一 SI，见 `UNIT_TABLE`）。
- 去重：同一 `(deviceId, sampleId)` 只生效一次，重复样本返回 `DUPLICATE`。
- 时序：时间戳必须单调不减，回退样本返回 `STALE_TIMESTAMP`，最新状态与最新时间戳均不被污染。
- 阈值规则按**状态边沿**触发：正常→越限产生一次 `TRIGGERED`；条件持续期间只更新状态、
  不重复告警；越限→正常产生 `RECOVERED` 并自动重新武装，之后可再次触发。

## 5. 固件升级状态机

```
PENDING → DOWNLOADING → DOWNLOADED → INSTALLING → COMPLETED
              任意阶段收到 FAILED
FAILED → ROLLING_BACK → ROLLED_BACK（当前版本确定为 rollbackVersion）
ROLLED_BACK ──retry（同一 jobId，attempts+1）──▶ PENDING（重新走状态机）
```

- `jobId` 幂等：重复创建返回同一任务；重复状态/进度上报返回 `IGNORED_DUPLICATE`。
- 进度百分比必须单调递增，回退/跳跃阶段返回 `IGNORED_OUT_OF_ORDER`，状态不被污染。
- 任意阶段失败都进入确定回滚：当前运行版本立即切换为创建任务时声明的
  **`rollbackVersion`（上一个已知可用版本）**，结论可预期。
- `COMPLETED` 与 `ROLLED_BACK` 之后的迟到上报均为 `ALREADY_FINAL`；
  只有 `ROLLED_BACK` 允许 `retry`。

## 本地验证

环境要求：JDK 21、Maven 3.6+。

```bash
mvn test
```

- 共 29 个用例，分布在 6 个测试类（见下）。控制台日志以 `BUSINESS-COVERAGE` 输出
  每个测试类覆盖的业务域、每个用例的中文名与通过/失败结论；业务组件自身日志带
  `[MQTT]`/`[TWIN]`/`[CMD]`/`[TELEMETRY]`/`[OTA]` 前缀，可直接对照业务过程。

| 测试类 | 用例数 | 覆盖边界 |
| --- | --- | --- |
| `MqttSessionTests` | 7 | 接管遗嘱、持久会话、QoS0 丢弃、QoS1 离线队列/重投/重复确认、通配符、retained |
| `DeviceTwinTests` | 5 | 版本推进、过期冲突、20 线程并发写入、重复上报、期望/上报合并 |
| `CommandTests` | 6 | 在线确认、离线 FIFO、重复去重、超时终态、迟到确认、不支持指令 |
| `TelemetryTests` | 4 | 单位归一化、去重、时间戳回退/乱序、边沿告警持续与恢复再触发 |
| `FirmwareUpgradeTests` | 6 | 正常升级、幂等创建/上报、乱序进度、失败回滚、失败重试、终态拒绝重试 |
| `ApplicationTests` | 1 | Spring 容器装配五个核心组件 |
