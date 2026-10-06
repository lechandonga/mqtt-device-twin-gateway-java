# 本地设备接入与状态同步网关（内存 MQTT 风格）

纯内存实现的设备接入网关，模拟 MQTT 风格的连接、会话与消息投递，并在其上构建设备影子、
指令下发、遥测告警与固件升级状态机。**不依赖任何外部 broker、云服务或真实凭据**，
所有状态保存在进程内存中，重启即清空。

## 运行环境

- JDK 17+（pom 目标为 21，本机已用 JDK 25 验证）
- Maven 3.9+

## 模块与代码结构

| 包 | 职责 |
| --- | --- |
| `mqtt` | 内存 Broker、持久会话、遗嘱、QoS0/1、通配符匹配 |
| `shadow` | 设备影子：期望态/上报态版本合并与冲突结论 |
| `command` | 指令下发：离线 FIFO、幂等去重、超时/不支持结论 |
| `telemetry` | 遥测归一化、去重、时间戳保护、阈值边沿告警 |
| `ota` | 固件升级状态机、乱序/重复幂等、失败确定回滚 |
| `gateway` / `web` | 接入门面与本地手工验证 REST 接口 |

## 一、会话与投递语义

实现在 `InMemoryMqttBroker` / `Session`。

- **接管（takeover）**：同一 `clientId` 再次连接时，旧连接被摘除并关闭（回调
  `onSessionClosed(TAKEOVER)`），旧连接的遗嘱（Last Will）发布且只发布一次；
  旧连接飞行中的 QoS1 消息先回到会话队列，新连接排干队列，消息不丢。
- **正常 DISCONNECT 不发遗嘱**；异常断线/接管才发布遗嘱。
- **持久会话（`cleanStart=false`）**：订阅关系、飞行中消息、离线队列都挂在 `Session`
  而非连接对象上，重连后订阅保留、未确认消息不丢，顺序为“最早未确认消息 + 离线新消息”。
  `cleanStart=true` 则丢弃全部会话状态。
- **至多一次（QoS 0）**：发后即忘，无 ACK、不重投；设备离线时直接丢弃。
- **至少一次（QoS 1）**：入会话飞行队列，设备 `PUBACK` 后移除；未 ACK 前可重投
  （`redeliverInflight`，模拟真实 broker 的定时重投），重复投递沿用同一 `packetId`。
  重复 `PUBACK` 幂等忽略并计入重复 ACK 统计。**可能重复但绝不丢失**，消费端应按
  `packetId` 做幂等（单测 `consumerDeduplicatesRedeliveryByPacketId` 示范）。
- **QoS 降级**：生效 QoS = min(发布 QoS, 订阅最大 QoS)。
- **通配符**：`+` 匹配单层，`#` 匹配其下所有层（只能位于末层）。同一条消息命中多个
  过滤器时，按“层级更具体 > 通配更少 > 字典序”稳定选择（`TopicMatcher.specificity`），
  `TopicMatcher.explain` 给出逐层匹配解释。
- **顺序保证**：同一设备的发布路由、ACK、连接/断开在同一把设备锁内串行，因此该设备的
  消息顺序与状态结论确定可复现。

## 二、设备影子与版本冲突规则

实现在 `DeviceShadow`。期望态（desired，服务端写）与上报态（reported，设备写）各有独立
单调版本号，更新为浅合并（只覆盖 patch 中出现的键）。

- **期望态乐观并发控制**：写入必须携带 `expectedVersion` 且严格等于当前版本：
  - 相等 → `ACCEPTED`，版本 +1；
  - 落后（`expectedVersion < current`）→ `REJECTED_STALE_VERSION`，返回当前版本供调用方重读；
  - 超前 → 视为非法状态，抛异常（存在版本缺口）。
  并发写同一版本槽时恰好一个成功、其余全部得到可区分的 STALE 结论，既不丢更新也不覆盖。
- **上报态**：`version` 必须连续：
  - 与当前版本相同 → `REJECTED_DUPLICATE_VERSION`（重复投递/重试，幂等忽略）；
  - 小于当前版本 → `REJECTED_STALE_VERSION`（乱序旧消息，拒绝）；
  - 跳版本（> current+1）→ 抛异常。
- `delta()` 给出期望态尚未被上报态追平的键，用于判断配置是否已生效。

## 三、指令队列

实现在 `CommandService`，指令经 QoS1 主题 `devices/{deviceId}/commands/{name}` 下发。

- 设备离线 → 进入该设备 FIFO 队列；重连（`DeviceGateway.connect` 内部调用
  `drainOnReconnect`）后**严格按入队顺序**送达。
- **幂等键 `commandId`**：同设备同 ID 重复下发返回同一状态对象，只入队/送达一次，
  重复 ACK、重复执行结果均幂等。
- 终态与失败结论：
  - `QUEUED → SENT → ACKED → SUCCEEDED`；
  - 超时未 ACK → `FAILED_TIMEOUT`（`sweepTimeouts()` 扫描；时钟可注入，测试无需真实等待）；
  - 设备能力不包含该指令 → `FAILED_UNSUPPORTED`（在线立即失败、离队时失败，均不入队送达）；
  - 设备主动回报不支持 / 执行失败 → `FAILED_UNSUPPORTED` / `FAILED_DEVICE_REJECTED`。
  终态之后的迟到 ACK/结果不会翻转状态。

## 四、遥测处理

实现在 `TelemetryService`，管线为：归一化 → 去重/时间戳校验 → 最新状态 → 阈值边沿判定。

- **归一化**：指标名 `trim` + 小写；`NaN`/`Infinity`/空键剔除；同消息同名指标取最大值。
- **去重**：按 `messageId` 幂等，重复投递（含不同载荷的同 ID 重试）判
  `DUPLICATE_MESSAGE`，不更新状态、不重复告警。
- **时间戳保护**：采样时间戳只允许单调不减，回退旧数据判 `STALE_TIMESTAMP`，
  最新状态不被污染；相等时间戳允许（同批补点）。
- **阈值告警边沿触发**（`ThresholdRule`）：正常→超限只产生一次 `FIRED`；条件持续期间
  重复上报不告警；回落到恢复线产生一次 `RECOVERED` 后才能再次 `FIRED`。支持迟滞
  （hysteresis）避免阈值附近抖动反复告警。

## 五、固件升级状态机

实现在 `OtaService` / `OtaTask`，每设备同时只允许一个活动任务：

```
PENDING → DOWNLOADING → INSTALLING → SUCCEEDED
                 │            │
                 └──── 任意失败 ┴→ ROLLING_BACK → ROLLED_BACK (activeVersion = knownGood)
```

- 创建任务时必须给出 **knownGoodVersion**（当前确认可用版本），它是任何失败后的确定
  回滚目标；目标版本与可用版本相同视为非法。
- 进度上报携带单调递增 `sequence` 与百分比，安装阶段通过 `installing=true` 显式进入：
  - 相同 sequence（或成功后重复成功）→ `DUPLICATE_SEQUENCE`，状态/进度不变；
  - sequence 回退、百分比倒退 → `OUT_OF_ORDER_SEQUENCE`，拒绝；
  - 非法阶段跳转（如未开始安装就报成功、终态后再报进度）→ `ILLEGAL_TRANSITION`；
  - 未知任务 → `UNKNOWN_TASK`。
- 任何非成功阶段上报失败都回滚到 `knownGoodVersion`，状态 `ROLLED_BACK`；重复失败上报
  保持同一确定结论，回滚后再报成功被拒绝。

## 本地验证

### 运行单元测试

```bash
mvn test
```

测试共 33 个用例（7 个测试类），控制台/surefire 报告按业务模块输出：

```
[业务模块] 设备接入 / 会话 / 投递语义（接管、遗嘱、QoS0 vs QoS1、通配符）
---- [用例] 同 clientId 再连接按接管处理：旧连接被关闭且遗嘱只发布一次 ----
==== [结论] ... -> 通过 ✅
```

| 测试类 | 覆盖业务 |
| --- | --- |
| `MqttSessionDeliveryTests` | 接管+遗嘱、持久会话恢复、QoS0/1、重投去重、重复 ACK、通配符、QoS 降级 |
| `DeviceShadowTests` | 版本合并、过期/重复版本区分、16 线程并发写、重读重试不丢更新、delta |
| `CommandQueueTests` | 离线 FIFO、重连按序送达、commandId 去重、超时、不支持、重复 ACK/结果 |
| `TelemetryAlarmTests` | 归一化、重复消息、时间戳回退、告警边沿与迟滞、并发重复上报 |
| `OtaStateMachineTests` | 正常路径、重复/乱序进度、下载/安装失败回滚、重复失败、终态保护 |
| `GatewayEndToEndTests` | 全链路：遗嘱 → 离线排队 → 重连补投 → 影子冲突 → 遥测去重 → 升级回滚 |
| `ApplicationTests` | Spring 上下文装配冒烟 |

### 手工调用 REST（可选）

```bash
mvn spring-boot:run

curl -X POST 'localhost:8080/api/devices/dev-1/connect' \
  --data-urlencode 'willTopic=events/dev-1' --data-urlencode 'willPayload=offline' -G
curl -X POST 'localhost:8080/api/devices/dev-1/subscribe?topicFilter=devices/dev-1/commands/%23&qos=1'
curl -X POST localhost:8080/api/devices/dev-1/telemetry \
  -H 'Content-Type: application/json' \
  -d '{"messageId":"m1","timestamp":1000,"metrics":{"cpu":96}}'
curl localhost:8080/api/devices/dev-1/inbox

# 设备影子（注意 expectedVersion 乐观并发控制）
curl -X POST localhost:8080/api/devices/dev-1/shadow/desired \
  -H 'Content-Type: application/json' \
  -d '{"expectedVersion":0,"state":{"mode":"eco"}}'
# 固件升级：下载 -> 安装 -> 成功（或 failure 触发回滚）
curl -X POST localhost:8080/api/devices/dev-1/ota/start \
  -H 'Content-Type: application/json' \
  -d '{"taskId":"ota-1","targetVersion":"2.0.0","knownGoodVersion":"1.0.0"}'
curl -X POST localhost:8080/api/ota/ota-1/progress \
  -H 'Content-Type: application/json' -d '{"sequence":1,"percent":100}'
```
