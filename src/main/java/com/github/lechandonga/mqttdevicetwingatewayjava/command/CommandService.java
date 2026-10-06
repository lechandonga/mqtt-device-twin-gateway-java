package com.github.lechandonga.mqttdevicetwingatewayjava.command;

import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttMessage;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 指令下发：
 * <ul>
 *   <li>离线 -> 入设备 FIFO 队列；重连后严格按入队顺序送达；</li>
 *   <li>同 commandId 幂等去重（重复下发/重投不会重复消费）；</li>
 *   <li>送达后超时未 ACK 给出 FAILED_TIMEOUT；设备声明不支持给 FAILED_UNSUPPORTED；</li>
 *   <li>指令走 QoS1，承载在 MQTT 会话之上，设备侧重复回调由 commandId 幂等保护。</li>
 * </ul>
 */
@Service
public class CommandService {

    private static final Logger log = LoggerFactory.getLogger(CommandService.class);

    private final InMemoryMqttBroker broker;
    private final Map<String, Deque<Command>> deviceQueues = new ConcurrentHashMap<>();
    private final Map<String, CommandState> states = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> deviceCapabilities = new ConcurrentHashMap<>();
    private final Map<String, Object> deviceLocks = new ConcurrentHashMap<>();

    private Clock clock = Clock.SYSTEM;
    private long ackTimeoutMillis = 5_000;

    public CommandService(InMemoryMqttBroker broker) {
        this.broker = broker;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public void setAckTimeoutMillis(long ackTimeoutMillis) {
        this.ackTimeoutMillis = ackTimeoutMillis;
    }

    public void registerCapabilities(String deviceId, Set<String> capabilities) {
        deviceCapabilities.put(deviceId, Set.copyOf(capabilities));
    }

    /**
     * 下发指令。
     * @return 指令状态；重复 commandId 返回已存在的同一状态（不重复入队/送达）
     */
    public CommandState send(Command command) {
        Object lock = lockFor(command.deviceId());
        synchronized (lock) {
            String key = key(command.deviceId(), command.commandId());
            CommandState existing = states.get(key);
            if (existing != null) {
                log.info("[command] device={} 重复指令 {}（当前状态 {}），幂等忽略",
                        command.deviceId(), command.commandId(), existing.status());
                return existing;
            }
            CommandState state = new CommandState(command);
            states.put(key, state);
            if (!isSupported(command.deviceId(), command.name())) {
                state.markFailed(CommandStatus.FAILED_UNSUPPORTED,
                        "设备不支持指令: " + command.name());
                log.info("[command] device={} 指令 {} 设备不支持，直接失败",
                        command.deviceId(), command.commandId());
                return state;
            }
            if (broker.isConnected(command.deviceId())) {
                deliver(command, state);
            } else {
                deviceQueues.computeIfAbsent(command.deviceId(), k -> new ArrayDeque<>()).addLast(command);
                log.info("[command] device={} 离线，指令 {} 排队（队列长度 {}）",
                        command.deviceId(), command.commandId(),
                        deviceQueues.get(command.deviceId()).size());
            }
            return state;
        }
    }

    /** 设备上线（重连）后调用：按序送达全部排队指令。 */
    public List<Command> drainOnReconnect(String deviceId) {
        Object lock = lockFor(deviceId);
        synchronized (lock) {
            Deque<Command> queue = deviceQueues.get(deviceId);
            List<Command> delivered = new ArrayList<>();
            if (queue == null) {
                return delivered;
            }
            Command command;
            while ((command = queue.pollFirst()) != null) {
                CommandState state = states.get(key(deviceId, command.commandId()));
                if (state == null || state.terminal()) {
                    continue;
                }
                if (!isSupported(deviceId, command.name())) {
                    state.markFailed(CommandStatus.FAILED_UNSUPPORTED, "设备不支持指令: " + command.name());
                    continue;
                }
                deliver(command, state);
                delivered.add(command);
            }
            log.info("[command] device={} 重连后按序送达 {} 条指令", deviceId, delivered.size());
            return delivered;
        }
    }

    private void deliver(Command command, CommandState state) {
        String topic = "devices/" + command.deviceId() + "/commands/" + command.name();
        String body = command.commandId() + "|" + command.name();
        MqttMessage message = new MqttMessage(broker.nextPacketId(), topic,
                body.getBytes(StandardCharsets.UTF_8), Qos.AT_LEAST_ONCE);
        broker.publish(message, command.deviceId());
        state.markSent(clock.nowMillis());
    }

    /** 设备收到指令后的 ACK（重复 ACK 幂等）。 */
    public CommandStatus acknowledge(String deviceId, String commandId) {
        Object lock = lockFor(deviceId);
        synchronized (lock) {
            CommandState state = states.get(key(deviceId, commandId));
            if (state == null) {
                throw new IllegalArgumentException("未知指令: " + commandId);
            }
            if (state.status() == CommandStatus.ACKED || state.status() == CommandStatus.SUCCEEDED) {
                log.info("[command] device={} 指令 {} 重复 ACK，忽略", deviceId, commandId);
                return state.status();
            }
            state.markAcked(clock.nowMillis());
            log.info("[command] device={} 指令 {} 已 ACK", deviceId, commandId);
            return state.status();
        }
    }

    /** 设备回报执行结果；重复结果幂等。 */
    public CommandStatus complete(String deviceId, String commandId, boolean success, String reason) {
        Object lock = lockFor(deviceId);
        synchronized (lock) {
            CommandState state = states.get(key(deviceId, commandId));
            if (state == null) {
                throw new IllegalArgumentException("未知指令: " + commandId);
            }
            if (state.terminal()) {
                log.info("[command] device={} 指令 {} 已终态 {}，重复结果忽略",
                        deviceId, commandId, state.status());
                return state.status();
            }
            if (success) {
                state.markSucceeded();
            } else {
                state.markFailed(CommandStatus.FAILED_DEVICE_REJECTED, reason);
            }
            log.info("[command] device={} 指令 {} -> {}", deviceId, commandId, state.status());
            return state.status();
        }
    }

    /** 设备声明不支持（亦可在 ACK 前主动上报）。 */
    public CommandStatus reportUnsupported(String deviceId, String commandId) {
        Object lock = lockFor(deviceId);
        synchronized (lock) {
            CommandState state = requireState(deviceId, commandId);
            if (!state.terminal()) {
                state.markFailed(CommandStatus.FAILED_UNSUPPORTED, "设备回报不支持");
            }
            return state.status();
        }
    }

    /** 扫描已送达但超时未 ACK 的指令（测试可显式推进时钟后调用）。 */
    public int sweepTimeouts() {
        long now = clock.nowMillis();
        int count = 0;
        for (CommandState state : new ArrayList<>(states.values())) {
            synchronized (lockFor(state.command().deviceId())) {
                if (state.status() == CommandStatus.SENT
                        && now - state.sentAtMillis() > ackTimeoutMillis) {
                    state.markFailed(CommandStatus.FAILED_TIMEOUT,
                            "超过 " + ackTimeoutMillis + "ms 未收到 ACK");
                    count++;
                    log.warn("[command] device={} 指令 {} 超时未 ACK -> FAILED_TIMEOUT",
                            state.command().deviceId(), state.command().commandId());
                }
            }
        }
        return count;
    }

    public Optional<CommandState> state(String deviceId, String commandId) {
        return Optional.ofNullable(states.get(key(deviceId, commandId)));
    }

    public List<CommandState> queuedStates(String deviceId) {
        Deque<Command> queue = deviceQueues.get(deviceId);
        if (queue == null) {
            return List.of();
        }
        List<CommandState> result = new ArrayList<>();
        for (Command command : queue) {
            result.add(states.get(key(deviceId, command.commandId())));
        }
        return result;
    }

    public Map<String, CommandStatus> snapshot(String deviceId) {
        Map<String, CommandStatus> snapshot = new LinkedHashMap<>();
        for (Map.Entry<String, CommandState> entry : states.entrySet()) {
            if (entry.getKey().startsWith(deviceId + "|")) {
                snapshot.put(entry.getValue().command().commandId(), entry.getValue().status());
            }
        }
        return snapshot;
    }

    private boolean isSupported(String deviceId, String commandName) {
        Set<String> capabilities = deviceCapabilities.get(deviceId);
        return capabilities == null || capabilities.contains(commandName);
    }

    private CommandState requireState(String deviceId, String commandId) {
        CommandState state = states.get(key(deviceId, commandId));
        if (state == null) {
            throw new IllegalArgumentException("未知指令: " + commandId);
        }
        return state;
    }

    private Object lockFor(String deviceId) {
        return deviceLocks.computeIfAbsent(deviceId, k -> new Object());
    }

    private String key(String deviceId, String commandId) {
        return deviceId + "|" + commandId;
    }
}
