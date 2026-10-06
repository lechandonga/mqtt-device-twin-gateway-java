package com.github.lechandonga.mqttdevicetwingatewayjava.command;

import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.ConnectionListener;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 指令下发服务（基于内存 broker 的 QoS1 通道）。
 * <ul>
 *     <li>commandId 全局幂等：重复指令直接拒绝，不重复排队/下发。</li>
 *     <li>设备离线：指令进入设备队列；重连后按创建顺序（FIFO）送达。</li>
 *     <li>设备不支持的指令立即给出 FAILED_UNSUPPORTED 终态，不入队。</li>
 *     <li>超时未确认：调度任务将状态置为 FAILED_TIMEOUT，后续迟到确认不再改写终态。</li>
 * </ul>
 */
@Component
public class CommandService {

    private static final Logger log = LoggerFactory.getLogger(CommandService.class);
    private static final long DEFAULT_TIMEOUT_MS = 2000L;

    private final InMemoryMqttBroker broker;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "command-timeout");
                t.setDaemon(true);
                return t;
            });

    private final Map<String, Set<String>> capabilities = new ConcurrentHashMap<>();
    private final Map<String, CommandRecord> commands = new ConcurrentHashMap<>();
    private final Map<String, List<String>> deviceQueues = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> timeouts = new ConcurrentHashMap<>();

    public CommandService(InMemoryMqttBroker broker) {
        this.broker = broker;
        broker.addConnectionListener(new ConnectionListener() {
            @Override
            public void onConnected(String clientId, boolean sessionPresent) {
                drainQueue(clientId);
            }
        });
    }

    /** 注册设备支持的指令集合。 */
    public void registerDevice(String deviceId, Set<String> supportedCommands) {
        capabilities.put(deviceId, Set.copyOf(supportedCommands));
        deviceQueues.putIfAbsent(deviceId, new ArrayList<>());
    }

    /**
     * 下发指令。
     *
     * @param timeoutMs <=0 表示使用默认超时
     */
    public CommandResult sendCommand(String deviceId, String commandId, String commandName,
                                     String payload, long timeoutMs) {
        if (commands.containsKey(commandId)) {
            log.warn("[CMD] 重复指令 commandId={} device='{}'，幂等拒绝", commandId, deviceId);
            return new CommandResult(false, true, commands.get(commandId).status(), commandId);
        }
        Set<String> supported = capabilities.getOrDefault(deviceId, Set.of());
        if (!supported.contains(commandName)) {
            CommandRecord rec = new CommandRecord(commandId, deviceId, commandName, payload,
                    CommandStatus.FAILED_UNSUPPORTED);
            rec.detail("device does not support command: " + commandName);
            commands.put(commandId, rec);
            log.warn("[CMD] 指令 {} 不被设备 '{}' 支持，立即失败", commandId, deviceId);
            return new CommandResult(false, false, CommandStatus.FAILED_UNSUPPORTED, commandId);
        }
        boolean online = broker.isOnline(deviceId);
        CommandRecord rec = new CommandRecord(commandId, deviceId, commandName, payload,
                online ? CommandStatus.DELIVERED : CommandStatus.QUEUED);
        commands.put(commandId, rec);
        deviceQueues.computeIfAbsent(deviceId, k -> new ArrayList<>());

        String wirePayload = encode(commandId, commandName, payload);
        if (online) {
            publish(deviceId, rec, wirePayload);
        } else {
            deviceQueues.get(deviceId).add(commandId);
            log.info("[CMD] 设备 '{}' 离线，指令 {} 排队（队列长度 {}）",
                    deviceId, commandId, deviceQueues.get(deviceId).size());
        }
        scheduleTimeout(commandId, timeoutMs <= 0 ? DEFAULT_TIMEOUT_MS : timeoutMs);
        return new CommandResult(true, false, rec.status(), commandId);
    }

    /** 设备确认收到/完成指令。终态（超时/不支持）之后的迟到确认不改变结论。 */
    public boolean acknowledge(String commandId) {
        CommandRecord rec = commands.get(commandId);
        if (rec == null) {
            return false;
        }
        synchronized (rec) {
            if (rec.status() == CommandStatus.ACKED) {
                log.info("[CMD] 指令 {} 重复确认，忽略", commandId);
                return true;
            }
            if (rec.status() == CommandStatus.FAILED_TIMEOUT
                    || rec.status() == CommandStatus.FAILED_UNSUPPORTED) {
                log.warn("[CMD] 指令 {} 已处于终态 {}，迟到确认不改变结论", commandId, rec.status());
                return false;
            }
            rec.status(CommandStatus.ACKED);
            cancelTimeout(commandId);
            log.info("[CMD] 指令 {} 已被设备 '{}' 确认", commandId, rec.deviceId());
            return true;
        }
    }

    /** 重连后调用（连接监听器也会自动调用）：FIFO 送达排队指令。 */
    public int drainQueue(String deviceId) {
        if (!broker.isOnline(deviceId)) {
            return 0;
        }
        List<String> queue = deviceQueues.get(deviceId);
        if (queue == null || queue.isEmpty()) {
            return 0;
        }
        int drained = 0;
        List<String> snapshot;
        synchronized (queue) {
            snapshot = new ArrayList<>(queue);
        }
        for (String commandId : snapshot) {
            CommandRecord rec = commands.get(commandId);
            if (rec == null || rec.status() != CommandStatus.QUEUED) {
                removeQueued(queue, commandId);
                continue;
            }
            synchronized (rec) {
                publish(deviceId, rec, encode(rec.commandId(), rec.name(), rec.payload()));
                rec.status(CommandStatus.DELIVERED);
            }
            removeQueued(queue, commandId);
            drained++;
        }
        log.info("[CMD] 设备 '{}' 重连后按序送达排队指令 {} 条", deviceId, drained);
        return drained;
    }

    public CommandStatus statusOf(String commandId) {
        CommandRecord rec = commands.get(commandId);
        return rec == null ? null : rec.status();
    }

    public CommandRecord recordOf(String commandId) {
        return commands.get(commandId);
    }

    public int queuedCount(String deviceId) {
        List<String> queue = deviceQueues.get(deviceId);
        return queue == null ? 0 : queue.size();
    }

    public static Map<String, String> decode(String wirePayload) {
        String json = new String(Base64.getDecoder().decode(wirePayload));
        String body = json.substring(1, json.length() - 1);
        Map<String, String> map = new LinkedHashMap<>();
        for (String part : body.split(",", -1)) {
            int idx = part.indexOf('=');
            if (idx > 0) {
                map.put(part.substring(0, idx), part.substring(idx + 1));
            }
        }
        return map;
    }

    private static String encode(String commandId, String name, String payload) {
        String safePayload = payload == null ? "" : payload.replace(",", " ");
        String json = "{commandId=" + commandId + ",name=" + name + ",payload=" + safePayload + "}";
        return Base64.getEncoder().encodeToString(json.getBytes());
    }

    private void publish(String deviceId, CommandRecord rec, String wirePayload) {
        rec.incrementAttempts();
        broker.publish("cmd/" + deviceId, wirePayload, Qos.AT_LEAST_ONCE, rec.commandId());
        log.info("[CMD] 下发指令 {} ({}) 到设备 '{}'，第 {} 次投递",
                rec.commandId(), rec.name(), deviceId, rec.deliveryAttempts());
    }

    private void removeQueued(List<String> queue, String commandId) {
        synchronized (queue) {
            queue.remove(commandId);
        }
    }

    private void scheduleTimeout(String commandId, long timeoutMs) {
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            CommandRecord rec = commands.get(commandId);
            if (rec == null) {
                return;
            }
            synchronized (rec) {
                if (rec.status() == CommandStatus.QUEUED || rec.status() == CommandStatus.DELIVERED) {
                    rec.status(CommandStatus.FAILED_TIMEOUT);
                    rec.detail("no acknowledgement within " + timeoutMs + "ms");
                    List<String> queue = deviceQueues.get(rec.deviceId());
                    if (queue != null) {
                        removeQueued(queue, commandId);
                    }
                    log.warn("[CMD] 指令 {} 在 {}ms 内未确认，判定超时失败", commandId, timeoutMs);
                }
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
        timeouts.put(commandId, future);
    }

    private void cancelTimeout(String commandId) {
        ScheduledFuture<?> future = timeouts.remove(commandId);
        if (future != null) {
            future.cancel(false);
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
