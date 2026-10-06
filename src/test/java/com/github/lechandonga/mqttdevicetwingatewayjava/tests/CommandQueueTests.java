package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.command.Command;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandService;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandStatus;
import com.github.lechandonga.mqttdevicetwingatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessCase;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessLogging;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessModule;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.RecordingClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(BusinessLogging.class)
@BusinessModule("指令下发 / 离线队列 / 幂等去重 / 超时与不支持结论")
class CommandQueueTests {

    private InMemoryMqttBroker broker;
    private CommandService commands;
    private DeviceGateway gateway;
    private final AtomicLong clock = new AtomicLong(1_000);

    @BeforeEach
    void setUp() {
        broker = new InMemoryMqttBroker();
        commands = new CommandService(broker);
        commands.setClock(clock::get);
        commands.setAckTimeoutMillis(5_000);
        gateway = new DeviceGateway(broker, commands);
    }

    private Command command(String deviceId, String commandId, String name) {
        return new Command(commandId, deviceId, name, Map.of(), clock.get());
    }

    private void connect(String deviceId) {
        gateway.connect(deviceId, false, new RecordingClient(deviceId), null);
        gateway.subscribe(deviceId, "devices/" + deviceId + "/commands/#", Qos.AT_LEAST_ONCE);
    }

    @Test
    @BusinessCase("离线期间多条指令 FIFO 排队，重连后严格按入队顺序送达")
    void offlineCommandsDeliverInOrderOnReconnect() {
        connect("dev-1");
        gateway.disconnect("dev-1", false);

        commands.send(command("dev-1", "c1", "reboot"));
        commands.send(command("dev-1", "c2", "set-led"));
        commands.send(command("dev-1", "c3", "reboot"));

        assertThat(commands.queuedStates("dev-1"))
                .extracting(s -> s.command().commandId())
                .containsExactly("c1", "c2", "c3");

        RecordingClient device = new RecordingClient("dev-1");
        gateway.connect("dev-1", false, device, null);

        assertThat(device.payloads()).containsExactly("c1|reboot", "c2|set-led", "c3|reboot");
        assertThat(commands.state("dev-1", "c1").orElseThrow().status())
                .isEqualTo(CommandStatus.SENT);
    }

    @Test
    @BusinessCase("重复指令（同 commandId）幂等：只入队/送达一次，返回同一状态对象")
    void duplicateCommandIdIsIdempotent() {
        connect("dev-2");
        Command original = command("dev-2", "dup-1", "reboot");
        var s1 = commands.send(original);
        var s2 = commands.send(command("dev-2", "dup-1", "reboot"));
        var s3 = commands.send(command("dev-2", "dup-1", "reboot"));

        assertThat(s1).isSameAs(s2).isSameAs(s3);
        assertThat(s1.deliveryAttempts()).isEqualTo(1);
    }

    @Test
    @BusinessCase("重复指令在离线队列中也只保留一条")
    void duplicateWhileOfflineQueuedOnce() {
        connect("dev-2b");
        gateway.disconnect("dev-2b", false);
        commands.send(command("dev-2b", "x", "reboot"));
        commands.send(command("dev-2b", "x", "reboot"));
        commands.send(command("dev-2b", "x", "reboot"));
        assertThat(commands.queuedStates("dev-2b")).hasSize(1);

        RecordingClient device = new RecordingClient("dev-2b");
        gateway.connect("dev-2b", false, device, null);
        assertThat(device.payloads()).containsExactly("x|reboot");
    }

    @Test
    @BusinessCase("指令送达后超时未 ACK -> FAILED_TIMEOUT，结论明确")
    void timeoutProducesExplicitFailure() {
        connect("dev-3");
        commands.send(command("dev-3", "t1", "reboot"));
        clock.addAndGet(4_999);
        assertThat(commands.sweepTimeouts()).isZero();
        clock.addAndGet(2);
        assertThat(commands.sweepTimeouts()).isEqualTo(1);
        assertThat(commands.state("dev-3", "t1").orElseThrow().status())
                .isEqualTo(CommandStatus.FAILED_TIMEOUT);
        // 超时后的迟到 ACK 不会把状态改回去（已终态）
        commands.acknowledge("dev-3", "t1");
        assertThat(commands.state("dev-3", "t1").orElseThrow().status())
                .isEqualTo(CommandStatus.FAILED_TIMEOUT);
    }

    @Test
    @BusinessCase("设备不支持的指令立即 FAILED_UNSUPPORTED，且不会送达/入队")
    void unsupportedCommandFailsImmediately() {
        commands.registerCapabilities("dev-4", Set.of("set-led"));
        connect("dev-4");
        var state = commands.send(command("dev-4", "u1", "factory-reset"));
        assertThat(state.status()).isEqualTo(CommandStatus.FAILED_UNSUPPORTED);

        gateway.disconnect("dev-4", false);
        var queued = commands.send(command("dev-4", "u2", "factory-reset"));
        assertThat(queued.status()).isEqualTo(CommandStatus.FAILED_UNSUPPORTED);
        assertThat(commands.queuedStates("dev-4")).isEmpty();
    }

    @Test
    @BusinessCase("ACK 与执行结果重复上报幂等；设备拒绝执行给 FAILED_DEVICE_REJECTED")
    void duplicateAcksAndResultsAreIdempotent() {
        connect("dev-5");
        commands.send(command("dev-5", "r1", "set-led"));

        assertThat(commands.acknowledge("dev-5", "r1")).isEqualTo(CommandStatus.ACKED);
        assertThat(commands.acknowledge("dev-5", "r1")).isEqualTo(CommandStatus.ACKED);
        assertThat(commands.complete("dev-5", "r1", false, "busy"))
                .isEqualTo(CommandStatus.FAILED_DEVICE_REJECTED);
        assertThat(commands.complete("dev-5", "r1", true, null))
                .isEqualTo(CommandStatus.FAILED_DEVICE_REJECTED);
    }

    @Test
    @BusinessCase("设备主动回报不支持 -> FAILED_UNSUPPORTED")
    void deviceReportsUnsupported() {
        connect("dev-6");
        commands.send(command("dev-6", "r2", "custom-x"));
        assertThat(commands.reportUnsupported("dev-6", "r2"))
                .isEqualTo(CommandStatus.FAILED_UNSUPPORTED);
        assertThat(commands.state("dev-6", "r2").orElseThrow().failureReason())
                .contains("不支持");
    }
}
