package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandResult;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandService;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandStatus;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：在线下发确认、离线排队 FIFO 重放、重复指令去重、超时失败终态、不支持指令失败、迟到确认不改终态。
 */
@DisplayName("指令下发与离线队列")
class CommandTests {

    @RegisterExtension
    static final CoverageLoggerExtension COVERAGE = new CoverageLoggerExtension();

    private InMemoryMqttBroker broker;
    private CommandService commands;

    @BeforeEach
    void setUp() {
        broker = new InMemoryMqttBroker();
        commands = new CommandService(broker);
        commands.registerDevice("dev", Set.of("reboot", "config"));
    }

    private List<String> attachDevice(String clientId) {
        List<String> raw = new ArrayList<>();
        broker.connect(clientId, false);
        broker.subscribe(clientId, "cmd/" + clientId, Qos.AT_LEAST_ONCE,
                (t, p, q, dup) -> raw.add(p));
        return raw;
    }

    @Test
    @DisplayName("在线下发：设备收到后确认即完成；重复确认无副作用")
    void onlineDeliverAndAck() {
        attachDevice("dev");
        CommandResult result = commands.sendCommand("dev", "cmd-1", "reboot", null, 0);

        assertTrue(result.accepted());
        assertEquals(CommandStatus.DELIVERED, result.status());
        assertTrue(commands.acknowledge("cmd-1"));
        assertEquals(CommandStatus.ACKED, commands.statusOf("cmd-1"));
        assertTrue(commands.acknowledge("cmd-1"), "重复确认幂等返回，状态不变");
        assertEquals(CommandStatus.ACKED, commands.statusOf("cmd-1"));
    }

    @Test
    @DisplayName("离线排队：离线期间指令 QUEUED，重连后按序 FIFO 送达并可确认")
    void offlineQueuedThenFifoOnReconnect() {
        List<String> raw = attachDevice("dev");
        broker.connectionLost("dev");

        commands.sendCommand("dev", "c1", "reboot", null, 0);
        commands.sendCommand("dev", "c2", "config", "x=1", 0);
        assertEquals(2, commands.queuedCount("dev"));
        assertEquals(CommandStatus.QUEUED, commands.statusOf("c1"));

        broker.connect("dev", false);
        commands.drainQueue("dev");
        assertEquals(0, commands.queuedCount("dev"));
        assertEquals(List.of(
                CommandService.decode(raw.get(0)).get("commandId"),
                CommandService.decode(raw.get(1)).get("commandId")), List.of("c1", "c2"),
                "重连后必须按排队顺序送达");
        assertEquals(CommandStatus.DELIVERED, commands.statusOf("c2"));
    }

    @Test
    @DisplayName("重复 commandId 被幂等拒绝，不会重复排队/下发")
    void duplicateCommandDeduped() {
        broker.connect("dev", false);
        broker.connectionLost("dev");

        assertTrue(commands.sendCommand("dev", "dup", "reboot", null, 0).accepted());
        CommandResult duplicate = commands.sendCommand("dev", "dup", "reboot", null, 0);

        assertFalse(duplicate.accepted());
        assertTrue(duplicate.duplicate());
        assertEquals(1, commands.queuedCount("dev"));
    }

    @Test
    @DisplayName("设备不支持的指令立即失败 FAILED_UNSUPPORTED，不入队")
    void unsupportedCommandFailsImmediately() {
        CommandResult result = commands.sendCommand("dev", "c-x", "factory-reset", null, 0);
        assertFalse(result.accepted());
        assertEquals(CommandStatus.FAILED_UNSUPPORTED, result.status());
        assertEquals(0, commands.queuedCount("dev"));
    }

    @Test
    @DisplayName("超时未确认得到 FAILED_TIMEOUT 终态，之后迟到的确认不能翻案")
    void timeoutIsFinal() throws InterruptedException {
        attachDevice("dev");
        commands.sendCommand("dev", "slow", "reboot", null, 80);
        assertEquals(CommandStatus.DELIVERED, commands.statusOf("slow"));

        Thread.sleep(250);
        assertEquals(CommandStatus.FAILED_TIMEOUT, commands.statusOf("slow"));
        assertFalse(commands.acknowledge("slow"), "迟到确认不得改变超时终态");
        assertEquals(CommandStatus.FAILED_TIMEOUT, commands.statusOf("slow"));
    }

    @Test
    @DisplayName("排队期间超时的指令在重连时被剔除，不会再下发")
    void queuedCommandTimedOutIsSkipped() throws InterruptedException {
        List<String> raw = attachDevice("dev");
        broker.connectionLost("dev");
        commands.sendCommand("dev", "expire", "reboot", null, 80);
        Thread.sleep(200);

        broker.connect("dev", false);
        commands.drainQueue("dev");
        assertEquals(CommandStatus.FAILED_TIMEOUT, commands.statusOf("expire"));
        assertTrue(raw.isEmpty());
    }
}
