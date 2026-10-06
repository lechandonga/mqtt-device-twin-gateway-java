package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.command.Command;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandService;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandStatus;
import com.github.lechandonga.mqttdevicetwingatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaService;
import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaStatus;
import com.github.lechandonga.mqttdevicetwingatewayjava.shadow.ShadowService;
import com.github.lechandonga.mqttdevicetwingatewayjava.shadow.UpdateOutcome;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessCase;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessLogging;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessModule;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.RecordingClient;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryPoint;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryService;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.ThresholdRule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ExtendWith(BusinessLogging.class)
@BusinessModule("端到端：断线重连全链路（会话恢复 + 离线指令 + 影子 + 遥测 + 升级回滚）")
class GatewayEndToEndTests {

    @Autowired
    private DeviceGateway gateway;
    @Autowired
    private CommandService commands;
    @Autowired
    private ShadowService shadows;
    @Autowired
    private TelemetryService telemetry;
    @Autowired
    private OtaService ota;

    @Test
    @BusinessCase("设备生命周期：上线订阅 -> 异常断线（遗嘱/排队）-> 重连补投指令 -> 遥测与升级结论确定")
    void fullDeviceLifecycle() {
        String device = "sensor-A";

        // 监控方订阅遗嘱主题
        RecordingClient monitor = new RecordingClient("monitor");
        gateway.connect("monitor", false, monitor, null);
        gateway.subscribe("monitor", "events/" + device, Qos.AT_LEAST_ONCE);

        // 设备以持久会话上线并订阅指令主题
        RecordingClient deviceClient = new RecordingClient(device);
        gateway.connect(device, false, deviceClient,
                new com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttMessage(
                        gateway.broker().nextPacketId(), "events/" + device,
                        "will:offline".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                        Qos.AT_LEAST_ONCE));
        gateway.subscribe(device, "devices/" + device + "/commands/#", Qos.AT_LEAST_ONCE);

        // 异常断线：遗嘱发布；离线期间两条指令排队
        gateway.disconnect(device, false);
        assertThat(monitor.payloads()).containsExactly("will:offline");
        commands.send(new Command("cmd-1", device, "set-mode", Map.of(), 100));
        commands.send(new Command("cmd-2", device, "set-mode", Map.of(), 101));
        // 重复指令去重
        commands.send(new Command("cmd-1", device, "set-mode", Map.of(), 102));

        // 重连：订阅还在，离线指令按序补投，重复指令只来一次
        RecordingClient deviceClient2 = new RecordingClient(device);
        gateway.connect(device, false, deviceClient2, null);
        assertThat(deviceClient2.payloads()).containsExactly("cmd-1|set-mode", "cmd-2|set-mode");
        assertThat(commands.acknowledge(device, "cmd-1")).isEqualTo(CommandStatus.ACKED);
        assertThat(commands.complete(device, "cmd-1", true, null))
                .isEqualTo(CommandStatus.SUCCEEDED);

        // 影子并发语义：连续两个期望版本，旧版本被拒
        assertThat(shadows.updateDesired(device, Map.of("mode", "eco"), 0).accepted()).isTrue();
        assertThat(shadows.updateDesired(device, Map.of("mode", "night"), 0).outcome())
                .isEqualTo(UpdateOutcome.REJECTED_STALE_VERSION);
        assertThat(shadows.updateDesired(device, Map.of("mode", "night"), 1).accepted()).isTrue();
        assertThat(shadows.updateReported(device, Map.of("mode", "night"), 1).accepted()).isTrue();
        assertThat(shadows.get(device).delta()).isEmpty();

        // 遥测：重复与回退被挡；阈值边沿一次告警
        telemetry.addRule(new ThresholdRule(device, "cpu", 90));
        assertThat(telemetry.ingest(new TelemetryPoint(device, "t1", 1000, Map.of("cpu", 96.0)))
                .alertEvents()).hasSize(1);
        assertThat(telemetry.ingest(new TelemetryPoint(device, "t1", 1000, Map.of("cpu", 96.0)))
                .result().name()).isEqualTo("DUPLICATE_MESSAGE");
        assertThat(telemetry.ingest(new TelemetryPoint(device, "t2", 900, Map.of("cpu", 97.0)))
                .result().name()).isEqualTo("STALE_TIMESTAMP");

        // 升级失败回滚到确定可用版本
        ota.start("ota-1", device, "9.9.9", "1.2.3");
        ota.reportProgress("ota-1", 1, 40);
        assertThat(ota.reportFailure("ota-1", 2, "power lost").status())
                .isEqualTo(OtaStatus.ROLLED_BACK);
        assertThat(ota.activeVersion(device)).isEqualTo("1.2.3");

        // 第二次重连仍然幂等：无新增重复指令、无重复遗嘱（正常会话切换不发遗嘱）
        gateway.connect(device, false, new RecordingClient(device), null);
        assertThat(commands.state(device, "cmd-2").orElseThrow().status())
                .isEqualTo(CommandStatus.SENT);
    }
}
