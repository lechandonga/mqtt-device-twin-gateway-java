package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandService;
import com.github.lechandonga.mqttdevicetwingatewayjava.firmware.FirmwareUpgradeService;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryService;
import com.github.lechandonga.mqttdevicetwingatewayjava.twin.DeviceTwinService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 业务覆盖：Spring 容器启动，网关各核心 Bean 装配可用。
 */
@SpringBootTest
@DisplayName("应用装配")
class ApplicationTests {

    @RegisterExtension
    static final CoverageLoggerExtension COVERAGE = new CoverageLoggerExtension();

    @Autowired
    private InMemoryMqttBroker broker;
    @Autowired
    private DeviceTwinService twinService;
    @Autowired
    private CommandService commandService;
    @Autowired
    private TelemetryService telemetryService;
    @Autowired
    private FirmwareUpgradeService firmwareUpgradeService;

    @Test
    @DisplayName("上下文加载：MQTT/影子/指令/遥测/升级五个核心组件均装配")
    void contextLoads() {
        assertNotNull(broker);
        assertNotNull(twinService);
        assertNotNull(commandService);
        assertNotNull(telemetryService);
        assertNotNull(firmwareUpgradeService);
    }
}
