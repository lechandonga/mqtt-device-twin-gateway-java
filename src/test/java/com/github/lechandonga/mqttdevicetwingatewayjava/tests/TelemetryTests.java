package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.AlertEvent;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.IngestResult;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetrySample;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryService;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.ThresholdRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：单位归一化、样本去重、时间戳回退拒绝、乱序遥测、阈值边沿触发（持续不重复告警、恢复再武装）。
 */
@DisplayName("遥测归一化与阈值告警")
class TelemetryTests {

    @RegisterExtension
    static final CoverageLoggerExtension COVERAGE = new CoverageLoggerExtension();

    private TelemetryService telemetry;

    @BeforeEach
    void setUp() {
        telemetry = new TelemetryService();
        telemetry.addRule("dev", new ThresholdRule("overheat", "temperature",
                ThresholdRule.Operator.GT, 40.0));
    }

    @Test
    @DisplayName("归一化：华氏度转摄氏度、km/h 转 m/s 后再更新状态")
    void normalization() {
        telemetry.ingestWithUnits("dev", "s1", 100, Map.of(
                "temperature", new TelemetryService.RawMetric(104, "F"),
                "speed", new TelemetryService.RawMetric(36, "km/h")));

        Map<String, Double> latest = telemetry.latestMetrics("dev");
        assertEquals(40.0, latest.get("temperature"), 0.001);
        assertEquals(10.0, latest.get("speed"), 0.001);
    }

    @Test
    @DisplayName("去重：同一 sampleId 重复上报被忽略，不触发重复告警")
    void duplicateSampleIgnored() {
        var first = telemetry.ingest("dev", new TelemetrySample("s2", 100, Map.of("temperature", 42.0)));
        var second = telemetry.ingest("dev", new TelemetrySample("s2", 100, Map.of("temperature", 42.0)));

        assertEquals(IngestResult.ACCEPTED, first.result());
        assertEquals(IngestResult.DUPLICATE, second.result());
        assertTrue(second.events().isEmpty());
    }

    @Test
    @DisplayName("时间戳回退与乱序遥测：旧时间戳样本被拒绝，最新结果不被污染")
    void staleTimestampRejected() {
        telemetry.ingest("dev", new TelemetrySample("s3", 200, Map.of("temperature", 30.0)));
        var stale = telemetry.ingest("dev", new TelemetrySample("s4", 150, Map.of("temperature", 99.0)));

        assertEquals(IngestResult.STALE_TIMESTAMP, stale.result());
        assertEquals(30.0, telemetry.latestMetrics("dev").get("temperature"));
        assertEquals(200L, telemetry.lastTimestamp("dev"));
    }

    @Test
    @DisplayName("边沿告警：越限只触发一次；条件持续期间不重复；恢复后可以再次触发")
    void edgeTriggeredAlerts() {
        var a1 = telemetry.ingest("dev", new TelemetrySample("t1", 1, Map.of("temperature", 41.0)));
        var a2 = telemetry.ingest("dev", new TelemetrySample("t2", 2, Map.of("temperature", 42.0)));
        var recover = telemetry.ingest("dev", new TelemetrySample("t3", 3, Map.of("temperature", 35.0)));
        var again = telemetry.ingest("dev", new TelemetrySample("t4", 4, Map.of("temperature", 43.0)));

        assertEquals(1, a1.events().size());
        assertTrue(a1.events().get(0).triggered());
        assertTrue(a2.events().isEmpty(), "持续越限不得重复告警");
        assertEquals(1, recover.events().size());
        assertFalse(recover.events().get(0).triggered());

        assertEquals(1, again.events().size(), "恢复正常后规则重新武装，可再次触发");
        assertTrue(again.events().get(0).triggered());
    }
}
