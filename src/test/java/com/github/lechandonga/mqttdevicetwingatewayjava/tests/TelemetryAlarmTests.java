package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.AlertEvent;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.IngestResult;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryPoint;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryService;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.ThresholdRule;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessCase;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessLogging;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(BusinessLogging.class)
@BusinessModule("遥测处理 / 归一化去重 / 时间戳保护 / 阈值边沿告警")
class TelemetryAlarmTests {

    private TelemetryService telemetry;

    @BeforeEach
    void setUp() {
        telemetry = new TelemetryService();
        telemetry.addRule(new ThresholdRule("dev", "cpu", 90, 5));
    }

    private TelemetryPoint point(String id, long ts, double cpu) {
        return new TelemetryPoint("dev", id, ts, Map.of("cpu", cpu));
    }

    @Test
    @BusinessCase("归一化：指标名 trim+小写、非法 NaN/Infinity 被剔除")
    void normalizationTrimsNamesAndDropsInvalidValues() {
        var result = telemetry.ingest(new TelemetryPoint("dev", "m1", 1,
                Map.of(" CPU ", 42.0, "bad", Double.NaN, "bad2", Double.POSITIVE_INFINITY)));
        assertThat(result.accepted()).isTrue();
        assertThat(telemetry.latestMetric("dev", "cpu")).isEqualTo(42.0);
        assertThat(telemetry.latestMetric("dev", "bad")).isNull();
        assertThat(telemetry.latestMetric("dev", "bad2")).isNull();
    }

    @Test
    @BusinessCase("重复 messageId（重复投递）幂等：只生效一次，不重复告警")
    void duplicateMessageIsIdempotent() {
        var first = telemetry.ingest(point("dup", 10, 95));
        var again = telemetry.ingest(point("dup", 10, 95));
        var third = telemetry.ingest(point("dup", 10, 99));

        assertThat(first.result()).isEqualTo(IngestResult.ACCEPTED);
        assertThat(again.result()).isEqualTo(IngestResult.DUPLICATE_MESSAGE);
        assertThat(third.result()).isEqualTo(IngestResult.DUPLICATE_MESSAGE);
        assertThat(telemetry.latestMetric("dev", "cpu")).isEqualTo(95);
        assertThat(telemetry.alertHistory("dev")).hasSize(1);
    }

    @Test
    @BusinessCase("乱序（时间戳回退）遥测被拒绝，最新状态不被旧数据污染")
    void staleTimestampCannotOverwriteLatest() {
        telemetry.ingest(point("new", 100, 60));
        var old = telemetry.ingest(point("old", 50, 5));
        var equal = telemetry.ingest(point("same", 100, 88));

        assertThat(old.result()).isEqualTo(IngestResult.STALE_TIMESTAMP);
        // 相等时间戳允许（同批指标），不会误判回退
        assertThat(equal.result()).isEqualTo(IngestResult.ACCEPTED);
        assertThat(telemetry.latestTimestamp("dev")).isEqualTo(100);
        assertThat(telemetry.latestMetric("dev", "cpu")).isEqualTo(88);
    }

    @Test
    @BusinessCase("边沿触发：越限只告警一次，持续高位不重复；恢复后可再次告警（含迟滞）")
    void alertFiresOnceAndRearmsAfterRecovery() {
        assertThat(telemetry.ingest(point("a1", 1, 95)).alertEvents())
                .singleElement().matches(e -> ((AlertEvent) e).firing());
        // 持续超阈值：无新事件
        assertThat(telemetry.ingest(point("a2", 2, 96)).alertEvents()).isEmpty();
        assertThat(telemetry.ingest(point("a3", 3, 99)).alertEvents()).isEmpty();
        assertThat(telemetry.isFiring("dev", "cpu")).isTrue();

        // 回到 87（>85 恢复线，迟滞带内）仍不恢复
        assertThat(telemetry.ingest(point("a4", 4, 87)).alertEvents()).isEmpty();
        assertThat(telemetry.ingest(point("a5", 5, 85)).alertEvents())
                .singleElement().matches(e -> ((AlertEvent) e).recovered());

        // 再次越限可以再次告警
        assertThat(telemetry.ingest(point("a6", 6, 92)).alertEvents())
                .singleElement().matches(e -> ((AlertEvent) e).firing());

        List<AlertEvent> history = telemetry.alertHistory("dev");
        assertThat(history).extracting(AlertEvent::type)
                .containsExactly("FIRED", "RECOVERED", "FIRED");
    }

    @Test
    @BusinessCase("并发上报：重复消息只入库一次，相同时间戳的不同消息都保留")
    void concurrentDuplicateMessagesIngestedOnce() throws Exception {
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            int index = i % 4;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                telemetry.ingest(point("m" + index, 100L + index, 50 + index));
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // m0..m3 每个 messageId 只被消费一次：值为各自身份，无异常覆盖
        assertThat(telemetry.latestMetric("dev", "cpu")).isEqualTo(53);
        assertThat(telemetry.alertHistory("dev")).isEmpty();
    }
}
