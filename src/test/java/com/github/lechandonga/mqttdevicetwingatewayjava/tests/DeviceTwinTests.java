package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.twin.DeviceTwinService;
import com.github.lechandonga.mqttdevicetwingatewayjava.twin.UpdateResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：期望态版本推进、过期版本冲突拒绝、重复上报幂等、并发写入不丢更新、期望/上报合并。
 */
@DisplayName("设备影子版本与冲突")
class DeviceTwinTests {

    @RegisterExtension
    static final CoverageLoggerExtension COVERAGE = new CoverageLoggerExtension();

    private final DeviceTwinService service = new DeviceTwinService();

    @Test
    @DisplayName("期望态按版本合并写入，版本号单调推进")
    void desiredUpdatesAdvanceVersion() {
        UpdateResult first = service.updateDesired("d1", Map.of("fan", "on"), 0);
        UpdateResult second = service.updateDesired("d1", Map.of("led", 1), first.newVersion());

        assertTrue(first.accepted());
        assertEquals(1, first.newVersion());
        assertEquals(2, second.newVersion());
        assertEquals("on", service.snapshot("d1").desired().get("fan"));
        assertEquals(1, service.snapshot("d1").desired().get("led"));
    }

    @Test
    @DisplayName("过期基准版本写入被拒绝，原因 VERSION_CONFLICT，不覆盖新值")
    void staleVersionRejected() {
        service.updateDesired("d2", Map.of("mode", "auto"), 0);
        UpdateResult stale = service.updateDesired("d2", Map.of("mode", "manual"), 0);

        assertFalse(stale.accepted());
        assertEquals("VERSION_CONFLICT", stale.reason());
        assertEquals("auto", service.snapshot("d2").desired().get("mode"), "旧值不得覆盖新值");
    }

    @Test
    @DisplayName("并发写入：只有持最新版本者成功，其余得到 VERSION_CONFLICT，更新不丢")
    void concurrentWritesNoLostUpdate() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(20);
        try {
            int accepted = 0;
            int conflicts = 0;
            Future<?>[] futures = new Future[20];
            java.util.List<UpdateResult> results = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            for (int i = 0; i < 20; i++) {
                final int seq = i;
                futures[i] = pool.submit((Callable<Void>) () -> {
                    results.add(service.updateDesired("d3", Map.of("v" + seq, seq), 0));
                    return null;
                });
            }
            for (Future<?> f : futures) {
                f.get();
            }
            for (UpdateResult r : results) {
                if (r.accepted()) {
                    accepted++;
                } else if ("VERSION_CONFLICT".equals(r.reason())) {
                    conflicts++;
                }
            }
            assertEquals(1, accepted, "同一过期基准下只允许一个写入成功");
            assertEquals(19, conflicts, "其余并发写入必须得到可区分的冲突原因");

            long latest = service.snapshot("d3").desiredVersion().values().stream()
                    .mapToLong(Long::longValue).max().orElseThrow();
            UpdateResult retry = service.updateDesired("d3", Map.of("final", true), latest);
            assertTrue(retry.accepted(), "拿最新版本重试可以成功，证明没有更新丢失");
        } finally {
            pool.shutdown();
        }
    }

    @Test
    @DisplayName("上报态：时间/版本回退拒绝；相同版本相同内容幂等 STALE_DUPLICATE")
    void reportedStaleAndDuplicate() {
        UpdateResult r1 = service.reportState("d4", Map.of("temp", 36.5), 1);
        UpdateResult dupSame = service.reportState("d4", Map.of("temp", 36.5), 1);
        UpdateResult staleDiff = service.reportState("d4", Map.of("temp", 0.0), 0);

        assertTrue(r1.accepted());
        assertFalse(dupSame.accepted());
        assertEquals("STALE_DUPLICATE", dupSame.reason());
        assertEquals("VERSION_CONFLICT", staleDiff.reason());
        assertEquals(36.5, service.snapshot("d4").reported().get("temp"));
    }

    @Test
    @DisplayName("期望态与上报态按字段合并并标 inSync")
    void mergeDesiredAndReported() {
        service.updateDesired("d5", Map.of("fan", "on", "led", 1), 0);
        service.reportState("d5", Map.of("fan", "on"), 1);

        Map<String, Object> merged = service.getMerged("d5");
        assertEquals(true, ((Map<?, ?>) merged.get("fan")).get("inSync"));
        assertEquals(false, ((Map<?, ?>) merged.get("led")).get("inSync"));
    }
}
