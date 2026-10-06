package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.shadow.DeviceShadow;
import com.github.lechandonga.mqttdevicetwingatewayjava.shadow.ShadowService;
import com.github.lechandonga.mqttdevicetwingatewayjava.shadow.UpdateOutcome;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessCase;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessLogging;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(BusinessLogging.class)
@BusinessModule("设备影子 / 版本合并 / 并发冲突（过期拒绝、重复幂等、不丢更新）")
class DeviceShadowTests {

    @Test
    @BusinessCase("期望态浅合并并单调递增版本；过期 expectedVersion 被拒绝且原因可区分")
    void desiredMergeAndStaleVersionRejected() {
        DeviceShadow shadow = new DeviceShadow("d");

        DeviceShadow.UpdateResult first =
                shadow.updateDesired(Map.of("power", "on", "brightness", 50), 0);
        assertThat(first.outcome()).isEqualTo(UpdateOutcome.ACCEPTED);
        assertThat(first.currentVersion()).isEqualTo(1);

        DeviceShadow.UpdateResult second =
                shadow.updateDesired(Map.of("brightness", 80), 1);
        assertThat(second.accepted()).isTrue();
        assertThat(shadow.desired()).containsEntry("power", "on").containsEntry("brightness", 80);

        // 另一个并发请求仍拿着旧版本 1
        DeviceShadow.UpdateResult stale = shadow.updateDesired(Map.of("brightness", 10), 1);
        assertThat(stale.outcome()).isEqualTo(UpdateOutcome.REJECTED_STALE_VERSION);
        assertThat(stale.currentVersion()).isEqualTo(2);
        // 过期写入未污染状态
        assertThat(shadow.desired()).containsEntry("brightness", 80);
    }

    @Test
    @BusinessCase("上报态：版本连续才合并；重复版本幂等、过期版本拒绝，原因不同")
    void reportedDuplicateAndStaleVersionsAreDistinguished() {
        DeviceShadow shadow = new DeviceShadow("d");

        assertThat(shadow.updateReported(Map.of("temp", 30), 1).accepted()).isTrue();
        DeviceShadow.UpdateResult duplicate = shadow.updateReported(Map.of("temp", 30), 1);
        assertThat(duplicate.outcome()).isEqualTo(UpdateOutcome.REJECTED_DUPLICATE_VERSION);

        DeviceShadow.UpdateResult stale = shadow.updateReported(Map.of("temp", 25), 1);
        assertThat(stale.outcome()).isEqualTo(UpdateOutcome.REJECTED_DUPLICATE_VERSION);

        assertThat(shadow.updateReported(Map.of("temp", 31), 2).accepted()).isTrue();
        DeviceShadow.UpdateResult oldArrival = shadow.updateReported(Map.of("temp", 1), 1);
        assertThat(oldArrival.outcome()).isEqualTo(UpdateOutcome.REJECTED_STALE_VERSION);
        assertThat(shadow.reported()).containsEntry("temp", 31);
    }

    @Test
    @BusinessCase("并发写同一影子：只有一个请求以当前版本成功，其余得到 STALE 结论，更新零丢失")
    void concurrentDesiredWritesExactlyOneWinsPerVersion() throws Exception {
        ShadowService service = new ShadowService();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            long baseVersion = i;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // 全部基于版本 0 写，竞争同一版本槽
                var result = service.updateDesired("concurrent-dev",
                        Map.of("k" + baseVersion, baseVersion), 0);
                if (result.accepted()) {
                    accepted.incrementAndGet();
                } else if (result.outcome() == UpdateOutcome.REJECTED_STALE_VERSION) {
                    rejected.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(accepted.get()).isEqualTo(1);
        assertThat(rejected.get()).isEqualTo(threads - 1);
        // 获胜写入保留
        assertThat(service.get("concurrent-dev").desiredVersion()).isEqualTo(1);
        assertThat(service.get("concurrent-dev").desired()).hasSize(1);
    }

    @Test
    @BusinessCase("串行重试全部成功：每个失败方读到新版本后重放，最终所有键都合并且无重复应用")
    void retryingWithFreshVersionLosesNoUpdate() throws Exception {
        DeviceShadow shadow = new DeviceShadow("d");

        Callable<Void> writerA = () -> {
            long v = shadow.desiredVersion();
            while (!shadow.updateDesired(Map.of("a", 1), v).accepted()) {
                v = shadow.desiredVersion();
            }
            return null;
        };
        Callable<Void> writerB = () -> {
            long v = shadow.desiredVersion();
            while (!shadow.updateDesired(Map.of("b", 2), v).accepted()) {
                v = shadow.desiredVersion();
            }
            return null;
        };
        writerA.call();
        writerB.call();

        assertThat(shadow.desiredVersion()).isEqualTo(2);
        assertThat(shadow.desired()).containsEntry("a", 1).containsEntry("b", 2);
        assertThat(shadow.delta()).containsKeys("a", "b");
    }

    @Test
    @BusinessCase("delta 差值：设备上报追平期望后差值清空")
    void deltaClearsOnceReportedCatchesUp() {
        DeviceShadow shadow = new DeviceShadow("d");
        shadow.updateDesired(Map.of("mode", "eco"), 0);
        assertThat(shadow.delta()).containsEntry("mode", "eco");
        shadow.updateReported(Map.of("mode", "eco"), 1);
        assertThat(shadow.delta()).isEmpty();
    }
}
