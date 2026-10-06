package com.github.lechandonga.mqttdevicetwingatewayjava.twin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备影子服务：期望态/上报态按字段版本合并。
 * <p>并发安全：每台设备一把锁，写入采用乐观版本检查——expectedVersion 必须等于当前版本，
 * 过期版本返回 VERSION_CONFLICT（不覆盖更新，也不推进版本）；相同版本相同内容视为幂等重放 STALE_DUPLICATE。
 */
@Component
public class DeviceTwinService {

    private static final Logger log = LoggerFactory.getLogger(DeviceTwinService.class);

    private final Map<String, TwinState> twins = new ConcurrentHashMap<>();
    private final Map<String, Object> deviceLocks = new ConcurrentHashMap<>();

    private TwinState twin(String deviceId) {
        return twins.computeIfAbsent(deviceId, k -> new TwinState());
    }

    private Object lock(String deviceId) {
        return deviceLocks.computeIfAbsent(deviceId, k -> new Object());
    }

    /**
     * 更新期望态。
     *
     * @param expectedVersion 调用方持有的期望态基准版本（0 表示初始）
     */
    public UpdateResult updateDesired(String deviceId, Map<String, Object> patch, long expectedVersion) {
        validatePatch(patch);
        synchronized (lock(deviceId)) {
            TwinState state = twin(deviceId);
            long current = state.desiredVersion().values().stream().mapToLong(Long::longValue).max().orElse(0L);
            if (expectedVersion < current) {
                log.warn("[TWIN] device='{}' 期望态版本冲突: expected={} < current={}",
                        deviceId, expectedVersion, current);
                return UpdateResult.reject("VERSION_CONFLICT", current, current + 1);
            }
            Map<String, Object> applied = applyPatch(state.desired(), state.desiredVersion(), patch,
                    expectedVersion, current, deviceId, "desired");
            long newVersion = state.desiredVersion().values().stream().mapToLong(Long::longValue).max().orElse(0L);
            return UpdateResult.ok(current, newVersion, applied);
        }
    }

    /**
     * 设备上报状态。相同版本（旧版本号）的上报为重复上报：内容相同则幂等 STALE_DUPLICATE，
     * 内容不同也一律拒绝，绝不回退污染最新结果。
     */
    public UpdateResult reportState(String deviceId, Map<String, Object> patch, long reportedVersion) {
        validatePatch(patch);
        synchronized (lock(deviceId)) {
            TwinState state = twin(deviceId);
            long current = state.reportedVersion().values().stream().mapToLong(Long::longValue).max().orElse(0L);
            if (reportedVersion < current) {
                String reason = sameContent(state.reported(), patch) ? "STALE_DUPLICATE" : "VERSION_CONFLICT";
                log.warn("[TWIN] device='{}' 过期上报 v{}<v{} reason={}", deviceId, reportedVersion, current, reason);
                return UpdateResult.reject(reason, current, current + 1);
            }
            if (reportedVersion == current && current > 0) {
                log.info("[TWIN] device='{}' 同版本重复上报 v{}，幂等处理", deviceId, current);
                return UpdateResult.reject("STALE_DUPLICATE", current, current + 1);
            }
            Map<String, Object> applied = applyPatch(state.reported(), state.reportedVersion(), patch,
                    reportedVersion, current, deviceId, "reported");
            return UpdateResult.ok(current, reportedVersion, applied);
        }
    }

    /**
     * 期望态与上报态按字段合并：值一致的字段进入 matched，其余报告 delta。
     */
    public Map<String, Object> getMerged(String deviceId) {
        synchronized (lock(deviceId)) {
            TwinState state = twin(deviceId);
            Map<String, Object> merged = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : state.desired().entrySet()) {
                String field = e.getKey();
                Object reportedValue = state.reported().get(field);
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("desired", e.getValue());
                entry.put("reported", reportedValue);
                entry.put("inSync", Objects.equals(e.getValue(), reportedValue));
                merged.put(field, entry);
            }
            return merged;
        }
    }

    public TwinState snapshot(String deviceId) {
        return twin(deviceId);
    }

    private Map<String, Object> applyPatch(Map<String, Object> values, Map<String, Long> versions,
                                           Map<String, Object> patch, long expectedVersion, long current,
                                           String deviceId, String section) {
        long next = Math.max(current + 1, expectedVersion + 1);
        Map<String, Object> applied = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : patch.entrySet()) {
            values.put(e.getKey(), e.getValue());
            versions.put(e.getKey(), next);
            applied.put(e.getKey(), e.getValue());
        }
        log.info("[TWIN] device='{}' {} 更新 {} 字段，版本 {} -> {}", deviceId, section, applied.size(),
                current, next);
        return applied;
    }

    private boolean sameContent(Map<String, Object> current, Map<String, Object> patch) {
        for (Map.Entry<String, Object> e : patch.entrySet()) {
            if (!Objects.equals(current.get(e.getKey()), e.getValue())) {
                return false;
            }
        }
        return true;
    }

    private void validatePatch(Map<String, Object> patch) {
        if (patch == null || patch.isEmpty()) {
            throw new IllegalArgumentException("patch must not be empty");
        }
    }
}
