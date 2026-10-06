package com.github.lechandonga.mqttdevicetwingatewayjava.shadow;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 设备影子：期望态（desired，服务端写）与上报态（reported，设备写）分别维护单调版本号。
 * 并发写入用“期望版本必须等于当前版本”的乐观并发控制；版本过期或重复都会被拒绝，
 * 因此既不会丢更新，也不会把同一份状态重复应用。
 */
public class DeviceShadow {

    private final String deviceId;
    private final Map<String, Object> desired = new LinkedHashMap<>();
    private final Map<String, Object> reported = new LinkedHashMap<>();
    private long desiredVersion = 0;
    private long reportedVersion = 0;

    public DeviceShadow(String deviceId) {
        this.deviceId = deviceId;
    }

    public String deviceId() {
        return deviceId;
    }

    /**
     * 合并期望态（浅合并：只覆盖 patch 中出现的键）。
     * @param expectedVersion 客户端读到的 desiredVersion；必须严格相等
     */
    public synchronized UpdateResult updateDesired(Map<String, Object> patch, long expectedVersion) {
        long current = desiredVersion;
        if (expectedVersion < current) {
            return UpdateResult.stale(UpdateOutcome.REJECTED_STALE_VERSION, current);
        }
        if (expectedVersion > current) {
            throw new IllegalStateException("期望版本 " + expectedVersion + " 超前于当前版本 " + current);
        }
        desired.putAll(patch);
        desiredVersion++;
        return UpdateResult.accepted(desiredVersion);
    }

    /**
     * 合并设备上报态。重复版本（重试/重复投递）幂等返回，不改变状态；
     * 过期版本（乱序到达的旧消息）被拒绝。
     */
    public synchronized UpdateResult updateReported(Map<String, Object> patch, long version) {
        if (version < reportedVersion) {
            return UpdateResult.stale(UpdateOutcome.REJECTED_STALE_VERSION, reportedVersion);
        }
        if (version == reportedVersion && reportedVersion > 0) {
            return UpdateResult.stale(UpdateOutcome.REJECTED_DUPLICATE_VERSION, reportedVersion);
        }
        if (version > reportedVersion + 1) {
            throw new IllegalStateException("上报版本 " + version + " 存在版本缺口（当前 " + reportedVersion + "）");
        }
        reported.putAll(patch);
        reportedVersion = version == 0 ? 1 : version;
        return UpdateResult.accepted(reportedVersion);
    }

    /** 服务端生成下一次上报版本号（设备每次上报 +1，由网关统一发号保证严格递增）。 */
    public synchronized long nextReportedVersion() {
        return reportedVersion + 1;
    }

    public synchronized long desiredVersion() {
        return desiredVersion;
    }

    public synchronized long reportedVersion() {
        return reportedVersion;
    }

    public synchronized Map<String, Object> desired() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(desired));
    }

    public synchronized Map<String, Object> reported() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(reported));
    }

    /** 期望态中尚未被上报态追平的键（差值），用于判断指令是否被设备执行。 */
    public synchronized Map<String, Object> delta() {
        Map<String, Object> diff = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : desired.entrySet()) {
            if (!java.util.Objects.equals(e.getValue(), reported.get(e.getKey()))) {
                diff.put(e.getKey(), e.getValue());
            }
        }
        return Collections.unmodifiableMap(diff);
    }

    public record UpdateResult(UpdateOutcome outcome, long currentVersion) {
        static UpdateResult accepted(long version) {
            return new UpdateResult(UpdateOutcome.ACCEPTED, version);
        }

        static UpdateResult stale(UpdateOutcome outcome, long currentVersion) {
            return new UpdateResult(outcome, currentVersion);
        }

        public boolean accepted() {
            return outcome == UpdateOutcome.ACCEPTED;
        }
    }
}
