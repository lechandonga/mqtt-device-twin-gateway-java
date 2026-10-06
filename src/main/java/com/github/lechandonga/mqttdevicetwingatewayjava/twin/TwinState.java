package com.github.lechandonga.mqttdevicetwingatewayjava.twin;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 设备影子：desired（期望态）与 reported（上报态）分别维护字段级版本。
 */
public final class TwinState {
    /** 字段 -> 期望值 */
    private final Map<String, Object> desired = new LinkedHashMap<>();
    /** 字段 -> 上报值 */
    private final Map<String, Object> reported = new LinkedHashMap<>();
    /** 字段 -> 期望态版本 */
    private final Map<String, Long> desiredVersion = new LinkedHashMap<>();
    /** 字段 -> 上报态版本 */
    private final Map<String, Long> reportedVersion = new LinkedHashMap<>();

    public Map<String, Object> desired() {
        return desired;
    }

    public Map<String, Object> reported() {
        return reported;
    }

    public Map<String, Long> desiredVersion() {
        return desiredVersion;
    }

    public Map<String, Long> reportedVersion() {
        return reportedVersion;
    }

    public long desiredVersionOf(String field) {
        return desiredVersion.getOrDefault(field, 0L);
    }

    public long reportedVersionOf(String field) {
        return reportedVersion.getOrDefault(field, 0L);
    }
}
