package com.github.lechandonga.mqttdevicetwingatewayjava.twin;

import java.util.Map;

/**
 * 影子写入结论。accepted 与 rejected 互斥；拒绝原因可区分：
 * VERSION_CONFLICT（过期版本）、STALE_DUPLICATE（同版本重复写，幂等跳过）、VALUE_MISSING。
 */
public record UpdateResult(boolean accepted, String reason, long baseVersion, long newVersion,
                           Map<String, Object> appliedFields) {

    public static UpdateResult ok(long baseVersion, long newVersion, Map<String, Object> applied) {
        return new UpdateResult(true, "OK", baseVersion, newVersion, Map.copyOf(applied));
    }

    public static UpdateResult reject(String reason, long baseVersion, long newVersion) {
        return new UpdateResult(false, reason, baseVersion, newVersion, Map.of());
    }
}
