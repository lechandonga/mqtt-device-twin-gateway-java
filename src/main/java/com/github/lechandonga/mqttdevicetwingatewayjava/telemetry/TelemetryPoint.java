package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

import java.util.Map;

/**
 * 一条设备遥测。
 * @param messageId 业务消息 ID（重复投递的同一条遥测 ID 相同），幂等去重键
 * @param timestampMillis 采样时间戳，回退的旧数据不得覆盖最新状态
 */
public record TelemetryPoint(String deviceId,
                             String messageId,
                             long timestampMillis,
                             Map<String, Double> metrics) {

    public TelemetryPoint {
        if (deviceId == null || deviceId.isBlank()) {
            throw new IllegalArgumentException("deviceId 不能为空");
        }
        if (messageId == null || messageId.isBlank()) {
            throw new IllegalArgumentException("messageId 不能为空");
        }
        metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
    }
}
