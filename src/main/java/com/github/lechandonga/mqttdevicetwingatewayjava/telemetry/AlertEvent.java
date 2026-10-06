package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

/**
 * 阈值边沿事件：TRIGGERED 仅在 正常->越限 边沿产生；RECOVERED 在越限->正常 时产生。
 */
public record AlertEvent(String deviceId, String rule, String metric, double value,
                         double threshold, boolean triggered, long timestamp) {
}
