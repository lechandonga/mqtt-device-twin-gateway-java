package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

import java.util.Map;

/**
 * 归一化后的遥测样本。
 *
 * @param id        设备内唯一样本标识（用于重复上报去重）
 * @param timestamp 设备时间戳（epoch ms），必须单调不减
 * @param metrics   归一化后的指标（统一 SI 单位）
 */
public record TelemetrySample(String id, long timestamp, Map<String, Double> metrics) {
}
