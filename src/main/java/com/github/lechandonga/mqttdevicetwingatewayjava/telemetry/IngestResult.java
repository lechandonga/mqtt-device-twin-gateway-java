package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

/**
 * 遥测入库结论：ACCEPTED 已更新最新状态；DUPLICATE 重复样本忽略；STALE_TIMESTAMP 时间戳回退忽略。
 */
public enum IngestResult {
    ACCEPTED,
    DUPLICATE,
    STALE_TIMESTAMP
}
