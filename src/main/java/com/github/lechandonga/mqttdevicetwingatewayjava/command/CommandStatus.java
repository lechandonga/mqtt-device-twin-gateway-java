package com.github.lechandonga.mqttdevicetwingatewayjava.command;

/**
 * 指令生命周期状态。
 * QUEUED（离线排队）-> DELIVERED（已投递给设备）-> ACKED（设备确认/完成）；
 * 失败终态：FAILED_TIMEOUT（超时未确认）、FAILED_UNSUPPORTED（设备不支持）。
 */
public enum CommandStatus {
    QUEUED,
    DELIVERED,
    ACKED,
    FAILED_TIMEOUT,
    FAILED_UNSUPPORTED
}
