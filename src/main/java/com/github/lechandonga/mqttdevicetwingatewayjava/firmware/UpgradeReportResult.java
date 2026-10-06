package com.github.lechandonga.mqttdevicetwingatewayjava.firmware;

/**
 * 设备上报处理结论：
 * APPLIED 已推进状态；IGNORED_DUPLICATE 重复上报；IGNORED_OUT_OF_ORDER 乱序进度；
 * ALREADY_FINAL 任务已终态；FAILED_TRIGGERED 本次上报为失败，已触发回滚。
 */
public enum UpgradeReportResult {
    APPLIED,
    IGNORED_DUPLICATE,
    IGNORED_OUT_OF_ORDER,
    ALREADY_FINAL,
    FAILED_TRIGGERED
}
