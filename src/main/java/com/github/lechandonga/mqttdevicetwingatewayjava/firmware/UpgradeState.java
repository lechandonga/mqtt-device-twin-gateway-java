package com.github.lechandonga.mqttdevicetwingatewayjava.firmware;

/**
 * 固件升级状态机：
 * <pre>
 * PENDING -> DOWNLOADING -> DOWNLOADED -> INSTALLING -> COMPLETED
 *   任意阶段 FAILED -> ROLLING_BACK -> ROLLED_BACK（可用版本确定）
 * ROLLED_BACK 后可用相同 jobId 重试（幂等），也可创建新 jobId。
 * </pre>
 */
public enum UpgradeState {
    PENDING,
    DOWNLOADING,
    DOWNLOADED,
    INSTALLING,
    COMPLETED,
    FAILED,
    ROLLING_BACK,
    ROLLED_BACK
}
