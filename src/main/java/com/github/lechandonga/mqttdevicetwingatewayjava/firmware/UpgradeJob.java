package com.github.lechandonga.mqttdevicetwingatewayjava.firmware;

/**
 * 一次固件升级任务。rollbackVersion 是失败回滚的确定可用版本。
 */
public final class UpgradeJob {
    private final String jobId;
    private final String deviceId;
    private final String targetVersion;
    private final String rollbackVersion;
    private volatile UpgradeState state = UpgradeState.PENDING;
    private volatile int progressPercent = -1;
    private volatile int attempts = 0;
    private volatile String failureReason = "";

    public UpgradeJob(String jobId, String deviceId, String targetVersion, String rollbackVersion) {
        this.jobId = jobId;
        this.deviceId = deviceId;
        this.targetVersion = targetVersion;
        this.rollbackVersion = rollbackVersion;
    }

    public String jobId() {
        return jobId;
    }

    public String deviceId() {
        return deviceId;
    }

    public String targetVersion() {
        return targetVersion;
    }

    public String rollbackVersion() {
        return rollbackVersion;
    }

    public UpgradeState state() {
        return state;
    }

    public void state(UpgradeState state) {
        this.state = state;
    }

    public int progressPercent() {
        return progressPercent;
    }

    public void progressPercent(int progressPercent) {
        this.progressPercent = progressPercent;
    }

    public int attempts() {
        return attempts;
    }

    public void incrementAttempts() {
        attempts++;
    }

    public String failureReason() {
        return failureReason;
    }

    public void failureReason(String failureReason) {
        this.failureReason = failureReason;
    }
}
