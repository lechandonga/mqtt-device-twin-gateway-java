package com.github.lechandonga.mqttdevicetwingatewayjava.ota;

/**
 * 固件升级任务。
 * @param knownGoodVersion 升级前确认可用的版本；任何失败都回滚到它，结论确定
 */
public class OtaTask {

    private final String taskId;
    private final String deviceId;
    private final String targetVersion;
    private final String knownGoodVersion;

    private OtaStatus status = OtaStatus.PENDING;
    private long lastSequence = -1;
    private int lastPercent = -1;
    private String failureReason;

    public OtaTask(String taskId, String deviceId, String targetVersion, String knownGoodVersion) {
        this.taskId = taskId;
        this.deviceId = deviceId;
        this.targetVersion = targetVersion;
        this.knownGoodVersion = knownGoodVersion;
    }

    public String taskId() {
        return taskId;
    }

    public String deviceId() {
        return deviceId;
    }

    public String targetVersion() {
        return targetVersion;
    }

    public String knownGoodVersion() {
        return knownGoodVersion;
    }

    public synchronized OtaStatus status() {
        return status;
    }

    public synchronized long lastSequence() {
        return lastSequence;
    }

    public synchronized int lastPercent() {
        return lastPercent;
    }

    public synchronized String failureReason() {
        return failureReason;
    }

    synchronized void advance(OtaStatus next, long sequence, int percent) {
        this.status = next;
        this.lastSequence = sequence;
        if (percent >= 0) {
            this.lastPercent = percent;
        }
    }

    synchronized void failAndRollback(String reason) {
        if (status == OtaStatus.SUCCEEDED || status == OtaStatus.ROLLED_BACK) {
            return;
        }
        this.failureReason = reason;
        this.status = OtaStatus.ROLLING_BACK;
        this.status = OtaStatus.ROLLED_BACK;
    }

    synchronized void succeed(long sequence, int percent) {
        this.status = OtaStatus.SUCCEEDED;
        this.lastSequence = sequence;
        this.lastPercent = 100;
        this.failureReason = null;
    }

    public synchronized boolean terminal() {
        return status == OtaStatus.SUCCEEDED || status == OtaStatus.ROLLED_BACK;
    }
}
