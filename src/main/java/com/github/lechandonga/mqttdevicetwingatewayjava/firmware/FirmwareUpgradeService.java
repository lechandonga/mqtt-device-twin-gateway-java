package com.github.lechandonga.mqttdevicetwingatewayjava.firmware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 固件升级编排服务（内存实现）。
 * <ul>
 *     <li>jobId 幂等：重复创建返回既有任务，不产生第二条升级。</li>
 *     <li>进度单调：0..100 必须递增，乱序进度（含回退的百分比）忽略；重复百分比忽略。</li>
 *     <li>失败上报：无论在哪个阶段，立即进入 FAILED -> ROLLING_BACK -> ROLLED_BACK，
 *         当前可用版本确定为 rollbackVersion（上一个已知可用版本）。</li>
 *     <li>重试：ROLLED_BACK 的任务可用同一 jobId 重试，attempts+1，状态机从头开始；
 *         COMPLETED 为终态，后续上报一律 ALREADY_FINAL。</li>
 * </ul>
 */
@Component
public class FirmwareUpgradeService {

    private static final Logger log = LoggerFactory.getLogger(FirmwareUpgradeService.class);

    private final Map<String, UpgradeJob> jobs = new ConcurrentHashMap<>();
    private final Map<String, String> currentVersions = new ConcurrentHashMap<>();

    /**
     * 创建升级任务。
     *
     * @param rollbackVersion 升级失败时回滚到的确定可用版本
     */
    public synchronized UpgradeJob createJob(String jobId, String deviceId,
                                             String targetVersion, String rollbackVersion) {
        UpgradeJob existing = jobs.get(jobId);
        if (existing != null) {
            log.info("[OTA] jobId={} 已存在（target={}, state={}），幂等返回",
                    jobId, existing.targetVersion(), existing.state());
            return existing;
        }
        UpgradeJob job = new UpgradeJob(jobId, deviceId, targetVersion, rollbackVersion);
        jobs.put(jobId, job);
        currentVersions.putIfAbsent(deviceId, rollbackVersion);
        log.info("[OTA] 创建升级任务 {}: device='{}' {} -> {} (回滚版本 {})",
                jobId, deviceId, rollbackVersion, targetVersion, rollbackVersion);
        return job;
    }

    /** 上报阶段状态。重复状态忽略；非法倒退（非失败路径）忽略。 */
    public synchronized UpgradeReportResult reportState(String jobId, UpgradeState reported, int progressPercent) {
        UpgradeJob job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("unknown job: " + jobId);
        }
        if (job.state() == UpgradeState.COMPLETED) {
            log.warn("[OTA] job {} 已 COMPLETED，忽略上报 {}", jobId, reported);
            return UpgradeReportResult.ALREADY_FINAL;
        }
        if (reported == UpgradeState.FAILED) {
            return failAndRollback(job, "device reported failure");
        }
        if (reported == job.state()) {
            log.info("[OTA] job {} 重复上报状态 {}，忽略", jobId, reported);
            return UpgradeReportResult.IGNORED_DUPLICATE;
        }
        int expected = expectedNext(job.state());
        int reportedOrder = orderOf(reported);
        if (reportedOrder < expected || reportedOrder > expected) {
            log.warn("[OTA] job {} 乱序状态上报: 当前={} 收到={}，忽略", jobId, job.state(), reported);
            return UpgradeReportResult.IGNORED_OUT_OF_ORDER;
        }
        job.state(reported);
        if (progressPercent >= 0) {
            applyProgress(job, progressPercent);
        }
        if (reported == UpgradeState.COMPLETED) {
            job.progressPercent(100);
            currentVersions.put(job.deviceId(), job.targetVersion());
        }
        log.info("[OTA] job {} 状态推进到 {} 进度={}%", jobId, reported, job.progressPercent());
        return UpgradeReportResult.APPLIED;
    }

    /** 上报下载/安装进度百分比：必须单调递增，重复或回退均忽略。 */
    public synchronized UpgradeReportResult reportProgress(String jobId, int percent) {
        UpgradeJob job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("unknown job: " + jobId);
        }
        if (job.state() == UpgradeState.COMPLETED) {
            return UpgradeReportResult.ALREADY_FINAL;
        }
        if (job.state() == UpgradeState.ROLLED_BACK || job.state() == UpgradeState.ROLLING_BACK) {
            log.warn("[OTA] job {} 已回滚，进度 {}% 忽略", jobId, percent);
            return UpgradeReportResult.ALREADY_FINAL;
        }
        if (percent == job.progressPercent()) {
            return UpgradeReportResult.IGNORED_DUPLICATE;
        }
        if (percent < job.progressPercent()) {
            log.warn("[OTA] job {} 乱序进度: {}% < {}%，忽略", jobId, percent, job.progressPercent());
            return UpgradeReportResult.IGNORED_OUT_OF_ORDER;
        }
        applyProgress(job, percent);
        log.info("[OTA] job {} 进度更新为 {}%", jobId, percent);
        return UpgradeReportResult.APPLIED;
    }

    /** 对 ROLLED_BACK 的任务执行失败重试：同一 jobId 幂等重跑，尝试次数 +1。 */
    public synchronized boolean retry(String jobId) {
        UpgradeJob job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("unknown job: " + jobId);
        }
        if (job.state() != UpgradeState.ROLLED_BACK) {
            log.warn("[OTA] job {} 状态为 {}，不允许重试", jobId, job.state());
            return false;
        }
        job.incrementAttempts();
        job.state(UpgradeState.PENDING);
        job.progressPercent(-1);
        job.failureReason("");
        log.info("[OTA] job {} 第 {} 次重试升级到 {}", jobId, job.attempts() + 1, job.targetVersion());
        return true;
    }

    private UpgradeReportResult failAndRollback(UpgradeJob job, String reason) {
        job.state(UpgradeState.FAILED);
        job.failureReason(reason);
        log.warn("[OTA] job {} 升级失败: {}，开始回滚", job.jobId(), reason);
        job.state(UpgradeState.ROLLING_BACK);
        job.state(UpgradeState.ROLLED_BACK);
        currentVersions.put(job.deviceId(), job.rollbackVersion());
        log.warn("[OTA] job {} 已回滚到确定可用版本 {}", job.jobId(), job.rollbackVersion());
        return UpgradeReportResult.FAILED_TRIGGERED;
    }

    private void applyProgress(UpgradeJob job, int percent) {
        if (percent >= 0 && percent > job.progressPercent()) {
            job.progressPercent(percent);
        }
    }

    private int expectedNext(UpgradeState current) {
        return switch (current) {
            case PENDING -> orderOf(UpgradeState.DOWNLOADING);
            case DOWNLOADING -> orderOf(UpgradeState.DOWNLOADED);
            case DOWNLOADED -> orderOf(UpgradeState.INSTALLING);
            case INSTALLING -> orderOf(UpgradeState.COMPLETED);
            default -> -1;
        };
    }

    private int orderOf(UpgradeState s) {
        return switch (s) {
            case PENDING -> 0;
            case DOWNLOADING -> 1;
            case DOWNLOADED -> 2;
            case INSTALLING -> 3;
            case COMPLETED -> 4;
            case FAILED, ROLLING_BACK, ROLLED_BACK -> 10;
        };
    }

    public UpgradeJob job(String jobId) {
        return jobs.get(jobId);
    }

    public String currentVersion(String deviceId) {
        return currentVersions.get(deviceId);
    }
}
