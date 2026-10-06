package com.github.lechandonga.mqttdevicetwingatewayjava.ota;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 固件升级状态机（每个设备同时只有一个活动任务）：
 * <pre>
 * PENDING -> DOWNLOADING -> INSTALLING -> SUCCEEDED
 *                    |             |
 *                    +------ 任意失败 -----> ROLLING_BACK -> ROLLED_BACK(active=knownGood)
 * </pre>
 * 进度上报带单调递增 sequence：重复 sequence 幂等忽略，回退 sequence 判乱序拒绝；
 * 失败（含重复失败上报）一律回滚到启动任务时确定的 knownGoodVersion。
 */
@Service
public class OtaService {

    private static final Logger log = LoggerFactory.getLogger(OtaService.class);

    private final Map<String, OtaTask> tasks = new ConcurrentHashMap<>();
    private final Map<String, String> activeVersions = new ConcurrentHashMap<>();
    private final Map<String, String> activeTaskByDevice = new ConcurrentHashMap<>();

    /**
     * 创建升级任务；重复 taskId 幂等返回既有任务。
     * @param knownGoodVersion 当前确认可用版本，失败回滚目标
     */
    public synchronized OtaTask start(String taskId, String deviceId,
                                      String targetVersion, String knownGoodVersion) {
        OtaTask existing = tasks.get(taskId);
        if (existing != null) {
            log.info("[ota] 重复创建任务 {} 幂等返回，状态={}", taskId, existing.status());
            return existing;
        }
        if (targetVersion.equals(knownGoodVersion)) {
            throw new IllegalArgumentException("目标版本与可用版本相同，无需升级: " + targetVersion);
        }
        String activeTaskId = activeTaskByDevice.get(deviceId);
        if (activeTaskId != null && !tasks.get(activeTaskId).terminal()) {
            throw new IllegalStateException("设备 " + deviceId + " 已有进行中的升级任务: " + activeTaskId);
        }
        OtaTask task = new OtaTask(taskId, deviceId, targetVersion, knownGoodVersion);
        tasks.put(taskId, task);
        activeTaskByDevice.put(deviceId, taskId);
        activeVersions.put(deviceId, knownGoodVersion);
        log.info("[ota] 创建任务 {} device={} {} -> {} (回滚版本={})",
                taskId, deviceId, knownGoodVersion, targetVersion, knownGoodVersion);
        return task;
    }

    /**
     * 上报进度（默认仍处于下载阶段）。
     * @param sequence 单调递增序号（同序号=重复，变小=乱序）
     * @param percent 0-100
     */
    public OtaReportResult reportProgress(String taskId, long sequence, int percent) {
        return reportProgress(taskId, sequence, percent, false);
    }

    /**
     * 上报进度。
     * @param installing true 表示设备已开始安装（进入 INSTALLING）；false 为下载阶段
     */
    public OtaReportResult reportProgress(String taskId, long sequence, int percent,
                                          boolean installing) {
        OtaTask task = tasks.get(taskId);
        if (task == null) {
            return OtaReportResult.reject(OtaRejectReason.UNKNOWN_TASK, OtaStatus.PENDING, null);
        }
        synchronized (task) {
            if (task.terminal()) {
                return OtaReportResult.reject(OtaRejectReason.ILLEGAL_TRANSITION,
                        task.status(), activeVersion(task));
            }
            if (sequence == task.lastSequence() && task.lastSequence() >= 0) {
                log.info("[ota] 任务 {} 重复进度 seq={} 幂等忽略", taskId, sequence);
                return OtaReportResult.reject(OtaRejectReason.DUPLICATE_SEQUENCE,
                        task.status(), activeVersion(task));
            }
            if (sequence < task.lastSequence()) {
                log.info("[ota] 任务 {} 乱序进度 seq={} < {} 拒绝",
                        taskId, sequence, task.lastSequence());
                return OtaReportResult.reject(OtaRejectReason.OUT_OF_ORDER_SEQUENCE,
                        task.status(), activeVersion(task));
            }
            if (percent < task.lastPercent()) {
                log.info("[ota] 任务 {} 进度回退 {}% < {}% 拒绝",
                        taskId, percent, task.lastPercent());
                return OtaReportResult.reject(OtaRejectReason.OUT_OF_ORDER_SEQUENCE,
                        task.status(), activeVersion(task));
            }
            OtaStatus next = installing ? OtaStatus.INSTALLING : OtaStatus.DOWNLOADING;
            if (!legal(task.status(), next)) {
                return OtaReportResult.reject(OtaRejectReason.ILLEGAL_TRANSITION,
                        task.status(), activeVersion(task));
            }
            task.advance(next, sequence, percent);
            log.info("[ota] 任务 {} seq={} 进度 {}% 阶段={}", taskId, sequence, percent, next);
            return OtaReportResult.ok(next, activeVersion(task));
        }
    }

    /** 上报升级成功（sequence 仍需大于上一次，重复成功幂等）。 */
    public OtaReportResult reportSuccess(String taskId, long sequence) {
        OtaTask task = tasks.get(taskId);
        if (task == null) {
            return OtaReportResult.reject(OtaRejectReason.UNKNOWN_TASK, OtaStatus.PENDING, null);
        }
        synchronized (task) {
            if (task.status() == OtaStatus.SUCCEEDED) {
                return OtaReportResult.reject(OtaRejectReason.DUPLICATE_SEQUENCE,
                        task.status(), activeVersion(task));
            }
            if (task.status() != OtaStatus.INSTALLING) {
                return OtaReportResult.reject(OtaRejectReason.ILLEGAL_TRANSITION,
                        task.status(), activeVersion(task));
            }
            if (sequence <= task.lastSequence()) {
                OtaRejectReason reason = sequence == task.lastSequence()
                        ? OtaRejectReason.DUPLICATE_SEQUENCE
                        : OtaRejectReason.OUT_OF_ORDER_SEQUENCE;
                return OtaReportResult.reject(reason, task.status(), activeVersion(task));
            }
            task.succeed(sequence, 100);
            activeVersions.put(task.deviceId(), task.targetVersion());
            log.info("[ota] 任务 {} 成功，activeVersion={}", taskId, task.targetVersion());
            return OtaReportResult.ok(OtaStatus.SUCCEEDED, task.targetVersion());
        }
    }

    /** 上报失败：回滚到 knownGoodVersion；重复/乱序失败保持同一确定结论。 */
    public OtaReportResult reportFailure(String taskId, long sequence, String reason) {
        OtaTask task = tasks.get(taskId);
        if (task == null) {
            return OtaReportResult.reject(OtaRejectReason.UNKNOWN_TASK, OtaStatus.PENDING, null);
        }
        synchronized (task) {
            if (task.status() == OtaStatus.ROLLED_BACK) {
                log.info("[ota] 任务 {} 重复失败上报，保持 ROLLED_BACK/{}",
                        taskId, task.knownGoodVersion());
                return OtaReportResult.reject(OtaRejectReason.DUPLICATE_SEQUENCE,
                        OtaStatus.ROLLED_BACK, task.knownGoodVersion());
            }
            if (task.status() == OtaStatus.SUCCEEDED) {
                return OtaReportResult.reject(OtaRejectReason.ILLEGAL_TRANSITION,
                        OtaStatus.SUCCEEDED, task.targetVersion());
            }
            log.warn("[ota] 任务 {} 失败 seq={} 原因={}，回滚 -> {}",
                    taskId, sequence, reason, task.knownGoodVersion());
            task.failAndRollback(reason);
            activeVersions.put(task.deviceId(), task.knownGoodVersion());
            return OtaReportResult.ok(OtaStatus.ROLLED_BACK, task.knownGoodVersion());
        }
    }

    public Optional<OtaTask> task(String taskId) {
        return Optional.ofNullable(tasks.get(taskId));
    }

    public String activeVersion(String deviceId) {
        return activeVersions.get(deviceId);
    }

    private String activeVersion(OtaTask task) {
        return activeVersions.get(task.deviceId());
    }

    private static boolean legal(OtaStatus from, OtaStatus to) {
        return switch (to) {
            case DOWNLOADING -> from == OtaStatus.PENDING || from == OtaStatus.DOWNLOADING;
            case INSTALLING -> from == OtaStatus.DOWNLOADING || from == OtaStatus.INSTALLING;
            default -> false;
        };
    }
}
