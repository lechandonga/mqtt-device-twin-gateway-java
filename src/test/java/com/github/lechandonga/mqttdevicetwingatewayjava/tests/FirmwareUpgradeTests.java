package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.firmware.FirmwareUpgradeService;
import com.github.lechandonga.mqttdevicetwingatewayjava.firmware.UpgradeJob;
import com.github.lechandonga.mqttdevicetwingatewayjava.firmware.UpgradeReportResult;
import com.github.lechandonga.mqttdevicetwingatewayjava.firmware.UpgradeState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：正常升级状态机、重复创建/重复上报幂等、进度乱序忽略、任意阶段失败回滚到确定可用版本、失败后重试。
 */
@DisplayName("固件 OTA 升级状态机")
class FirmwareUpgradeTests {

    @RegisterExtension
    static final CoverageLoggerExtension COVERAGE = new CoverageLoggerExtension();

    private FirmwareUpgradeService ota;

    @BeforeEach
    void setUp() {
        ota = new FirmwareUpgradeService();
    }

    @Test
    @DisplayName("正常路径：下载->下载完成->安装->完成，当前版本切换为目标版本")
    void happyPath() {
        UpgradeJob job = ota.createJob("j1", "dev", "2.0.0", "1.0.0");
        assertEquals(UpgradeReportResult.APPLIED, ota.reportState("j1", UpgradeState.DOWNLOADING, 10));
        assertEquals(UpgradeReportResult.APPLIED, ota.reportState("j1", UpgradeState.DOWNLOADED, 100));
        assertEquals(UpgradeReportResult.APPLIED, ota.reportState("j1", UpgradeState.INSTALLING, 100));
        assertEquals(UpgradeReportResult.APPLIED, ota.reportState("j1", UpgradeState.COMPLETED, 100));

        assertEquals(UpgradeState.COMPLETED, job.state());
        assertEquals("2.0.0", ota.currentVersion("dev"));
    }

    @Test
    @DisplayName("幂等：重复 jobId 返回同一任务；重复状态上报与重复进度被忽略；完成后上报 ALREADY_FINAL")
    void idempotentCreationAndReports() {
        UpgradeJob first = ota.createJob("j2", "dev", "2.0.0", "1.0.0");
        assertSame(first, ota.createJob("j2", "dev", "2.0.0", "1.0.0"));

        ota.reportState("j2", UpgradeState.DOWNLOADING, 10);
        assertEquals(UpgradeReportResult.IGNORED_DUPLICATE,
                ota.reportState("j2", UpgradeState.DOWNLOADING, 10));
        assertEquals(UpgradeReportResult.IGNORED_DUPLICATE, ota.reportProgress("j2", 10));

        ota.reportState("j2", UpgradeState.DOWNLOADED, 100);
        ota.reportState("j2", UpgradeState.INSTALLING, 100);
        ota.reportState("j2", UpgradeState.COMPLETED, 100);
        assertEquals(UpgradeReportResult.ALREADY_FINAL, ota.reportState("j2", UpgradeState.INSTALLING, 50));
    }

    @Test
    @DisplayName("乱序：进度回退忽略；阶段跳跃（如 PENDING 直接 INSTALLING）忽略，状态不被污染")
    void outOfOrderIgnored() {
        ota.createJob("j3", "dev", "2.0.0", "1.0.0");
        ota.reportState("j3", UpgradeState.DOWNLOADING, 50);

        assertEquals(UpgradeReportResult.IGNORED_OUT_OF_ORDER, ota.reportProgress("j3", 30));
        assertEquals(50, ota.job("j3").progressPercent());

        assertEquals(UpgradeReportResult.IGNORED_OUT_OF_ORDER,
                ota.reportState("j3", UpgradeState.INSTALLING, 50));
        assertEquals(UpgradeState.DOWNLOADING, ota.job("j3").state());
    }

    @Test
    @DisplayName("失败回滚：安装阶段上报失败，状态到 ROLLED_BACK，当前版本确定回滚到 1.0.0")
    void failureRollsBackToKnownGood() {
        ota.createJob("j4", "dev", "2.0.0", "1.0.0");
        ota.reportState("j4", UpgradeState.DOWNLOADING, 50);
        ota.reportState("j4", UpgradeState.DOWNLOADED, 100);
        ota.reportState("j4", UpgradeState.INSTALLING, 100);

        UpgradeReportResult result = ota.reportState("j4", UpgradeState.FAILED, -1);

        assertEquals(UpgradeReportResult.FAILED_TRIGGERED, result);
        assertEquals(UpgradeState.ROLLED_BACK, ota.job("j4").state());
        assertEquals("1.0.0", ota.currentVersion("dev"), "失败后必须是确定的可用版本");
        assertEquals(UpgradeReportResult.ALREADY_FINAL, ota.reportProgress("j4", 90),
                "回滚后的迟到进度必须被忽略");
    }

    @Test
    @DisplayName("失败重试：同一 jobId 重试从头开始且幂等，成功后版本变为 2.0.0")
    void retryAfterRollback() {
        ota.createJob("j5", "dev", "2.0.0", "1.0.0");
        ota.reportState("j5", UpgradeState.DOWNLOADING, 20);
        ota.reportState("j5", UpgradeState.FAILED, -1);
        assertEquals(UpgradeState.ROLLED_BACK, ota.job("j5").state());

        assertTrue(ota.retry("j5"));
        assertEquals(1, ota.job("j5").attempts());
        assertEquals(UpgradeState.PENDING, ota.job("j5").state());

        ota.reportState("j5", UpgradeState.DOWNLOADING, 10);
        ota.reportState("j5", UpgradeState.DOWNLOADED, 100);
        ota.reportState("j5", UpgradeState.INSTALLING, 100);
        ota.reportState("j5", UpgradeState.COMPLETED, 100);
        assertEquals("2.0.0", ota.currentVersion("dev"));
    }

    @Test
    @DisplayName("完成态任务不允许重试")
    void retryRejectedWhenCompleted() {
        ota.createJob("j6", "dev", "2.0.0", "1.0.0");
        ota.reportState("j6", UpgradeState.DOWNLOADING, 10);
        ota.reportState("j6", UpgradeState.DOWNLOADED, 100);
        ota.reportState("j6", UpgradeState.INSTALLING, 100);
        ota.reportState("j6", UpgradeState.COMPLETED, 100);
        assertEquals(false, ota.retry("j6"));
    }
}
