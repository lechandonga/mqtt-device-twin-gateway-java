package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaRejectReason;
import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaService;
import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaStatus;
import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaTask;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessCase;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessLogging;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(BusinessLogging.class)
@BusinessModule("固件升级状态机 / 乱序与重复进度幂等 / 失败确定回滚")
class OtaStateMachineTests {

    private OtaService ota;

    @BeforeEach
    void setUp() {
        ota = new OtaService();
    }

    private OtaTask startHappyTask() {
        return ota.start("task-1", "dev", "2.0.0", "1.0.0");
    }

    @Test
    @BusinessCase("正常升级路径：下载 -> 安装 -> 成功，activeVersion 切到目标版本")
    void happyPathActivatesTargetVersion() {
        startHappyTask();
        assertThat(ota.reportProgress("task-1", 1, 30).status()).isEqualTo(OtaStatus.DOWNLOADING);
        assertThat(ota.reportProgress("task-1", 2, 100, true).status()).isEqualTo(OtaStatus.INSTALLING);
        assertThat(ota.reportSuccess("task-1", 3).accepted()).isTrue();

        assertThat(ota.task("task-1").orElseThrow().status()).isEqualTo(OtaStatus.SUCCEEDED);
        assertThat(ota.activeVersion("dev")).isEqualTo("2.0.0");
    }

    @Test
    @BusinessCase("重复进度序号幂等忽略，不改变状态/进度")
    void duplicateSequenceIsIdempotent() {
        startHappyTask();
        ota.reportProgress("task-1", 1, 30);
        var dup1 = ota.reportProgress("task-1", 1, 30);
        var dup2 = ota.reportProgress("task-1", 1, 99);

        assertThat(dup1.accepted()).isFalse();
        assertThat(dup1.reason()).isEqualTo(OtaRejectReason.DUPLICATE_SEQUENCE);
        assertThat(dup2.reason()).isEqualTo(OtaRejectReason.DUPLICATE_SEQUENCE);
        assertThat(ota.task("task-1").orElseThrow().lastPercent()).isEqualTo(30);
    }

    @Test
    @BusinessCase("乱序进度：sequence 回退或百分比倒退都被拒绝并给出原因")
    void outOfOrderProgressRejected() {
        startHappyTask();
        ota.reportProgress("task-1", 5, 50);

        var backSequence = ota.reportProgress("task-1", 4, 60);
        var backPercent = ota.reportProgress("task-1", 6, 40);

        assertThat(backSequence.accepted()).isFalse();
        assertThat(backSequence.reason()).isEqualTo(OtaRejectReason.OUT_OF_ORDER_SEQUENCE);
        assertThat(backPercent.reason()).isEqualTo(OtaRejectReason.OUT_OF_ORDER_SEQUENCE);
        assertThat(ota.task("task-1").orElseThrow().lastPercent()).isEqualTo(50);
    }

    @Test
    @BusinessCase("下载失败/安装失败都回滚到启动时确定的可用版本")
    void failureRollsBackToKnownGoodVersion() {
        startHappyTask();
        ota.reportProgress("task-1", 1, 30);
        var failed = ota.reportFailure("task-1", 2, "checksum mismatch");

        assertThat(failed.accepted()).isTrue();
        assertThat(failed.status()).isEqualTo(OtaStatus.ROLLED_BACK);
        assertThat(failed.activeVersion()).isEqualTo("1.0.0");
        assertThat(ota.activeVersion("dev")).isEqualTo("1.0.0");
        assertThat(ota.task("task-1").orElseThrow().status()).isEqualTo(OtaStatus.ROLLED_BACK);
    }

    @Test
    @BusinessCase("安装阶段失败同样回滚；重复失败上报保持同一确定结论")
    void installFailureAndDuplicateFailureReportsAreStable() {
        startHappyTask();
        ota.reportProgress("task-1", 1, 100, true);
        assertThat(ota.reportFailure("task-1", 2, "boot verify failed").status())
                .isEqualTo(OtaStatus.ROLLED_BACK);

        var duplicateFailure = ota.reportFailure("task-1", 9, "another failure");
        assertThat(duplicateFailure.accepted()).isFalse();
        assertThat(duplicateFailure.reason()).isEqualTo(OtaRejectReason.DUPLICATE_SEQUENCE);
        assertThat(duplicateFailure.status()).isEqualTo(OtaStatus.ROLLED_BACK);
        assertThat(duplicateFailure.activeVersion()).isEqualTo("1.0.0");

        // 回滚后不能再报成功
        assertThat(ota.reportSuccess("task-1", 10).reason())
                .isEqualTo(OtaRejectReason.ILLEGAL_TRANSITION);
    }

    @Test
    @BusinessCase("任务幂等：重复 start 复用任务；成功后的旧进度上报被拒绝")
    void duplicateStartAndPostSuccessReports() {
        OtaTask first = startHappyTask();
        OtaTask again = ota.start("task-1", "dev", "2.0.0", "1.0.0");
        assertThat(first).isSameAs(again);

        ota.reportProgress("task-1", 1, 50, false);
        ota.reportProgress("task-1", 2, 100, true);
        ota.reportSuccess("task-1", 3);
        var staleProgress = ota.reportProgress("task-1", 4, 50);
        assertThat(staleProgress.reason()).isEqualTo(OtaRejectReason.ILLEGAL_TRANSITION);
        assertThat(staleProgress.status()).isEqualTo(OtaStatus.SUCCEEDED);
    }

    @Test
    @BusinessCase("未知任务返回 UNKNOWN_TASK；同设备并行升级被拒绝；目标版本等于回滚版本非法")
    void guardConditions() {
        assertThat(ota.reportProgress("ghost", 1, 1).reason())
                .isEqualTo(OtaRejectReason.UNKNOWN_TASK);

        startHappyTask();
        assertThatThrownBy(() -> ota.start("task-2", "dev", "3.0.0", "1.0.0"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("进行中");

        assertThatThrownBy(() -> ota.start("task-3", "dev-other", "1.0.0", "1.0.0"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
