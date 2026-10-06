package com.github.lechandonga.mqttdevicetwingatewayjava.ota;

/** 进度/结果上报结论。accepted=false 时给出可区分的拒绝原因。 */
public record OtaReportResult(boolean accepted,
                              OtaRejectReason reason,
                              OtaStatus status,
                              String activeVersion) {

    static OtaReportResult ok(OtaStatus status, String activeVersion) {
        return new OtaReportResult(true, OtaRejectReason.NONE, status, activeVersion);
    }

    static OtaReportResult reject(OtaRejectReason reason, OtaStatus status, String activeVersion) {
        return new OtaReportResult(false, reason, status, activeVersion);
    }
}
