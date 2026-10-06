package com.github.lechandonga.mqttdevicetwingatewayjava.ota;

public enum OtaRejectReason {
    NONE,
    DUPLICATE_SEQUENCE,
    OUT_OF_ORDER_SEQUENCE,
    ILLEGAL_TRANSITION,
    UNKNOWN_TASK
}
