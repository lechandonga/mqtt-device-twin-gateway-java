package com.github.lechandonga.mqttdevicetwingatewayjava.ota;

public enum OtaStatus {
    PENDING,
    DOWNLOADING,
    INSTALLING,
    SUCCEEDED,
    FAILED,
    ROLLING_BACK,
    ROLLED_BACK
}
