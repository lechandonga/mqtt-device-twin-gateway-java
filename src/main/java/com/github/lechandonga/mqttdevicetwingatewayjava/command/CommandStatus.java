package com.github.lechandonga.mqttdevicetwingatewayjava.command;

public enum CommandStatus {
    QUEUED,
    SENT,
    ACKED,
    SUCCEEDED,
    FAILED_TIMEOUT,
    FAILED_UNSUPPORTED,
    FAILED_DEVICE_REJECTED
}
