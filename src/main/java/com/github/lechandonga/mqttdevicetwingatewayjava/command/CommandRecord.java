package com.github.lechandonga.mqttdevicetwingatewayjava.command;

import java.time.Instant;

/**
 * 一条下发指令的完整记录。
 */
public final class CommandRecord {
    private final String commandId;
    private final String deviceId;
    private final String name;
    private final String payload;
    private final Instant createdAt = Instant.now();
    private volatile CommandStatus status;
    private volatile int deliveryAttempts;
    private volatile String detail = "";

    public CommandRecord(String commandId, String deviceId, String name, String payload, CommandStatus status) {
        this.commandId = commandId;
        this.deviceId = deviceId;
        this.name = name;
        this.payload = payload;
        this.status = status;
    }

    public String commandId() {
        return commandId;
    }

    public String deviceId() {
        return deviceId;
    }

    public String name() {
        return name;
    }

    public String payload() {
        return payload;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public CommandStatus status() {
        return status;
    }

 public void status(CommandStatus status) {
        this.status = status;
    }

    public int deliveryAttempts() {
        return deliveryAttempts;
    }

    public void incrementAttempts() {
        this.deliveryAttempts++;
    }

    public String detail() {
        return detail;
    }

    public void detail(String detail) {
        this.detail = detail;
    }
}
