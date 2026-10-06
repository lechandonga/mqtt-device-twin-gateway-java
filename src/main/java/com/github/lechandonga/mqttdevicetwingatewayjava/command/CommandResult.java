package com.github.lechandonga.mqttdevicetwingatewayjava.command;

/**
 * sendCommand 的即时结论。duplicate=true 表示 commandId 重复，已被幂等拒绝。
 */
public record CommandResult(boolean accepted, boolean duplicate, CommandStatus status, String commandId) {
}
