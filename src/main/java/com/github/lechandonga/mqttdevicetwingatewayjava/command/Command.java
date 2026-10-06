package com.github.lechandonga.mqttdevicetwingatewayjava.command;

import java.util.Map;

/**
 * 下发给设备的指令。{@code commandId} 为业务幂等键：
 * 同设备同 commandId 重复下发直接复用原指令，不会重复送达或重复消费。
 */
public record Command(String commandId,
                      String deviceId,
                      String name,
                      Map<String, Object> payload,
                      long createdAtMillis) {

    public Command {
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId 不能为空");
        }
        if (payload == null) {
            payload = Map.of();
        }
    }
}
