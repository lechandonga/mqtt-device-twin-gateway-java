package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

public record AlertEvent(String deviceId,
                         String metric,
                         String type,
                         double value,
                         double threshold,
                         long timestampMillis) {

    public boolean firing() {
        return "FIRED".equals(type);
    }

    public boolean recovered() {
        return "RECOVERED".equals(type);
    }
}
