package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * 一条活动连接（非持久，断线即失效；持久状态在 {@link Session} 中）。
 */
public class Connection {

    private final String clientId;
    private final boolean cleanStart;
    private final MqttClient client;
    private final MqttMessage will;

    public Connection(String clientId, boolean cleanStart, MqttClient client, MqttMessage will) {
        this.clientId = clientId;
        this.cleanStart = cleanStart;
        this.client = client;
        this.will = will;
    }

    public String clientId() {
        return clientId;
    }

    public boolean cleanStart() {
        return cleanStart;
    }

    public MqttClient client() {
        return client;
    }

    public MqttMessage will() {
        return will;
    }
}
