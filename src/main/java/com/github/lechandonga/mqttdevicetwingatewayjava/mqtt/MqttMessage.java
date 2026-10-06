package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * 一条 MQTT 风格的应用消息。{@code packetId} 用于 QoS 1 会话内去重：
 * 重连后未确认消息沿用同一 packetId，重复 PUBACK 由此被识别。
 */
public record MqttMessage(long packetId, String topic, byte[] payload, Qos qos) {

    public MqttMessage {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (payload == null) {
            payload = new byte[0];
        }
        if (qos == null) {
            qos = Qos.AT_MOST_ONCE;
        }
    }

    public String payloadAsString() {
        return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "MqttMessage{packetId=" + packetId + ", topic='" + topic + "', qos=" + qos.level()
                + ", payload='" + payloadAsString() + "'}";
    }
}
