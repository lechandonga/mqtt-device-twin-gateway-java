package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 已下发但未收到 PUBACK 的 QoS1 消息。重连时按 packetId 重投。
 */
public final class InFlightMessage {
    private final int packetId;
    private final String topic;
    private final String payload;
    private final AtomicInteger deliveryCount = new AtomicInteger(0);

    public InFlightMessage(int packetId, String topic, String payload) {
        this.packetId = packetId;
        this.topic = topic;
        this.payload = payload;
    }

    public int packetId() {
        return packetId;
    }

    public String topic() {
        return topic;
    }

    public String payload() {
        return payload;
    }

    public int markRedelivered() {
        return deliveryCount.incrementAndGet();
    }

    public int deliveryCount() {
        return deliveryCount.get();
    }
}
