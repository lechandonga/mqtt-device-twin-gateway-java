package com.github.lechandonga.mqttdevicetwingatewayjava.support;

import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttClient;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 测试用设备端：记录收到的消息及被接管/关闭事件。 */
public class RecordingClient implements MqttClient {

    public record Delivery(MqttMessage message, boolean redelivery) {
    }

    private final String clientId;
    public final List<Delivery> deliveries = Collections.synchronizedList(new ArrayList<>());
    public volatile CloseReason closeReason;
    public volatile boolean willPublishedOnClose;

    public RecordingClient(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public void onMessage(MqttMessage message) {
        deliveries.add(new Delivery(message, false));
    }

    @Override
    public void onSessionClosed(CloseReason reason, boolean willAsPublished) {
        this.closeReason = reason;
        this.willPublishedOnClose = willAsPublished;
    }

    public List<String> payloads() {
        return deliveries.stream().map(d -> d.message().payloadAsString()).toList();
    }

    public List<Long> packetIds() {
        return deliveries.stream().map(d -> d.message().packetId()).toList();
    }

    public void reset() {
        deliveries.clear();
    }
}
