package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import java.util.List;

/** 设备侧客户端：由 {@link InMemoryMqttBroker} 投递的消息回调到该接口。 */
public interface MqttClient {

    String clientId();

    /** Broker 把消息投递给设备（QoS1 消息在设备 ACK 前会保留在会话中）。 */
    void onMessage(MqttMessage message);

    /**
     * 被同 clientId 的新连接接管，或被服务端踢出时回调。
     * @param reason 会话结束原因（TAKEOVER / SERVER_CLOSE）
     * @param willAsPublished 本连接遗嘱是否已经发布（接管时发布）
     */
    default void onSessionClosed(CloseReason reason, boolean willAsPublished) {
    }

    enum CloseReason {
        TAKEOVER, SERVER_CLOSE
    }

    /** 测试/排障用：当前未确认（飞行中）的 packetId。 */
    default List<Long> inFlightPacketIds() {
        return List.of();
    }
}
