package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/** Broker 事件日志，便于测试与排障观察“接管/遗嘱/重复 ACK”等结论。 */
public interface BrokerEvents {

    default void onConnect(String clientId, boolean cleanStart, boolean resumed) {
    }

    default void onTakeover(String clientId) {
    }

    default void onWillPublished(String clientId, MqttMessage will) {
    }

    default void onDeliver(String clientId, MqttMessage message, boolean redelivery) {
    }

    default void onDuplicateAck(String clientId, long packetId) {
    }

    default void onDisconnect(String clientId, boolean clean) {
    }
}
