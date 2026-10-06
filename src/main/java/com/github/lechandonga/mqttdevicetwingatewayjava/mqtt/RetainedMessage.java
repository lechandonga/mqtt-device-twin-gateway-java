package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * Retained 消息：新订阅建立时立即投递一次保留消息，与实时流区分开。
 */
public record RetainedMessage(String topic, String payload, Qos qos) {
}
