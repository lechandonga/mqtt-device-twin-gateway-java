package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * 遗嘱消息：异常掉线或被接管时由 broker 代为发布。
 */
public record WillMessage(String topic, String payload, Qos qos, boolean retained) {
}
