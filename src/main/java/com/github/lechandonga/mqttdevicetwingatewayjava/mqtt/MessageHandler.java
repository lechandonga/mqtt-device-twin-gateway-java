package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * 订阅者回调。duplicate=true 表示这是至少一次语义下的重复投递。
 */
@FunctionalInterface
public interface MessageHandler {
    void handle(String topic, String payload, Qos qos, boolean duplicate);
}
