package com.github.lechandonga.mqttdevicetwingatewayjava.gateway;

import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandService;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttClient;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttMessage;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Session;
import org.springframework.stereotype.Component;

/**
 * 统一设备接入门面：连接、订阅、遗嘱、断线重连都从这里走，
 * 保证“重连后排空指令队列”这一动作与 MQTT 会话恢复在同一把设备锁语义下顺序发生。
 */
@Component
public class DeviceGateway {

    private final InMemoryMqttBroker broker;
    private final CommandService commands;

    public DeviceGateway(InMemoryMqttBroker broker, CommandService commands) {
        this.broker = broker;
        this.commands = commands;
    }

    public Session connect(String deviceId, boolean cleanStart, MqttClient client,
                           MqttMessage will) {
        Session session = broker.connect(deviceId, cleanStart, client, will);
        if (!cleanStart) {
            commands.drainOnReconnect(deviceId);
        }
        return session;
    }

    public Session subscribe(String deviceId, String filter, Qos maxQos) {
        return broker.subscribe(deviceId, filter, maxQos);
    }

    public void disconnect(String deviceId, boolean clean) {
        broker.disconnect(deviceId, clean);
    }

    public void publish(MqttMessage message) {
        broker.publish(message);
    }

    public boolean acknowledge(String deviceId, long packetId) {
        return broker.acknowledge(deviceId, packetId);
    }

    public InMemoryMqttBroker broker() {
        return broker;
    }
}
