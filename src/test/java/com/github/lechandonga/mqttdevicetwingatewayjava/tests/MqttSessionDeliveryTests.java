package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.BrokerEvents;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttMessage;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.TopicMatcher;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessCase;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessLogging;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessModule;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.RecordingClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(BusinessLogging.class)
@BusinessModule("设备接入 / 会话 / 投递语义（接管、遗嘱、QoS0 vs QoS1、通配符）")
class MqttSessionDeliveryTests {

    private InMemoryMqttBroker broker;
    private final List<String> events = new ArrayList<>();

    @BeforeEach
    void setUp() {
        broker = new InMemoryMqttBroker();
        events.clear();
        broker.addListener(new BrokerEvents() {
            @Override
            public void onTakeover(String clientId) {
                events.add("TAKEOVER:" + clientId);
            }

            @Override
            public void onWillPublished(String clientId, MqttMessage will) {
                events.add("WILL:" + will.topic() + "=" + will.payloadAsString());
            }

            @Override
            public void onDuplicateAck(String clientId, long packetId) {
                events.add("DUPACK:" + packetId);
            }
        });
    }

    private MqttMessage msg(String topic, String payload, Qos qos) {
        return new MqttMessage(broker.nextPacketId(), topic,
                payload.getBytes(StandardCharsets.UTF_8), qos);
    }

    @Test
    @BusinessCase("同 clientId 再连接按接管处理：旧连接被关闭且遗嘱只发布一次")
    void takeoverClosesOldConnectionAndPublishesWill() {
        RecordingClient oldDevice = new RecordingClient("dev-1");
        RecordingClient watcher = new RecordingClient("watcher");
        broker.connect("watcher", true, watcher, null);
        broker.subscribe("watcher", "status/dev-1", Qos.AT_LEAST_ONCE);
        broker.connect("dev-1", true, oldDevice,
                msg("status/dev-1", "offline", Qos.AT_LEAST_ONCE));

        RecordingClient newDevice = new RecordingClient("dev-1");
        broker.connect("dev-1", true, newDevice, null);

        assertThat(oldDevice.closeReason).isEqualTo(RecordingClient.CloseReason.TAKEOVER);
        assertThat(oldDevice.willPublishedOnClose).isTrue();
        assertThat(watcher.payloads()).containsExactly("offline");
        assertThat(events).contains("TAKEOVER:dev-1", "WILL:status/dev-1=offline");
    }

    @Test
    @BusinessCase("正常 DISCONNECT 不发遗嘱；异常断线才发遗嘱")
    void cleanDisconnectSkipsWill() {
        RecordingClient watcher = new RecordingClient("watcher");
        broker.connect("watcher", true, watcher, null);
        broker.subscribe("watcher", "status/d", Qos.AT_LEAST_ONCE);
        broker.connect("d", true, new RecordingClient("d"),
                msg("status/d", "last-will", Qos.AT_LEAST_ONCE));

        broker.disconnect("d", true);
        assertThat(watcher.payloads()).isEmpty();

        RecordingClient d2 = new RecordingClient("d");
        broker.connect("d", true, d2, msg("status/d", "last-will", Qos.AT_LEAST_ONCE));
        broker.disconnect("d", false);
        assertThat(watcher.payloads()).containsExactly("last-will");
    }

    @Test
    @BusinessCase("持久会话重连：订阅关系保留、离线 QoS1 消息按序补投，QoS0 离线丢弃")
    void persistentSessionResumesSubscriptionsAndBacklog() {
        RecordingClient device = new RecordingClient("dev-2");
        broker.connect("dev-2", false, device, null);
        broker.subscribe("dev-2", "cmd/+/rx", Qos.AT_LEAST_ONCE);
        broker.disconnect("dev-2", false);

        broker.publish(msg("cmd/1/rx", "qos1-a", Qos.AT_LEAST_ONCE));
        broker.publish(msg("cmd/2/rx", "qos0-b", Qos.AT_MOST_ONCE));
        broker.publish(msg("cmd/3/rx", "qos1-c", Qos.AT_LEAST_ONCE));

        RecordingClient device2 = new RecordingClient("dev-2");
        broker.connect("dev-2", false, device2, null);

        assertThat(device2.payloads()).containsExactly("qos1-a", "qos1-c");
        assertThat(broker.requireSession("dev-2").subscriptions()).containsKey("cmd/+/rx");
    }

    @Test
    @BusinessCase("未确认消息不丢：断线飞行中消息回到队首，重连后先于新消息重投")
    void inflightMessagesAreRedeliveredBeforeNewBacklog() {
        RecordingClient device = new RecordingClient("dev-3");
        broker.connect("dev-3", false, device, null);
        broker.subscribe("dev-3", "t", Qos.AT_LEAST_ONCE);

        MqttMessage first = msg("t", "first", Qos.AT_LEAST_ONCE);
        broker.publish(first);
        // 设备收到但没 ACK 就掉线；掉线期间又来一条
        broker.disconnect("dev-3", false);
        MqttMessage second = msg("t", "second", Qos.AT_LEAST_ONCE);
        broker.publish(second);

        RecordingClient device2 = new RecordingClient("dev-3");
        broker.connect("dev-3", false, device2, null);

        assertThat(device2.payloads()).containsExactly("first", "second");
        // 重投沿用原 packetId
        assertThat(device2.packetIds()).containsExactly(first.packetId(), second.packetId());

        // 第一个消息再被重投一次（至少一次语义允许重复），ACK 两次只有第一次有效
        broker.redeliverInflight("dev-3");
        assertThat(broker.acknowledge("dev-3", first.packetId())).isTrue();
        assertThat(broker.acknowledge("dev-3", first.packetId())).isFalse();
        assertThat(events).contains("DUPACK:" + first.packetId());
    }

    @Test
    @BusinessCase("至少一次的重复投递：消费端按 packetId 去重，只消费一次且不丢消息")
    void consumerDeduplicatesRedeliveryByPacketId() {
        RecordingClient device = new RecordingClient("dev-4");
        broker.connect("dev-4", false, device, null);
        broker.subscribe("dev-4", "t", Qos.AT_LEAST_ONCE);
        broker.publish(msg("t", "payload-x", Qos.AT_LEAST_ONCE));

        // 模拟 broker 重投
        broker.redeliverInflight("dev-4");
        broker.redeliverInflight("dev-4");
        assertThat(device.deliveries).hasSize(3);

        java.util.Set<Long> consumed = new java.util.HashSet<>();
        List<String> applied = new ArrayList<>();
        for (RecordingClient.Delivery delivery : device.deliveries) {
            if (consumed.add(delivery.message().packetId())) {
                applied.add(delivery.message().payloadAsString());
            }
        }
        assertThat(applied).containsExactly("payload-x");
    }

    @Test
    @BusinessCase("通配符匹配稳定：+ 单层、# 多层、具体过滤器优先，匹配过程可解释")
    void wildcardMatchingIsDeterministicAndExplainable() {
        assertThat(TopicMatcher.matches("a/+/c", "a/b/c")).isTrue();
        assertThat(TopicMatcher.matches("a/+/c", "a/b/x/c")).isFalse();
        assertThat(TopicMatcher.matches("a/#", "a/b/c/d")).isTrue();
        assertThat(TopicMatcher.matches("a/+", "a/b/c")).isFalse();

        // 命中多个过滤器时“更具体”的稳定排在前面
        List<String> filters = List.of("a/#", "a/+/c", "a/b/c", "x/#");
        String best = filters.stream()
                .filter(f -> TopicMatcher.matches(f, "a/b/c"))
                .min(TopicMatcher.specificity())
                .orElseThrow();
        assertThat(best).isEqualTo("a/b/c");

        List<String> explanation = TopicMatcher.explain("a/+/c", "a/b/c");
        assertThat(explanation).anyMatch(s -> s.contains("'+' 匹配 'b'"));
    }

    @Test
    @BusinessCase("订阅 QoS 降级：发布 QoS1、订阅 QoS0 时按至多一次投递")
    void subscriptionDowngradesQos() {
        RecordingClient device = new RecordingClient("dev-5");
        broker.connect("dev-5", false, device, null);
        broker.subscribe("dev-5", "t", Qos.AT_MOST_ONCE);
        broker.publish(msg("t", "downgraded", Qos.AT_LEAST_ONCE));
        assertThat(device.deliveries.get(0).message().qos()).isEqualTo(Qos.AT_MOST_ONCE);
        // 未进入飞行队列，重投不会再发
        assertThat(broker.redeliverInflight("dev-5")).isZero();
    }
}
