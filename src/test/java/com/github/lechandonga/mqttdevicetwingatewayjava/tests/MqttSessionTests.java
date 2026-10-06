package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.InMemoryMqttBroker;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.TopicMatcher;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.WillMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：会话接管与遗嘱、持久会话重连、QoS0/1 投递语义、重复投递去重、乱序确认、通配符、retained。
 */
@DisplayName("MQTT 会话与投递语义")
class MqttSessionTests {

    @RegisterExtension
    static final CoverageLoggerExtension COVERAGE = new CoverageLoggerExtension();

    private InMemoryMqttBroker broker;

    @BeforeEach
    void setUp() {
        broker = new InMemoryMqttBroker();
    }

    private List<String> trackingClient(String clientId, String filter, Qos qos) {
        List<String> received = new ArrayList<>();
        broker.connect(clientId, false);
        broker.subscribe(clientId, filter, qos, (t, p, q, dup) -> received.add(p + "|dup=" + dup));
        return received;
    }

    @Test
    @DisplayName("同一 clientId 重连按接管处理：旧连接遗嘱触发一次，新连接不触发遗嘱")
    void takeoverPublishesWillOfOldConnection() {
        List<String> monitor = new ArrayList<>();
        broker.connect("monitor", true);
        broker.subscribe("monitor", "status/#", Qos.AT_LEAST_ONCE,
                (t, p, q, dup) -> monitor.add(p));

        broker.connect("dev-1", true,
                new WillMessage("status/dev-1", "offline", Qos.AT_LEAST_ONCE, false));
        broker.connect("dev-1", true,
                new WillMessage("status/dev-1", "offline-2", Qos.AT_LEAST_ONCE, false));
        broker.disconnect("dev-1");

        assertEquals(List.of("offline"), monitor, "接管应发布旧连接遗嘱；正常 DISCONNECT 不发遗嘱");
        assertFalse(broker.isOnline("dev-1"));
    }

    @Test
    @DisplayName("异常掉线触发遗嘱；cleanSession=false 重连后订阅关系保留(sessionPresent)")
    void persistentSessionKeepsSubscriptions() {
        List<String> monitor = trackingClient("dev-2", "data/+", Qos.AT_MOST_ONCE);
        broker.connectionLost("dev-2");
        boolean sessionPresent = broker.connect("dev-2", false);

        assertTrue(sessionPresent, "持久会话重连应复用会话");
        assertEquals(1, broker.subscriptionCount("dev-2"), "订阅关系不应丢失");
        broker.publish("data/temp", "v1", Qos.AT_MOST_ONCE, "m1");
        assertEquals(List.of("v1|dup=false"), monitor, "无需重新订阅即可继续收消息");
    }

    @Test
    @DisplayName("QoS1：离线排队 -> 重连按序重投 -> 未确认消息不丢；重复确认幂等")
    void qos1OfflineQueueAndRedelivery() {
        List<String> dev = trackingClient("dev-3", "cmd/dev-3", Qos.AT_LEAST_ONCE);
        broker.connectionLost("dev-3");

        broker.publish("cmd/dev-3", "c1", Qos.AT_LEAST_ONCE, "id-1");
        broker.publish("cmd/dev-3", "c2", Qos.AT_LEAST_ONCE, "id-2");
        assertEquals(2, broker.queuedCount("dev-3"));
        assertEquals(0, broker.inFlightCount("dev-3"));

        broker.connect("dev-3", false);
        int flushed = broker.flushPending("dev-3");
        assertEquals(2, flushed);
        assertEquals(List.of("c1|dup=false", "c2|dup=false"), dev, "离线消息按序送达");

        List<Integer> packetIds = List.of(1, 2);
        packetIds.forEach(id -> assertTrue(broker.ack("dev-3", id)));
        assertEquals(0, broker.inFlightCount("dev-3"));
        assertFalse(broker.ack("dev-3", 1), "重复 PUBACK 必须幂等且不产生副作用");
    }

    @Test
    @DisplayName("QoS1：未确认消息重连后标记 duplicate 重投，消费端不重复消费")
    void unackedRedeliveryIsDuplicateAndDeduped() {
        List<String> dev = new ArrayList<>();
        broker.connect("dev-4", false);
        broker.subscribe("dev-4", "t/x", Qos.AT_LEAST_ONCE,
                (t, p, q, dup) -> dev.add(p + "|dup=" + dup));

        broker.publish("t/x", "hello", Qos.AT_LEAST_ONCE, "msg-1");
        broker.connectionLost("dev-4");
        broker.connect("dev-4", false);
        broker.flushPending("dev-4");

        assertEquals(List.of("hello|dup=false", "hello|dup=true"), dev,
                "第二次为重复投递；业务层通过 dup 标记去重");
    }

    @Test
    @DisplayName("QoS0：至多一次，离线期间消息直接丢弃，不排队不重投")
    void qos0AtMostOnceDropsWhileOffline() {
        List<String> dev = trackingClient("dev-5", "fire", Qos.AT_MOST_ONCE);
        broker.connectionLost("dev-5");
        broker.publish("fire", "lost", Qos.AT_MOST_ONCE, "m");
        broker.connect("dev-5", false);
        broker.flushPending("dev-5");
        assertTrue(dev.isEmpty());
        assertEquals(0, broker.queuedCount("dev-5"));
    }

    @Test
    @DisplayName("通配符匹配稳定可解释：+ 单层，# 多层，层级边界敏感")
    void wildcardMatching() {
        assertTrue(TopicMatcher.matches("a/+/c", "a/b/c"));
        assertFalse(TopicMatcher.matches("a/+/c", "a/b/x/c"));
        assertTrue(TopicMatcher.matches("a/#", "a/b/c/d"));
        assertTrue(TopicMatcher.matches("a/#", "a"));
        assertFalse(TopicMatcher.matches("a/+", "a"));
        assertFalse(TopicMatcher.matches("a/b", "a/b/"));
        assertTrue(TopicMatcher.matches("#", "anything/here"));
    }

    @Test
    @DisplayName("retained：新订阅立即收到最新保留消息，空 payload 清除保留")
    void retainedMessageDeliveredToNewSubscriber() {
        broker.publish("cfg/level", "high", Qos.AT_MOST_ONCE, "r1", true);
        List<String> late = new ArrayList<>();
        broker.connect("late", true);
        broker.subscribe("late", "cfg/#", Qos.AT_MOST_ONCE, (t, p, q, dup) -> late.add(p));
        assertEquals(List.of("high"), late);

        broker.publish("cfg/level", "", Qos.AT_MOST_ONCE, "r2", true);
        List<String> later = new ArrayList<>();
        broker.connect("later", true);
        broker.subscribe("later", "cfg/#", Qos.AT_MOST_ONCE, (t, p, q, dup) -> later.add(p));
        assertTrue(later.isEmpty());
    }
}
