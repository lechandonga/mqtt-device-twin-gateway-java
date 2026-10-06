package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 纯内存 MQTT 风格 Broker：
 * <ul>
 *   <li>同 clientId 再次连接 = 接管：旧连接关闭并发布其遗嘱；</li>
 *   <li>cleanStart=false 时跨连接保留订阅/飞行/离线消息；</li>
 *   <li>QoS0 至多一次（离线直接丢弃），QoS1 至少一次（离线排队、重连重投、重复 ACK 幂等）；</li>
 *   <li>同一 clientId 的消息严格按 publish 顺序投递（broker 级锁串行化该设备的路由）。</li>
 * </ul>
 */
@Component
public class InMemoryMqttBroker {

    private static final Logger log = LoggerFactory.getLogger(InMemoryMqttBroker.class);

    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    private final Map<String, Session> persistentSessions = new ConcurrentHashMap<>();
    private final Object brokerLock = new Object();
    private final AtomicLong packetIdSequence = new AtomicLong(0);

    private final List<BrokerEvents> listeners = new ArrayList<>();

    public void addListener(BrokerEvents listener) {
        listeners.add(listener);
    }

    private void emit(java.util.function.Consumer<BrokerEvents> action) {
        for (BrokerEvents listener : listeners) {
            action.accept(listener);
        }
    }

    public long nextPacketId() {
        return packetIdSequence.incrementAndGet();
    }

    /** 建立连接；返回该设备使用的会话（可能是恢复的旧会话）。 */
    public Session connect(String clientId, boolean cleanStart, MqttClient client, MqttMessage will) {
        Object lock = brokerLock;
        synchronized (lock) {
            Connection previous = connections.get(clientId);
            if (previous != null) {
                log.info("[broker] clientId={} 被新连接接管，关闭旧连接", clientId);
                emit(e -> e.onTakeover(clientId));
                connections.remove(clientId);
                Session oldSession = persistentSessions.get(clientId);
                if (oldSession != null) {
                    oldSession.moveInFlightBackToQueue();
                }
                publishWill(clientId, previous);
                previous.client().onSessionClosed(MqttClient.CloseReason.TAKEOVER, previous.will() != null);
            }

            Session session;
            boolean resumed;
            if (cleanStart) {
                session = new Session(clientId);
                persistentSessions.put(clientId, session);
                resumed = false;
            } else {
                session = persistentSessions.computeIfAbsent(clientId, Session::new);
                resumed = previous == null
                        && (!session.subscriptions().isEmpty()
                            || session.queuedCount() > 0
                            || !session.redeliveryOrder().isEmpty());
            }
            connections.put(clientId, new Connection(clientId, cleanStart, client, will));
            log.info("[broker] clientId={} 已连接 cleanStart={} resumed={}", clientId, cleanStart, resumed);
            emit(e -> e.onConnect(clientId, cleanStart, resumed));

            if (!cleanStart) {
                drainQueueTo(clientId, client, session, false);
            }
            return session;
        }
    }

    private void publishWill(String clientId, Connection connection) {
        MqttMessage will = connection.will();
        if (will != null) {
            log.info("[broker] clientId={} 触发遗嘱 -> {}", clientId, will);
            emit(e -> e.onWillPublished(clientId, will));
            route(will, clientId);
        }
    }

    /** 设备正常/异常断开。clean=true 表示客户端发了正常 DISCONNECT：不发遗嘱。 */
    public void disconnect(String clientId, boolean clean) {
        Object lock = brokerLock;
        synchronized (lock) {
            Connection connection = connections.remove(clientId);
            if (connection == null) {
                return;
            }
            Session session = persistentSessions.get(clientId);
            if (!clean) {
                publishWill(clientId, connection);
                if (session != null) {
                    session.moveInFlightBackToQueue();
                }
            }
            if (connection.cleanStart()) {
                persistentSessions.remove(clientId);
            }
            connection.client().onSessionClosed(MqttClient.CloseReason.SERVER_CLOSE, false);
            log.info("[broker] clientId={} 断开 clean={}", clientId, clean);
            emit(e -> e.onDisconnect(clientId, clean));
        }
    }

    public Session subscribe(String clientId, String filter, Qos maxQos) {
        Object lock = brokerLock;
        synchronized (lock) {
            Session session = requireSession(clientId);
            session.subscribe(filter, maxQos);
            log.info("[broker] clientId={} 订阅 {} (maxQos={})", clientId, filter, maxQos);
            return session;
        }
    }

    /**
     * 发布消息并立即路由给在线订阅者。
     * @param publisherId 发布者 clientId（不投递给自己遗嘱以外的自己？这里仍正常投递），可为 null（服务端发布）
     */
    public void publish(MqttMessage message) {
        publish(message, null);
    }

    public void publish(MqttMessage message, String publisherId) {
        log.info("[broker] 收到发布 {}", message);
        route(message, publisherId);
    }

    private void route(MqttMessage message, String publisherId) {
        List<String> targets = new ArrayList<>(persistentSessions.keySet());
        for (String clientId : targets) {
            Object lock = brokerLock;
            synchronized (lock) {
                Session session = persistentSessions.get(clientId);
                if (session == null) {
                    continue;
                }
                String matched = bestMatchingFilter(session, message.topic());
                if (matched == null) {
                    continue;
                }
                Qos effective = effectiveQos(message.qos(), session.subscriptions().get(matched));
                MqttMessage toDeliver = downgrade(message, effective);
                Connection connection = connections.get(clientId);
                if (connection == null) {
                    if (effective == Qos.AT_LEAST_ONCE) {
                        session.enqueueOffline(toDeliver);
                        log.info("[broker] {} 离线，QoS1 消息入队（队列长度={}）: {}",
                                clientId, session.queuedCount(), toDeliver);
                    } else {
                        log.info("[broker] {} 离线，QoS0 消息丢弃: {}", clientId, toDeliver);
                    }
                    continue;
                }
                deliver(clientId, connection.client(), session, toDeliver, false);
            }
        }
    }

    private void deliver(String clientId, MqttClient client, Session session,
                         MqttMessage message, boolean redelivery) {
        if (message.qos() == Qos.AT_LEAST_ONCE) {
            session.markInFlight(message);
        }
        log.info("[broker] 投递给 {} {}{}", clientId, redelivery ? "[重投] " : "", message);
        emit(e -> e.onDeliver(clientId, message, redelivery));
        client.onMessage(message);
    }

    private void drainQueueTo(String clientId, MqttClient client, Session session, boolean redelivery) {
        MqttMessage message;
        while ((message = session.pollPending()) != null) {
            String matched = bestMatchingFilter(session, message.topic());
            if (matched == null) {
                // 重连后订阅已取消：QoS1 消息不再投递（MQTT 会话语义：订阅删除后丢弃）
                log.info("[broker] {} 重连后订阅已取消，丢弃消息 {}", clientId, message);
                continue;
            }
            Qos effective = effectiveQos(message.qos(), session.subscriptions().get(matched));
            deliver(clientId, client, session, downgrade(message, effective), true);
        }
    }

    /** 设备对 QoS1 消息回 ACK；重复 ACK 幂等（返回 false 且计为重复）。 */
    public boolean acknowledge(String clientId, long packetId) {
        Object lock = brokerLock;
        synchronized (lock) {
            Session session = persistentSessions.get(clientId);
            if (session == null) {
                return false;
            }
            boolean first = session.acknowledge(packetId);
            if (!first) {
                log.info("[broker] clientId={} 重复 PUBACK packetId={}，忽略", clientId, packetId);
                emit(e -> e.onDuplicateAck(clientId, packetId));
            }
            return first;
        }
    }

    /** 模拟“未 ACK 的消息重投”（真实 broker 会定时重投，这里供网关/测试显式触发）。 */
    public int redeliverInflight(String clientId) {
        Object lock = brokerLock;
        synchronized (lock) {
            Connection connection = connections.get(clientId);
            Session session = persistentSessions.get(clientId);
            if (connection == null || session == null) {
                return 0;
            }
            int count = 0;
            for (MqttMessage message : session.redeliveryOrder()) {
                deliver(clientId, connection.client(), session, message, true);
                count++;
            }
            return count;
        }
    }

    public Session requireSession(String clientId) {
        Session session = persistentSessions.get(clientId);
        if (session == null) {
            throw new IllegalStateException("clientId=" + clientId + " 无会话（未连接或 cleanStart）");
        }
        return session;
    }

    public boolean isConnected(String clientId) {
        return connections.containsKey(clientId);
    }

    static String bestMatchingFilter(Session session, String topic) {
        return session.subscriptions().keySet().stream()
                .filter(f -> TopicMatcher.matches(f, topic))
                .min(TopicMatcher.specificity())
                .orElse(null);
    }

    private static Qos effectiveQos(Qos published, Qos subscribedMax) {
        if (subscribedMax == null) {
            return published;
        }
        return published.level() <= subscribedMax.level() ? published : subscribedMax;
    }

    private static MqttMessage downgrade(MqttMessage message, Qos qos) {
        return message.qos() == qos ? message
                : new MqttMessage(message.packetId(), message.topic(), message.payload(), qos);
    }
}
