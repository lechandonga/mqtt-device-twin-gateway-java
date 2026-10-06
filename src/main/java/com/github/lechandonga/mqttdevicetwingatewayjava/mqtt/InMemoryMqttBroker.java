package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 纯内存的 MQTT 风格 broker 模拟器。
 * <p>关键语义：
 * <ul>
 *     <li>同一 clientId 再次连接 = 接管：旧连接立即置离线并触发遗嘱，随后新连接完成。</li>
 *     <li>cleanSession=false：订阅关系、未确认(in-flight)消息、离线消息在重连后保留并继续投递。</li>
 *     <li>QoS0 至多一次：在线才投递，丢弃即丢弃；QoS1 至少一次：离线排队，重连后重投，重复 PUBACK 幂等。</li>
 *     <li>消费端按 messageId 去重，重复投递（duplicate=true）不会被重复消费。</li>
 *     <li>支持 '+'/'#' 通配符与 retained 保留消息。</li>
 * </ul>
 */
@Component
public class InMemoryMqttBroker {

    private static final Logger log = LoggerFactory.getLogger(InMemoryMqttBroker.class);

    private record Subscriber(String clientId, String filter, Qos qos, MessageHandler handler) {
    }

    private final Map<String, Session> sessions = new LinkedHashMap<>();
    private final Map<String, WillMessage> wills = new LinkedHashMap<>();
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
    private final Map<String, RetainedMessage> retained = new LinkedHashMap<>();
    private final List<ConnectionListener> listeners = new CopyOnWriteArrayList<>();

    public synchronized void addConnectionListener(ConnectionListener listener) {
        listeners.add(listener);
    }

    /**
     * 客户端连接。返回是否复用了已有会话（sessionPresent）。
     */
    public synchronized boolean connect(String clientId, boolean cleanSession, WillMessage will) {
        Session old = sessions.get(clientId);
        boolean sessionPresent = false;
        if (old != null) {
            if (old.isOnline()) {
                log.info("[MQTT] client '{}' 再次连接，接管旧连接并触发遗嘱", clientId);
                fireDisconnected(clientId, true);
                WillMessage lastWill = wills.remove(clientId);
                if (lastWill != null) {
                    publishWill(clientId, lastWill);
                }
            }
            if (old.cleanSession() || cleanSession) {
                purgeSession(old);
                old = null;
            }
        }
        if (old == null) {
            sessions.put(clientId, new Session(clientId, cleanSession));
        } else {
            old.setOnline(true);
            sessionPresent = true;
            log.info("[MQTT] client '{}' 复用持久会话：订阅 {} 个，未确认 {} 条，离线队列 {} 条",
                    clientId, old.subscriptions().size(), old.inFlight().size(), old.queued().size());
        }
        if (will != null) {
            wills.put(clientId, will);
        }
        fireConnected(clientId, sessionPresent);
        return sessionPresent;
    }

    public boolean connect(String clientId, boolean cleanSession) {
        return connect(clientId, cleanSession, null);
    }

    /**
     * 主动正常断开：不触发遗嘱（与 MQTT DISCONNECT 一致）。clean 会话随之清除。
     */
    public synchronized void disconnect(String clientId) {
        Session session = sessions.get(clientId);
        if (session == null) {
            return;
        }
        wills.remove(clientId);
        subscribers.removeIf(s -> s.clientId().equals(clientId));
        if (session.cleanSession()) {
            purgeSession(session);
            sessions.remove(clientId);
        } else {
            session.setOnline(false);
        }
        fireDisconnected(clientId, false);
    }

    /**
     * 模拟异常掉线（网络中断）：持久会话保留，并触发遗嘱。
     */
    public synchronized void connectionLost(String clientId) {
        Session session = sessions.get(clientId);
        if (session == null || !session.isOnline()) {
            return;
        }
        session.setOnline(false);
        log.info("[MQTT] client '{}' 异常掉线，持久会话保留（订阅 {}，未确认 {}）",
                clientId, session.subscriptions().size(), session.inFlight().size());
        WillMessage will = wills.remove(clientId);
        if (will != null) {
            publishWill(clientId, will);
        }
        fireDisconnected(clientId, false);
    }

    private void publishWill(String ownerClientId, WillMessage will) {
        log.info("[MQTT] 发布 client '{}' 的遗嘱消息 topic={}", ownerClientId, will.topic());
        doPublish(will.topic(), will.payload(), will.qos(), true, will.retained());
    }

    public synchronized void subscribe(String clientId, String filter, Qos maxQos, MessageHandler handler) {
        Session session = requireSession(clientId);
        session.subscriptions().add(filter);
        subscribers.removeIf(s -> s.clientId().equals(clientId) && s.filter().equals(filter));
        subscribers.add(new Subscriber(clientId, filter, maxQos, handler));
        log.info("[MQTT] client '{}' 订阅 {} (maxQoS={})", clientId, filter, maxQos);
        for (RetainedMessage rm : retained.values()) {
            if (TopicMatcher.matches(filter, rm.topic())) {
                Qos effective = rm.qos().ordinal() > maxQos.ordinal() ? maxQos : rm.qos();
                deliver(session, new Subscriber(clientId, filter, effective, handler),
                        rm.topic(), rm.payload(), effective, "retained");
            }
        }
    }

    /** 取消订阅：同时移除实时订阅者。 */
    public synchronized void unsubscribe(String clientId, String filter) {
        Session session = requireSession(clientId);
        session.subscriptions().remove(filter);
        subscribers.removeIf(s -> s.clientId().equals(clientId) && s.filter().equals(filter));
    }

    /**
     * 发布消息。messageId 用于消费端幂等去重；payload 本身也携带在消息中。
     */
    public synchronized PublishResult publish(String topic, String payload, Qos qos,
                                              String messageId, boolean retain) {
        return doPublish(topic, payload, qos, false, retain);
    }

    public PublishResult publish(String topic, String payload, Qos qos, String messageId) {
        return publish(topic, payload, qos, messageId, false);
    }

    private PublishResult doPublish(String topic, String payload, Qos qos, boolean isWill, boolean retain) {
        if (retain) {
            if (payload.isEmpty()) {
                retained.remove(topic);
            } else {
                retained.put(topic, new RetainedMessage(topic, payload, qos));
            }
        }
        int delivered = 0;
        int queued = 0;
        List<String> matched = new ArrayList<>();
        List<Subscriber> matchedSubs = new ArrayList<>();
        for (Subscriber sub : subscribers) {
            if (TopicMatcher.matches(sub.filter(), topic)) {
                matchedSubs.add(sub);
                matched.add(sub.clientId() + "@" + sub.filter());
            }
        }
        for (Subscriber sub : matchedSubs) {
            Session session = sessions.get(sub.clientId());
            Qos effective = qos.ordinal() > sub.qos().ordinal() ? sub.qos() : qos;
            boolean online = session != null && session.isOnline();
            if (effective == Qos.AT_MOST_ONCE) {
                if (online) {
                    deliver(session, sub, topic, payload, effective, isWill ? "will" : "live");
                    delivered++;
                }
                // QoS0 离线直接丢弃，至多一次
                continue;
            }
            // QoS1
            if (online) {
                deliver(session, sub, topic, payload, effective, isWill ? "will" : "live");
                delivered++;
            } else if (session != null && !session.cleanSession()) {
                int packetId = session.allocatePacketId();
                session.queued().put(packetId, new InFlightMessage(packetId, topic, payload));
                queued++;
                log.info("[MQTT] client '{}' 离线，QoS1 消息进入离线队列 packetId={} topic={}",
                        sub.clientId(), packetId, topic);
            }
        }
        return new PublishResult(delivered, queued, List.copyOf(matched));
    }

    private void deliver(Session session, Subscriber sub, String topic, String payload,
                         Qos effective, String source) {
        Integer packetId = null;
        boolean duplicate = false;
        if (effective == Qos.AT_LEAST_ONCE) {
            InFlightMessage inflight = null;
            for (InFlightMessage m : session.inFlight().values()) {
                if (m.topic().equals(topic) && m.payload().equals(payload)) {
                    inflight = m;
                    break;
                }
            }
            if (inflight == null) {
                int id = session.allocatePacketId();
                inflight = new InFlightMessage(id, topic, payload);
                session.inFlight().put(id, inflight);
            }
            packetId = inflight.packetId();
            int times = inflight.markRedelivered();
            duplicate = times > 1;
        }
        boolean firstConsume = session.consumedMessageIds().add(topic + "#" + payload);
        log.info("[MQTT] 投递[{}] -> '{}' topic={} qos={} packetId={} dup={} firstConsume={}",
                source, sub.clientId(), topic, effective, packetId, duplicate, firstConsume);
        sub.handler().handle(topic, payload, effective, duplicate || !firstConsume);
    }

    /**
     * QoS1 确认。重复确认或未知 packetId 均为无副作用操作（幂等）。
     *
     * @return true 表示本次确认真正完成了一条消息
     */
    public synchronized boolean ack(String clientId, int packetId) {
        Session session = requireSession(clientId);
        InFlightMessage removed = session.inFlight().remove(packetId);
        session.queued().remove(packetId);
        if (removed == null) {
            log.warn("[MQTT] client '{}' 重复/未知 PUBACK packetId={}，忽略", clientId, packetId);
            return false;
        }
        log.info("[MQTT] client '{}' 确认 packetId={} (累计投递 {} 次)",
                clientId, packetId, removed.deliveryCount());
        return true;
    }

    /**
     * 重连（或显式调用）后排空离线队列并重投未确认消息：队列按 packetId 升序，保证离线期间按序送达。
     */
    public synchronized int flushPending(String clientId) {
        Session session = requireSession(clientId);
        if (!session.isOnline()) {
            throw new IllegalStateException("client '" + clientId + "' is offline");
        }
        int count = 0;
        List<Integer> queueIds = new ArrayList<>(session.queued().keySet());
        java.util.Set<Integer> flushedFromQueue = new java.util.HashSet<>();
        for (Integer id : queueIds) {
            InFlightMessage msg = session.queued().remove(id);
            if (msg == null) {
                continue;
            }
            session.inFlight().putIfAbsent(id, msg);
            flushedFromQueue.add(id);
            redeliver(session, msg);
            count++;
        }
        for (InFlightMessage msg : new ArrayList<>(session.inFlight().values())) {
            if (flushedFromQueue.contains(msg.packetId())) {
                continue;
            }
            redeliver(session, msg);
            count++;
        }
        if (count > 0) {
            log.info("[MQTT] client '{}' 重投未完成消息 {} 条", clientId, count);
        }
        return count;
    }

    private void redeliver(Session session, InFlightMessage msg) {
        List<Subscriber> mine = subscribers.stream()
                .filter(s -> s.clientId().equals(session.clientId()))
                .filter(s -> TopicMatcher.matches(s.filter(), msg.topic()))
                .toList();
        int times = msg.markRedelivered();
        for (Subscriber sub : mine) {
            boolean firstConsume = session.consumedMessageIds().add(msg.topic() + "#" + msg.payload());
            log.info("[MQTT] 重投 -> '{}' topic={} packetId={} 第{}次 firstConsume={}",
                    session.clientId(), msg.topic(), msg.packetId(), times, firstConsume);
            sub.handler().handle(msg.topic(), msg.payload(), Qos.AT_LEAST_ONCE,
                    times > 1 || !firstConsume);
        }
    }

    public synchronized boolean isOnline(String clientId) {
        Session session = sessions.get(clientId);
        return session != null && session.isOnline();
    }

    public synchronized int queuedCount(String clientId) {
        Session session = sessions.get(clientId);
        return session == null ? 0 : session.queued().size();
    }

    public synchronized int inFlightCount(String clientId) {
        Session session = sessions.get(clientId);
        return session == null ? 0 : session.inFlight().size();
    }

    public synchronized int subscriptionCount(String clientId) {
        Session session = sessions.get(clientId);
        return session == null ? 0 : session.subscriptions().size();
    }

    private Session requireSession(String clientId) {
        Session session = sessions.get(clientId);
        if (session == null) {
            throw new IllegalStateException("no session for client '" + clientId + "'");
        }
        return session;
    }

    private void purgeSession(Session session) {
        subscribers.removeIf(s -> s.clientId().equals(session.clientId()));
        session.subscriptions().clear();
        session.inFlight().clear();
        session.queued().clear();
        session.consumedMessageIds().clear();
    }

    private void fireConnected(String clientId, boolean sessionPresent) {
        for (ConnectionListener l : listeners) {
            l.onConnected(clientId, sessionPresent);
        }
    }

    private void fireDisconnected(String clientId, boolean takenOver) {
        for (ConnectionListener l : listeners) {
            l.onDisconnected(clientId, takenOver);
        }
    }
}
