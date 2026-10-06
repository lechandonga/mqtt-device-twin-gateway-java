package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 持久会话（cleanStart=false 时跨连接保留）：
 * 订阅关系、飞行中（已投递未 PUBACK）消息与离线队列都挂在这里，而不是连接对象上，
 * 因此“断线重连后订阅不丢、未确认消息不丢”。
 */
public class Session {

    private final String clientId;
    /** 过滤器 -> 客户端要求的最大 QoS（订阅时生效取发布 QoS 与订阅 QoS 的较小值）。 */
    private final Map<String, Qos> subscriptions = new LinkedHashMap<>();
    /** 离线期间排队的 QoS1 消息（FIFO）。 */
    private final Deque<MqttMessage> queued = new ArrayDeque<>();
    /** packetId -> 飞行中消息；重连后按入队顺序重投。 */
    private final Map<Long, MqttMessage> inFlight = new LinkedHashMap<>();
    /** 已经确认过的 packetId，重复 PUBACK 直接忽略并计数。 */
    private final Map<Long, Integer> ackedPacketIds = new HashMap<>();

    private int duplicateAcks;

    public Session(String clientId) {
        this.clientId = clientId;
    }

    public String clientId() {
        return clientId;
    }

    public synchronized Map<String, Qos> subscriptions() {
        return new LinkedHashMap<>(subscriptions);
    }

    public synchronized void subscribe(String filter, Qos qos) {
        TopicMatcher.split(filter);
        subscriptions.put(filter, qos == null ? Qos.AT_MOST_ONCE : qos);
    }

    public synchronized void unsubscribe(String filter) {
        subscriptions.remove(filter);
    }

    public synchronized void enqueueOffline(MqttMessage message) {
        if (message.qos() == Qos.AT_LEAST_ONCE) {
            queued.add(message);
        }
    }

    public synchronized void moveInFlightBackToQueue() {
        // 断线：飞行中消息回到队首，顺序为“最早未确认消息 + 原离线队列”，保证不丢且不乱序
        List<MqttMessage> pending = new ArrayList<>(inFlight.values());
        List<MqttMessage> offline = new ArrayList<>(queued);
        queued.clear();
        inFlight.clear();
        queued.addAll(pending);
        queued.addAll(offline);
    }

    public synchronized MqttMessage pollPending() {
        return queued.pollFirst();
    }

    public synchronized int queuedCount() {
        return queued.size();
    }

    public synchronized void markInFlight(MqttMessage message) {
        inFlight.put(message.packetId(), message);
    }

    public synchronized boolean acknowledge(long packetId) {
        boolean first = inFlight.remove(packetId) != null;
        if (!first) {
            if (ackedPacketIds.containsKey(packetId)) {
                duplicateAcks++;
                ackedPacketIds.merge(packetId, 1, Integer::sum);
            }
            return false;
        }
        ackedPacketIds.put(packetId, 1);
        return true;
    }

    public synchronized List<MqttMessage> redeliveryOrder() {
        return new ArrayList<>(inFlight.values());
    }

    public synchronized int duplicateAckCount() {
        return duplicateAcks;
    }

    public synchronized boolean isAcked(long packetId) {
        return ackedPacketIds.containsKey(packetId);
    }
}
