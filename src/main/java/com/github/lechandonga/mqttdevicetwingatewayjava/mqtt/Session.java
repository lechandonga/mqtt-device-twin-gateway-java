package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 客户端会话。cleanSession=false 时会话在断线后保留：订阅关系、未确认消息、离线消息均不丢。
 */
public final class Session {
    private final String clientId;
    private final boolean cleanSession;
    private final Set<String> subscriptions = new LinkedHashSet<>();
    private final Map<Integer, InFlightMessage> inFlight = new LinkedHashMap<>();
    private final Map<Integer, InFlightMessage> queued = new LinkedHashMap<>();
    private final Set<String> consumedMessageIds = new LinkedHashSet<>();
    private int nextPacketId = 1;
    private volatile boolean online;

    public Session(String clientId, boolean cleanSession) {
        this.clientId = clientId;
        this.cleanSession = cleanSession;
        this.online = true;
    }

    public String clientId() {
        return clientId;
    }

    public boolean cleanSession() {
        return cleanSession;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    public Set<String> subscriptions() {
        return subscriptions;
    }

    public Map<Integer, InFlightMessage> inFlight() {
        return inFlight;
    }

    public Map<Integer, InFlightMessage> queued() {
        return queued;
    }

    public Set<String> consumedMessageIds() {
        return consumedMessageIds;
    }

    public int allocatePacketId() {
        return nextPacketId++;
    }
}
