package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 遥测处理管线：归一化 -> 去重/时间戳校验 -> 最新状态 -> 阈值边沿判定。
 * 全流程按设备串行，保证并发上报下结论确定。
 */
@Service
public class TelemetryService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryService.class);

    private final Map<String, DeviceTelemetryState> devices = new ConcurrentHashMap<>();

    private static class DeviceTelemetryState {
        private final Set<String> seenMessageIds = ConcurrentHashMap.newKeySet();
        private long latestTimestamp = Long.MIN_VALUE;
        private final Map<String, Double> latestMetrics = new LinkedHashMap<>();
        private final Map<String, ThresholdRule> rules = new LinkedHashMap<>();
        private final List<AlertEvent> alertHistory = new ArrayList<>();
    }

    public void addRule(ThresholdRule rule) {
        DeviceTelemetryState state = devices.computeIfAbsent(rule.deviceId(),
                k -> new DeviceTelemetryState());
        synchronized (state) {
            state.rules.put(rule.metric(), rule);
        }
    }

    /**
     * 处理一条遥测。
     * @return 去重/过期原因及本次产生的告警边沿事件（持续超限不产生事件）
     */
    public TelemetryIngestResult ingest(TelemetryPoint raw) {
        TelemetryPoint point = normalize(raw);
        DeviceTelemetryState state = devices.computeIfAbsent(point.deviceId(),
                k -> new DeviceTelemetryState());
        synchronized (state) {
            if (state.seenMessageIds.contains(point.messageId())) {
                log.info("[telemetry] device={} 重复消息 {} 忽略", point.deviceId(), point.messageId());
                return new TelemetryIngestResult(IngestResult.DUPLICATE_MESSAGE, List.of());
            }
            if (point.timestampMillis() < state.latestTimestamp) {
                log.info("[telemetry] device={} 时间戳回退 {} < {} 忽略",
                        point.deviceId(), point.timestampMillis(), state.latestTimestamp);
                return new TelemetryIngestResult(IngestResult.STALE_TIMESTAMP, List.of());
            }
            state.seenMessageIds.add(point.messageId());
            state.latestTimestamp = point.timestampMillis();
            state.latestMetrics.putAll(point.metrics());

            List<AlertEvent> events = new ArrayList<>();
            for (Map.Entry<String, Double> entry : point.metrics().entrySet()) {
                ThresholdRule rule = state.rules.get(entry.getKey());
                if (rule == null) {
                    continue;
                }
                AlertEvent event = rule.evaluate(entry.getValue(), point.timestampMillis());
                if (event != null) {
                    events.add(event);
                    state.alertHistory.add(event);
                    log.info("[telemetry] device={} 指标 {} 边沿事件 {} value={}",
                            point.deviceId(), entry.getKey(), event.type(), entry.getValue());
                }
            }
            return new TelemetryIngestResult(IngestResult.ACCEPTED, events);
        }
    }

    /** 归一化：指标名 trim 转小写、去空键；非法（NaN/Infinity）值剔除。 */
    private TelemetryPoint normalize(TelemetryPoint point) {
        Map<String, Double> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : point.metrics().entrySet()) {
            String name = entry.getKey() == null ? null : entry.getKey().trim().toLowerCase();
            if (name == null || name.isBlank() || entry.getValue() == null
                    || Double.isNaN(entry.getValue()) || Double.isInfinite(entry.getValue())) {
                continue;
            }
            normalized.merge(name, entry.getValue(), Math::max);
        }
        return new TelemetryPoint(point.deviceId(), point.messageId(),
                point.timestampMillis(), normalized);
    }

    public Double latestMetric(String deviceId, String metric) {
        DeviceTelemetryState state = devices.get(deviceId);
        if (state == null) {
            return null;
        }
        synchronized (state) {
            return state.latestMetrics.get(metric.trim().toLowerCase());
        }
    }

    public long latestTimestamp(String deviceId) {
        DeviceTelemetryState state = devices.get(deviceId);
        if (state == null) {
            return Long.MIN_VALUE;
        }
        synchronized (state) {
            return state.latestTimestamp;
        }
    }

    public List<AlertEvent> alertHistory(String deviceId) {
        DeviceTelemetryState state = devices.get(deviceId);
        synchronized (state) {
            return List.copyOf(state.alertHistory);
        }
    }

    public boolean isFiring(String deviceId, String metric) {
        DeviceTelemetryState state = devices.get(deviceId);
        if (state == null) {
            return false;
        }
        synchronized (state) {
            ThresholdRule rule = state.rules.get(metric.trim().toLowerCase());
            return rule != null && rule.firing();
        }
    }

    public record TelemetryIngestResult(IngestResult result, List<AlertEvent> alertEvents) {
        public boolean accepted() {
            return result == IngestResult.ACCEPTED;
        }
    }
}
