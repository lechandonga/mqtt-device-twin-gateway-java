package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 遥测处理服务。
 * <ol>
 *     <li>归一化：原始指标按单位表转换为 SI 单位（如 °F->°C, km/h->m/s）。</li>
 *     <li>去重：同一 (deviceId, sampleId) 只处理一次。</li>
 *     <li>时序：时间戳回退的样本拒绝（STALE_TIMESTAMP），不污染最新状态。</li>
 *     <li>告警：阈值规则按状态边沿触发，条件持续期间不重复告警；恢复后自动重新武装。</li>
 * </ol>
 */
@Component
public class TelemetryService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryService.class);

    private static final Map<String, Map<String, UnitConverter>> UNIT_TABLE = Map.of(
            "temperature", Map.of(
                    "C", v -> v,
                    "F", v -> (v - 32.0) * 5.0 / 9.0),
            "speed", Map.of(
                    "m/s", v -> v,
                    "km/h", v -> v / 3.6)
    );

    @FunctionalInterface
    private interface UnitConverter {
        double convert(double raw);
    }

    private static final class DeviceState {
        private long lastTimestamp = Long.MIN_VALUE;
        private final Map<String, Double> latest = new LinkedHashMap<>();
        private final Set<String> consumedIds = new HashSet<>();
        private final Set<String> activeAlerts = new HashSet<>();
    }

    private final Map<String, DeviceState> devices = new ConcurrentHashMap<>();
    private final Map<String, List<ThresholdRule>> ruleStore = new ConcurrentHashMap<>();

    public void addRule(String deviceId, ThresholdRule rule) {
        ruleStore.computeIfAbsent(deviceId, k -> new ArrayList<>()).add(rule);
    }

    public record IngestOutcome(IngestResult result, List<AlertEvent> events) {
    }

    /** 摄入已归一化（SI 单位）的遥测样本。 */
    public IngestOutcome ingest(String deviceId, TelemetrySample sample) {
        return doIngest(deviceId, sample.id(), sample.timestamp(), sample.metrics());
    }

    /** 带单位的摄入：先归一化，再去重、判时序、更新状态、评估阈值。 */
    public IngestOutcome ingestWithUnits(String deviceId, String sampleId, long timestamp,
                                         Map<String, RawMetric> raw) {
        Map<String, Double> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, RawMetric> e : raw.entrySet()) {
            RawMetric rm = e.getValue();
            Map<String, UnitConverter> converters = UNIT_TABLE.get(e.getKey());
            double value;
            if (converters != null) {
                UnitConverter converter = converters.get(rm.unit());
                if (converter == null) {
                    throw new IllegalArgumentException("unknown unit '" + rm.unit() + "' for metric " + e.getKey());
                }
                value = converter.convert(rm.value());
            } else {
                value = rm.value();
            }
            normalized.put(e.getKey(), round(value));
        }
        return doIngest(deviceId, sampleId, timestamp, normalized);
    }

    private synchronized IngestOutcome doIngest(String deviceId, String sampleId, long timestamp,
                                                Map<String, Double> metrics) {
        DeviceState state = devices.computeIfAbsent(deviceId,
                k -> new DeviceState());
        if (!state.consumedIds.add(sampleId)) {
            log.warn("[TELEMETRY] device='{}' 重复样本 {} 被忽略", deviceId, sampleId);
            return new IngestOutcome(IngestResult.DUPLICATE, List.of());
        }
        if (timestamp < state.lastTimestamp) {
            log.warn("[TELEMETRY] device='{}' 时间戳回退 {} < {}，样本 {} 被拒绝",
                    deviceId, timestamp, state.lastTimestamp, sampleId);
            return new IngestOutcome(IngestResult.STALE_TIMESTAMP, List.of());
        }
        state.lastTimestamp = timestamp;
        state.latest.putAll(metrics);
        log.info("[TELEMETRY] device='{}' 样本 {} @{} 已更新最新状态: {}", deviceId, sampleId, timestamp, metrics);
        return new IngestOutcome(IngestResult.ACCEPTED, evaluateRules(deviceId, metrics, timestamp, state));
    }

    private List<AlertEvent> evaluateRules(String deviceId, Map<String, Double> metrics,
                                           long timestamp, DeviceState state) {
        List<AlertEvent> events = new ArrayList<>();
        for (ThresholdRule rule : ruleStore.getOrDefault(deviceId, List.of())) {
            Double value = metrics.get(rule.metric());
            if (value == null) {
                continue;
            }
            boolean breached = rule.breached(value);
            String activeKey = rule.name();
            if (breached && state.activeAlerts.add(activeKey)) {
                events.add(new AlertEvent(deviceId, rule.name(), rule.metric(), value,
                        rule.threshold(), true, timestamp));
                log.warn("[TELEMETRY] 告警触发 device='{}' rule={} value={} threshold={}",
                        deviceId, rule.name(), value, rule.threshold());
            } else if (!breached && state.activeAlerts.remove(activeKey)) {
                events.add(new AlertEvent(deviceId, rule.name(), rule.metric(), value,
                        rule.threshold(), false, timestamp));
                log.info("[TELEMETRY] 告警恢复 device='{}' rule={} value={}", deviceId, rule.name(), value);
            }
        }
        return events;
    }

    private static double round(double v) {
        return Math.round(v * 1000.0) / 1000.0;
    }

    public Map<String, Double> latestMetrics(String deviceId) {
        DeviceState state = devices.get(deviceId);
        return state == null ? Map.of() : new HashMap<>(state.latest);
    }

    public Long lastTimestamp(String deviceId) {
        DeviceState state = devices.get(deviceId);
        return state == null ? null : state.lastTimestamp;
    }

    public Set<String> activeAlerts(String deviceId) {
        DeviceState state = devices.get(deviceId);
        return state == null ? Set.of() : Set.copyOf(state.activeAlerts);
    }

    /** 原始指标值 + 单位。 */
    public record RawMetric(double value, String unit) {
    }
}
