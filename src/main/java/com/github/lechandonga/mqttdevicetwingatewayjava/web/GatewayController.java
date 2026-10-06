package com.github.lechandonga.mqttdevicetwingatewayjava.web;

import com.github.lechandonga.mqttdevicetwingatewayjava.command.Command;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandService;
import com.github.lechandonga.mqttdevicetwingatewayjava.command.CommandState;
import com.github.lechandonga.mqttdevicetwingatewayjava.gateway.DeviceGateway;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttClient;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.MqttMessage;
import com.github.lechandonga.mqttdevicetwingatewayjava.mqtt.Qos;
import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaReportResult;
import com.github.lechandonga.mqttdevicetwingatewayjava.ota.OtaService;
import com.github.lechandonga.mqttdevicetwingatewayjava.shadow.DeviceShadow;
import com.github.lechandonga.mqttdevicetwingatewayjava.shadow.ShadowService;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryPoint;
import com.github.lechandonga.mqttdevicetwingatewayjava.telemetry.TelemetryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地演示/手工验证接口（纯内存，无外部依赖）。
 * 设备消息收集在 {@code /api/devices/{id}/inbox}，模拟“设备侧收到什么”。
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final DeviceGateway gateway;
    private final ShadowService shadows;
    private final CommandService commands;
    private final TelemetryService telemetry;
    private final OtaService ota;

    private final Map<String, List<String>> inboxes = new ConcurrentHashMap<>();

    public GatewayController(DeviceGateway gateway, ShadowService shadows,
                             CommandService commands, TelemetryService telemetry, OtaService ota) {
        this.gateway = gateway;
        this.shadows = shadows;
        this.commands = commands;
        this.telemetry = telemetry;
        this.ota = ota;
    }

    private MqttClient simulatedDevice(String deviceId) {
        return new MqttClient() {
            @Override
            public String clientId() {
                return deviceId;
            }

            @Override
            public void onMessage(MqttMessage message) {
                inboxes.computeIfAbsent(deviceId, k -> new ArrayList<>())
                        .add(message.payloadAsString());
            }
        };
    }

    @PostMapping("/devices/{id}/connect")
    public Map<String, Object> connect(@PathVariable String id,
                                       @RequestParam(defaultValue = "false") boolean cleanStart,
                                       @RequestParam(required = false) String willTopic,
                                       @RequestParam(required = false) String willPayload) {
        MqttMessage will = willTopic == null ? null
                : new MqttMessage(gateway.broker().nextPacketId(), willTopic,
                willPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8), Qos.AT_LEAST_ONCE);
        gateway.connect(id, cleanStart, simulatedDevice(id), will);
        return Map.of("deviceId", id, "cleanStart", cleanStart, "connected", true);
    }

    @PostMapping("/devices/{id}/disconnect")
    public Map<String, Object> disconnect(@PathVariable String id,
                                          @RequestParam(defaultValue = "false") boolean clean) {
        gateway.disconnect(id, clean);
        return Map.of("deviceId", id, "connected", false, "clean", clean);
    }

    @PostMapping("/devices/{id}/subscribe")
    public Map<String, Object> subscribe(@PathVariable String id, @RequestParam String topicFilter,
                                         @RequestParam(defaultValue = "1") int qos) {
        gateway.subscribe(id, topicFilter, Qos.of(qos));
        return Map.of("deviceId", id, "topicFilter", topicFilter, "qos", qos);
    }

    @PostMapping("/publish")
    public Map<String, Object> publish(@RequestBody Map<String, String> request) {
        MqttMessage message = new MqttMessage(
                gateway.broker().nextPacketId(),
                request.get("topic"),
                request.getOrDefault("payload", "").getBytes(java.nio.charset.StandardCharsets.UTF_8),
                Qos.of(Integer.parseInt(request.getOrDefault("qos", "1"))));
        gateway.publish(message);
        return Map.of("packetId", message.packetId(), "topic", message.topic());
    }

    @PostMapping("/devices/{id}/ack/{packetId}")
    public Map<String, Object> ack(@PathVariable String id, @PathVariable long packetId) {
        return Map.of("firstAck", gateway.acknowledge(id, packetId));
    }

    @GetMapping("/devices/{id}/inbox")
    public List<String> inbox(@PathVariable String id) {
        return inboxes.getOrDefault(id, List.of());
    }

    @PostMapping("/devices/{id}/shadow/desired")
    public DeviceShadow.UpdateResult updateDesired(@PathVariable String id,
                                                   @RequestBody Map<String, Object> body) {
        long expected = ((Number) body.getOrDefault("expectedVersion", 0)).longValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> state = (Map<String, Object>) body.getOrDefault("state", Map.of());
        return shadows.updateDesired(id, state, expected);
    }

    @PostMapping("/devices/{id}/shadow/reported")
    public DeviceShadow.UpdateResult updateReported(@PathVariable String id,
                                                    @RequestBody Map<String, Object> body) {
        long version = ((Number) body.getOrDefault("version", 0)).longValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> state = (Map<String, Object>) body.getOrDefault("state", Map.of());
        return shadows.updateReported(id, state, version);
    }

    @GetMapping("/devices/{id}/shadow")
    public Map<String, Object> shadow(@PathVariable String id) {
        DeviceShadow shadow = shadows.get(id);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("desiredVersion", shadow.desiredVersion());
        view.put("reportedVersion", shadow.reportedVersion());
        view.put("desired", shadow.desired());
        view.put("reported", shadow.reported());
        view.put("delta", shadow.delta());
        return view;
    }

    @PostMapping("/devices/{id}/commands")
    public Map<String, Object> sendCommand(@PathVariable String id,
                                           @RequestBody Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        Command command = new Command(
                (String) body.get("commandId"),
                id,
                (String) body.getOrDefault("name", "noop"),
                (Map<String, Object>) body.getOrDefault("payload", Map.of()),
                System.currentTimeMillis());
        CommandState state = commands.send(command);
        return Map.of("commandId", command.commandId(), "status", state.status());
    }

    @PostMapping("/devices/{id}/commands/{commandId}/ack")
    public Map<String, Object> ackCommand(@PathVariable String id, @PathVariable String commandId) {
        return Map.of("status", commands.acknowledge(id, commandId));
    }

    @PostMapping("/devices/{id}/telemetry")
    public TelemetryService.TelemetryIngestResult ingest(@PathVariable String id,
                                                         @RequestBody Map<String, Object> body) {
        String messageId = (String) body.get("messageId");
        long timestamp = ((Number) body.getOrDefault("timestamp", System.currentTimeMillis())).longValue();
        @SuppressWarnings("unchecked")
        Map<String, Number> rawMetrics = (Map<String, Number>) body.getOrDefault("metrics", Map.of());
        Map<String, Double> metrics = new LinkedHashMap<>();
        rawMetrics.forEach((k, v) -> metrics.put(k, v.doubleValue()));
        return telemetry.ingest(new TelemetryPoint(id, messageId, timestamp, metrics));
    }

    @PostMapping("/devices/{id}/ota/start")
    public Map<String, Object> otaStart(@PathVariable String id, @RequestBody Map<String, String> body) {
        return Map.of("taskId", ota.start(body.get("taskId"), id,
                body.get("targetVersion"), body.get("knownGoodVersion")).taskId());
    }

    @PostMapping("/ota/{taskId}/progress")
    public OtaReportResult otaProgress(@PathVariable String taskId, @RequestBody Map<String, Number> body) {
        boolean installing = body.get("percent").intValue() >= 100;
        return ota.reportProgress(taskId, body.get("sequence").longValue(),
                body.get("percent").intValue(), installing);
    }

    @PostMapping("/ota/{taskId}/success")
    public OtaReportResult otaSuccess(@PathVariable String taskId, @RequestBody Map<String, Number> body) {
        return ota.reportSuccess(taskId, body.get("sequence").longValue());
    }

    @PostMapping("/ota/{taskId}/failure")
    public OtaReportResult otaFailure(@PathVariable String taskId, @RequestBody Map<String, String> body) {
        long sequence = Long.parseLong(body.getOrDefault("sequence", "0"));
        return ota.reportFailure(taskId, sequence, body.getOrDefault("reason", "unknown"));
    }

    @GetMapping("/devices/{id}/ota")
    public Map<String, Object> otaView(@PathVariable String id) {
        return Map.of("activeVersion", String.valueOf(ota.activeVersion(id)));
    }
}
