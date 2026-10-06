package com.github.lechandonga.mqttdevicetwingatewayjava.shadow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 设备影子注册表：所有设备共享一个内存存储，按 deviceId 加锁串行化写入。 */
@Service
public class ShadowService {

    private static final Logger log = LoggerFactory.getLogger(ShadowService.class);

    private final Map<String, DeviceShadow> shadows = new ConcurrentHashMap<>();

    public DeviceShadow get(String deviceId) {
        return shadows.computeIfAbsent(deviceId, DeviceShadow::new);
    }

    public DeviceShadow.UpdateResult updateDesired(String deviceId, Map<String, Object> patch,
                                                   long expectedVersion) {
        DeviceShadow.UpdateResult result = get(deviceId).updateDesired(patch, expectedVersion);
        log.info("[shadow] device={} desired 更新 expectedVersion={} -> {} (version={})",
                deviceId, expectedVersion, result.outcome(), result.currentVersion());
        return result;
    }

    public DeviceShadow.UpdateResult updateReported(String deviceId, Map<String, Object> patch,
                                                    long version) {
        DeviceShadow.UpdateResult result = get(deviceId).updateReported(patch, version);
        log.info("[shadow] device={} reported 更新 version={} -> {} (currentVersion={})",
                deviceId, version, result.outcome(), result.currentVersion());
        return result;
    }
}
