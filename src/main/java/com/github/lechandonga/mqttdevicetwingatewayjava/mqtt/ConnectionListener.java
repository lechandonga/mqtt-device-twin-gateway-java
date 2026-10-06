package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * 连接事件监听器：设备上线/掉线（含被接管）通知业务层。
 */
public interface ConnectionListener {

    default void onConnected(String clientId, boolean sessionPresent) {
    }

    /**
     * @param takenOver true 表示因同一 clientId 再次连接而被接管
     */
    default void onDisconnected(String clientId, boolean takenOver) {
    }
}
