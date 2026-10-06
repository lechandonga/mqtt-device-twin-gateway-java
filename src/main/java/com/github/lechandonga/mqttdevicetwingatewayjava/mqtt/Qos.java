package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * 投递质量等级。
 * <ul>
 *   <li>{@link #AT_MOST_ONCE} 至多一次：发后即忘，不重试，可能丢失但绝不重复；</li>
 *   <li>{@link #AT_LEAST_ONCE} 至少一次：未收到 PUBACK 前保留在会话中并重投，可能重复但不丢失。</li>
 * </ul>
 */
public enum Qos {
    AT_MOST_ONCE(0),
    AT_LEAST_ONCE(1);

    private final int level;

    Qos(int level) {
        this.level = level;
    }

    public int level() {
        return level;
    }

    public static Qos of(int level) {
        return switch (level) {
            case 0 -> AT_MOST_ONCE;
            case 1 -> AT_LEAST_ONCE;
            default -> throw new IllegalArgumentException("unsupported QoS level: " + level);
        };
    }
}
