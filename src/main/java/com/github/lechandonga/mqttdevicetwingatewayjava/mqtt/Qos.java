package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * MQTT 风格投递语义。
 * <ul>
 *     <li>{@link #AT_MOST_ONCE}：至多一次，投递即忘，不重投、不确认。</li>
 *     <li>{@link #AT_LEAST_ONCE}：至少一次，未确认时重连后重投，可能重复，由消费端去重。</li>
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
