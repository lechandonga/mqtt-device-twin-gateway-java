package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

/**
 * 阈值告警规则。边沿触发：
 * 指标从正常跨入超限只产生一次 FIRED；条件持续期间重复上报不重复告警；
 * 回到阈值内产生一次 RECOVERED，之后才能再次 FIRED。
 */
public class ThresholdRule {

    private final String deviceId;
    private final String metric;
    private final double upperLimit;
    private final double hysteresis;
    private boolean firing;

    public ThresholdRule(String deviceId, String metric, double upperLimit) {
        this(deviceId, metric, upperLimit, 0.0);
    }

    /**
     * @param hysteresis 恢复迟滞：恢复需低于 upperLimit - hysteresis，避免阈值附近抖动反复告警
     */
    public ThresholdRule(String deviceId, String metric, double upperLimit, double hysteresis) {
        this.deviceId = deviceId;
        this.metric = metric;
        this.upperLimit = upperLimit;
        this.hysteresis = hysteresis;
    }

    public String metric() {
        return metric;
    }

    public String deviceId() {
        return deviceId;
    }

    public synchronized boolean firing() {
        return firing;
    }

    /**
     * 根据新值推进状态机。
     * @return 边沿事件；无状态变化返回 null
     */
    public synchronized AlertEvent evaluate(double value, long timestampMillis) {
        if (!firing && value > upperLimit) {
            firing = true;
            return new AlertEvent(deviceId, metric, "FIRED", value, upperLimit, timestampMillis);
        }
        if (firing && value <= upperLimit - hysteresis) {
            firing = false;
            return new AlertEvent(deviceId, metric, "RECOVERED", value, upperLimit, timestampMillis);
        }
        return null;
    }
}
