package com.github.lechandonga.mqttdevicetwingatewayjava.telemetry;

/**
 * 阈值规则：指标 metric 在 value 与 operator 比较成立时越限。
 */
public record ThresholdRule(String name, String metric, Operator operator, double threshold) {

    public enum Operator {
        GT, LT, GE, LE
    }

    public boolean breached(double value) {
        return switch (operator) {
            case GT -> value > threshold;
            case LT -> value < threshold;
            case GE -> value >= threshold;
            case LE -> value <= threshold;
        };
    }
}
