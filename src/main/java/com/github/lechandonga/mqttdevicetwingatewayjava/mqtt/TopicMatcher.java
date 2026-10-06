package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * MQTT 主题过滤器匹配（{@code +} 单层通配、{@code #} 多层通配，仅允许出现在过滤器末层）。
 * 匹配结果稳定可解释：同一条消息若命中多个订阅，按“具体层数优先、通配靠后、字典序”排序，
 * 并以生效的最高 QoS 投递，因此投递顺序与选择的 QoS 在任何客户端上都可复现。
 */
public final class TopicMatcher {

    private TopicMatcher() {
    }

    public static boolean matches(String filter, String topic) {
        String[] f = split(filter);
        String[] t = split(topic);
        int i = 0;
        while (i < f.length) {
            if ("#".equals(f[i])) {
                return true;
            }
            if (i >= t.length) {
                return false;
            }
            if (!"+".equals(f[i]) && !f[i].equals(t[i])) {
                return false;
            }
            i++;
        }
        return i == t.length;
    }

    /** 通配越少、层级越具体的过滤器排越前；完全相同再按字典序，保证确定性。 */
    public static Comparator<String> specificity() {
        return Comparator.comparingInt((String f) -> (int) split(f).length).reversed()
                .thenComparingInt((String f) -> countWildcards(f))
                .thenComparing(Comparator.naturalOrder());
    }

    public static List<String> explain(String filter, String topic) {
        List<String> steps = new ArrayList<>();
        String[] f = split(filter);
        String[] t = split(topic);
        for (int i = 0; i < f.length; i++) {
            if ("#".equals(f[i])) {
                steps.add("层" + (i + 1) + " '#' 多层通配，剩余 " + Math.max(0, t.length - i) + " 层全部接受");
                break;
            }
            if (i >= t.length) {
                steps.add("层" + (i + 1) + " 过滤器 '" + f[i] + "' 无对应主题层 -> 不匹配");
                break;
            }
            if ("+".equals(f[i])) {
                steps.add("层" + (i + 1) + " '+' 匹配 '" + t[i] + "'");
            } else if (f[i].equals(t[i])) {
                steps.add("层" + (i + 1) + " 字面量 '" + f[i] + "' 相等");
            } else {
                steps.add("层" + (i + 1) + " '" + f[i] + "' != '" + t[i] + "' -> 不匹配");
            }
        }
        if (f.length < t.length && steps.stream().noneMatch(s -> s.contains("#"))) {
            steps.add("主题多出 " + (t.length - f.length) + " 层 -> 不匹配");
        }
        return steps;
    }

    private static int countWildcards(String filter) {
        int c = 0;
        for (String part : split(filter)) {
            if ("+".equals(part) || "#".equals(part)) {
                c++;
            }
        }
        return c;
    }

    static String[] split(String topic) {
        if (topic.isEmpty() || topic.startsWith("/") || topic.endsWith("/") || topic.contains("//")) {
            throw new IllegalArgumentException("非法主题: '" + topic + "'");
        }
        String[] parts = topic.split("/");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (part.contains("#") && !"#".equals(part)) {
                throw new IllegalArgumentException("'#' 只能独占一层: " + topic);
            }
            if (part.contains("+") && !"+".equals(part)) {
                throw new IllegalArgumentException("'+' 只能独占一层: " + topic);
            }
            if ("#".equals(part) && i != parts.length - 1) {
                throw new IllegalArgumentException("'#' 只能出现在最后一层: " + topic);
            }
        }
        return parts;
    }
}
