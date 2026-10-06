package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

/**
 * MQTT 主题通配符匹配。
 * <ul>
 *     <li>'+' 匹配单层；'#' 只允许出现在末尾，匹配零到多层。</li>
 *     <li>以 '/' 分层，匹配规则对层级位置敏感（"a/" 与 "a" 不同）。</li>
 * </ul>
 */
public final class TopicMatcher {

    private TopicMatcher() {
    }

    public static boolean matches(String filter, String topic) {
        if (filter.equals(topic)) {
            return true;
        }
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        int ti = 0;
        for (int fi = 0; fi < f.length; fi++) {
            String level = f[fi];
            if ("#".equals(level)) {
                return fi == f.length - 1;
            }
            if (ti >= t.length) {
                return false;
            }
            if ("+".equals(level)) {
                ti++;
            } else if (level.equals(t[ti])) {
                ti++;
            } else {
                return false;
            }
        }
        return ti == t.length;
    }
}
