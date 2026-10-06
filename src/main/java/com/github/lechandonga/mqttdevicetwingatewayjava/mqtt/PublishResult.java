package com.github.lechandonga.mqttdevicetwingatewayjava.mqtt;

import java.util.List;

/**
 * publish 结果：delivered 为本次同步投递给在线订阅者的数量，queued 为进入离线队列的份数。
 */
public record PublishResult(int delivered, int queued, List<String> matchedSubscribers) {
}
