package com.github.lechandonga.mqttdevicetwingatewayjava.command;

/** 可替换的时钟，测试中超时判定无需真实等待。 */
public interface Clock {
    long nowMillis();

    Clock SYSTEM = System::currentTimeMillis;
}
