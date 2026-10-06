package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessCase;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessLogging;
import com.github.lechandonga.mqttdevicetwingatewayjava.support.BusinessModule;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author Vincent
 * @version 1.0
 */
@SpringBootTest
@ExtendWith(BusinessLogging.class)
@BusinessModule("Spring 上下文装配冒烟")
public class ApplicationTests {

    @Test
    @BusinessCase("应用上下文可启动，所有网关门面/服务 Bean 装配成功")
    public void contentLoad() {
    }
}
