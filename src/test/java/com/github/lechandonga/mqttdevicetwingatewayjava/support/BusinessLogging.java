package com.github.lechandonga.mqttdevicetwingatewayjava.support;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;

/**
 * 测试日志扩展：运行时清晰打印“业务模块 -> 用例 -> 结论”，
 * 让 mvn test 的日志能直接看出覆盖了哪些业务及每条用例的通过情况。
 */
public class BusinessLogging implements BeforeAllCallback, AfterAllCallback,
        BeforeTestExecutionCallback, AfterTestExecutionCallback {

    private static final String BANNER =
            "==============================================================";

    @Override
    public void beforeAll(ExtensionContext context) {
        System.out.println();
        System.out.println(BANNER);
        System.out.println("[业务模块] " + businessName(context));
        System.out.println(BANNER);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        System.out.println("[模块结束] " + businessName(context));
        System.out.println();
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        System.out.println("---- [用例] " + displayName(context) + " ----");
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        boolean failed = context.getExecutionException().isPresent();
        System.out.println("==== [结论] " + displayName(context) + " -> "
                + (failed ? "失败 ❌" : "通过 ✅"));
    }

    private String businessName(ExtensionContext context) {
        Class<?> testClass = context.getRequiredTestClass();
        return testClass.isAnnotationPresent(BusinessModule.class)
                ? testClass.getAnnotation(BusinessModule.class).value()
                : testClass.getSimpleName();
    }

    private String displayName(ExtensionContext context) {
        Method method = context.getRequiredTestMethod();
        BusinessCase annotation = method.getAnnotation(BusinessCase.class);
        return annotation != null ? annotation.value() : method.getName();
    }
}
