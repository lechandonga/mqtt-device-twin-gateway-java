package com.github.lechandonga.mqttdevicetwingatewayjava.tests;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 让单测运行日志清晰展示：每个测试类覆盖的业务域、每个用例的名称与执行结果。
 */
public class CoverageLoggerExtension implements BeforeAllCallback, BeforeTestExecutionCallback,
        AfterTestExecutionCallback, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger("BUSINESS-COVERAGE");

    @Override
    public void beforeAll(ExtensionContext context) {
        log.info("==================== 业务测试开始: {} ====================",
                context.getRequiredTestClass().getSimpleName());
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        log.info(">>> [用例开始] {}", context.getDisplayName());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        boolean failed = context.getExecutionException().isPresent();
        log.info("<<< [用例{}] {}", failed ? "失败" : "通过", context.getDisplayName());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        log.info("==================== 业务测试结束: {} ====================",
                context.getRequiredTestClass().getSimpleName());
    }
}
