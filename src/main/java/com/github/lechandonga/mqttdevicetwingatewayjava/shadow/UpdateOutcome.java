package com.github.lechandonga.mqttdevicetwingatewayjava.shadow;

/** 影子更新结论；冲突原因可区分，便于上层给出明确失败结论。 */
public enum UpdateOutcome {
    ACCEPTED,
    REJECTED_STALE_VERSION,
    REJECTED_DUPLICATE_VERSION
}
