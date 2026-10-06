package com.github.lechandonga.mqttdevicetwingatewayjava.command;

/** 指令在队列/状态机中的可变状态。 */
public class CommandState {

    private final Command command;
    private CommandStatus status;
    private long sentAtMillis = -1;
    private long ackedAtMillis = -1;
    private int deliveryAttempts;
    private String failureReason;

    public CommandState(Command command) {
        this.command = command;
        this.status = CommandStatus.QUEUED;
    }

    public Command command() {
        return command;
    }

    public synchronized CommandStatus status() {
        return status;
    }

    public synchronized void markSent(long now) {
        this.status = CommandStatus.SENT;
        this.sentAtMillis = now;
        this.deliveryAttempts++;
    }

    public synchronized void markAcked(long now) {
        if (status == CommandStatus.SENT) {
            this.status = CommandStatus.ACKED;
            this.ackedAtMillis = now;
        }
    }

    public synchronized void markSucceeded() {
        this.status = CommandStatus.SUCCEEDED;
    }

    public synchronized void markFailed(CommandStatus failure, String reason) {
        this.status = failure;
        this.failureReason = reason;
    }

    public synchronized long sentAtMillis() {
        return sentAtMillis;
    }

    public synchronized int deliveryAttempts() {
        return deliveryAttempts;
    }

    public synchronized String failureReason() {
        return failureReason;
    }

    public synchronized boolean terminal() {
        return status == CommandStatus.SUCCEEDED
                || status == CommandStatus.FAILED_TIMEOUT
                || status == CommandStatus.FAILED_UNSUPPORTED
                || status == CommandStatus.FAILED_DEVICE_REJECTED;
    }
}
