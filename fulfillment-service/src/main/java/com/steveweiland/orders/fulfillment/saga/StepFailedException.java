package com.steveweiland.orders.fulfillment.saga;

public final class StepFailedException extends Exception {
    private final String stepName;

    public StepFailedException(String stepName, String reason) {
        super(reason);
        this.stepName = stepName;
    }

    public String stepName() {
        return stepName;
    }
}
