package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.kestra.core.models.flows.State;

public enum JobStatus {
    NUMBER_1(1), // Queued

    NUMBER_2(2), // Starting

    NUMBER_3(3), // Running

    NUMBER_10(10), // Success

    NUMBER_20(20), // Error

    NUMBER_30(30); // Cancelled

    private final Integer value;

    JobStatus(Integer value) {
        this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
        return String.valueOf(value);
    }

    @JsonCreator
    public static JobStatus fromValue(String text) {
        for (JobStatus b : JobStatus.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }

    public State.Type state() {
        switch (this.value) {
            case 1:
            case 2:
                return State.Type.CREATED;
            case 3:
                return State.Type.RUNNING;
            case 10:
                return State.Type.SUCCESS;
            case 20:
                return State.Type.FAILED;
            case 30:
                return State.Type.KILLED;
        }

        throw new IllegalStateException("No suitable status for '" + this.value + "'");
    }
}
