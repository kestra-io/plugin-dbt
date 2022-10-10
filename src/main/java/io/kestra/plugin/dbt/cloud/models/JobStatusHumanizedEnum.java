package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JobStatusHumanizedEnum {
    QUEUED("Queued"),

    STARTING("Starting"),

    RUNNING("Running"),

    SUCCESS("Success"),

    ERROR("Error"),

    CANCELLED("Cancelled");

    private String value;

    JobStatusHumanizedEnum(String value) {
        this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
        return String.valueOf(value);
    }

    @JsonCreator
    public static JobStatusHumanizedEnum fromValue(String text) {
        for (JobStatusHumanizedEnum b : JobStatusHumanizedEnum.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }
}
