package io.kestra.plugin.dbt.cloud;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JobScheduleTimeType {
    EVERY_HOUR("every_hour"),

    AT_EXACT_HOURS("at_exact_hours");

    private String value;

    JobScheduleTimeType(String value) {
        this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
        return String.valueOf(value);
    }

    @JsonCreator
    public static JobScheduleTimeType fromValue(String text) {
        for (JobScheduleTimeType b : JobScheduleTimeType.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }
}
