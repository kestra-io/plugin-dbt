package io.kestra.plugin.dbt.cloud;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JobScheduleDateType {
    EVERY_DAY("every_day"),

    DAYS_OF_WEEK("days_of_week"),

    CUSTOM_CRON("custom_cron");

    private String value;

    JobScheduleDateType(String value) {
        this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
        return String.valueOf(value);
    }

    @JsonCreator
    public static JobScheduleDateType fromValue(String text) {
        for (JobScheduleDateType b : JobScheduleDateType.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }
}
