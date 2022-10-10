package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Gets or Sets logLocation
 */
public enum LogLocation {
    LEGACY("legacy"),

    DB("db"),

    S3("s3"),

    EMPTY("empty");

    private String value;

    LogLocation(String value) {
        this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
        return String.valueOf(value);
    }

    @JsonCreator
    public static LogLocation fromValue(String text) {
        for (LogLocation b : LogLocation.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }
}
