package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Gets or Sets logArchiveType
 */
public enum LogArchiveType {
    DB_FLUSHED("db_flushed"),

    SCRIBE("scribe");

    private String value;

    LogArchiveType(String value) {
        this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
        return String.valueOf(value);
    }

    @JsonCreator
    public static LogArchiveType fromValue(String text) {
        for (LogArchiveType b : LogArchiveType.values()) {
            if (String.valueOf(b.value).equals(text)) {
                return b;
            }
        }
        return null;
    }
}
