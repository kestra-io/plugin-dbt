package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.ZonedDateTime;

@Value
@Jacksonized
@SuperBuilder
public class Step {
    @JsonProperty("id")
    Integer id;

    @JsonProperty("run_id")
    Integer runId;

    @JsonProperty("account_id")
    Integer accountId;

    @JsonProperty("name")
    String name;

    @JsonProperty("logs")
    String logs;

    @JsonProperty("debug_logs")
    String debugLogs;

    @JsonProperty("log_location")
    LogLocation logLocation;

    @JsonProperty("log_path")
    String logPath;

    @JsonProperty("debug_log_path")
    String debugLogPath;

    @JsonProperty("log_archive_type")
    LogArchiveType logArchiveType;

    @JsonProperty("truncated_debug_logs")
    String truncatedDebugLogs;

    @JsonProperty("created_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime createdAt;

    @JsonProperty("updated_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime updatedAt;

    @JsonProperty("started_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime startedAt;

    @JsonProperty("finished_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime finishedAt;

    @JsonProperty("status_color")
    String statusColor;

    @JsonProperty("status")
    JobStatus status;

    @JsonProperty("duration")
    String duration;

    @JsonProperty("duration_humanized")
    String durationHumanized;
}
