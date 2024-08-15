package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.ZonedDateTime;
import java.util.List;

@Value
@Jacksonized
@SuperBuilder
public class Run {
    @JsonProperty("id")
    Long id;

    @JsonProperty("trigger_id")
    Long triggerId;

    @JsonProperty("account_id")
    Long accountId;

    @JsonProperty("project_id")
    Long projectId;

    @JsonProperty("job_id")
    Long jobId;

    @JsonProperty("job_definition_id")
    Long jobDefinitionId;

    @JsonProperty("status")
    JobStatus status;

    @JsonProperty("git_branch")
    String gitBranch;

    @JsonProperty("git_sha")
    String gitSha;

    @JsonProperty("status_message")
    String statusMessage;

    @JsonProperty("dbt_version")
    String dbtVersion;

    @JsonProperty("created_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime createdAt;

    @JsonProperty("updated_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime updatedAt;

    @JsonProperty("dequeued_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime dequeuedAt;

    @JsonProperty("started_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime startedAt;

    @JsonProperty("finished_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime finishedAt;

    @JsonProperty("last_checked_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime lastCheckedAt;

    @JsonProperty("last_heartbeat_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime lastHeartbeatAt;

    @JsonProperty("should_start_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime shouldStartAt;

    @JsonProperty("owner_thread_id")
    String ownerThreadId;

    @JsonProperty("executed_by_thread_id")
    String executedByThreadId;

    @JsonProperty("deferring_run_id")
    String deferringRunId;

    @JsonProperty("artifacts_saved")
    Boolean artifactsSaved;

    @JsonProperty("artifact_s3_path")
    String artifactS3Path;

    @JsonProperty("has_docs_generated")
    Boolean hasDocsGenerated;

    @JsonProperty("has_sources_generated")
    Boolean hasSourcesGenerated;

    @JsonProperty("notifications_sent")
    Boolean notificationsSent;

    @JsonProperty("scribe_enabled")
    Boolean scribeEnabled;

    @JsonProperty("trigger")
    Trigger trigger;

    @JsonProperty("job")
    Job job;

    @JsonProperty("environment")
    Environment environment;

    @JsonProperty("run_steps")
    List<Step> runSteps;

    @JsonProperty("duration")
    String duration;

    @JsonProperty("queued_duration")
    String queuedDuration;

    @JsonProperty("run_duration")
    String runDuration;

    @JsonProperty("duration_humanized")
    String durationHumanized;

    @JsonProperty("queued_duration_humanized")
    String queuedDurationHumanized;

    @JsonProperty("run_duration_humanized")
    String runDurationHumanized;

    @JsonProperty("finished_at_humanized")
    String finishedAtHumanized;

    @JsonProperty("status_humanized")
    JobStatusHumanizedEnum statusHumanized;

    @JsonProperty("created_at_humanized")
    String createdAtHumanized;
}
