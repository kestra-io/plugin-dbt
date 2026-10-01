package io.kestra.plugin.dbt.cloud.models;

import java.time.ZonedDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

// Allow-list of run fields safe to expose: the raw Run carries the environment, which can hold credentials.
@Value
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class RunDetails {
    @Schema(title = "Run status, e.g. `Success`, `Error`, `Cancelled`")
    String status;

    @Schema(title = "Status message dbt Cloud reported for the run, usually set on failure")
    String statusMessage;

    @Schema(title = "dbt Cloud job ID")
    Long jobId;

    @Schema(title = "dbt Cloud job name")
    String jobName;

    @Schema(title = "dbt Cloud environment name")
    String environmentName;

    @Schema(title = "Git branch the run checked out")
    String gitBranch;

    @Schema(title = "dbt version used by the run")
    String dbtVersion;

    @Schema(title = "Total duration, as formatted by dbt Cloud")
    String duration;

    @Schema(title = "Time spent queued, as formatted by dbt Cloud")
    String queuedDuration;

    @Schema(title = "Time spent running, as formatted by dbt Cloud")
    String runDuration;

    @Schema(title = "When the run started")
    ZonedDateTime startedAt;

    @Schema(title = "When the run finished")
    ZonedDateTime finishedAt;

    public static RunDetails from(Run run) {
        return RunDetails.builder()
            .status(run.getStatusHumanized() == null ? null : run.getStatusHumanized().toString())
            .statusMessage(run.getStatusMessage())
            .jobId(run.getJobId())
            .jobName(run.getJob() == null ? null : run.getJob().getName())
            .environmentName(run.getEnvironment() == null ? null : run.getEnvironment().getName())
            .gitBranch(run.getGitBranch())
            .dbtVersion(run.getDbtVersion())
            .duration(run.getDurationHumanized())
            .queuedDuration(run.getQueuedDurationHumanized())
            .runDuration(run.getRunDurationHumanized())
            .startedAt(run.getStartedAt())
            .finishedAt(run.getFinishedAt())
            .build();
    }
}
