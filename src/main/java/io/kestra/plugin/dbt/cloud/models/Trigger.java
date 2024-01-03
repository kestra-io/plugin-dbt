package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.time.ZonedDateTime;
import java.util.List;
import jakarta.validation.Valid;

@Value
@Jacksonized
@SuperBuilder
public class Trigger {
    @JsonProperty("id")
    Integer id;

    @JsonProperty("cause")
    String cause;

    @JsonProperty("job_definition_id")
    Integer jobDefinitionId;

    @JsonProperty("git_branch")
    String gitBranch;

    @JsonProperty("git_sha")
    String gitSha;

    @JsonProperty("github_pull_request_id")
    Integer githubPullRequestId;

    @JsonProperty("schema_override")
    String schemaOverride;

    @JsonProperty("dbt_version_override")
    String dbtVersionOverride;

    @JsonProperty("threads_override")
    Integer threadsOverride;

    @JsonProperty("target_name_override")
    String targetNameOverride;

    @JsonProperty("generate_docs_override")
    Boolean generateDocsOverride;

    @JsonProperty("timeout_seconds_override")
    Integer timeoutSecondsOverride;

    @JsonProperty("steps_override")
    @Valid
    List<String> stepsOverride;

    @JsonProperty("created_at")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd' 'HH:mm:ss[.SSSSSS]XXX")
    ZonedDateTime createdAt;
}
