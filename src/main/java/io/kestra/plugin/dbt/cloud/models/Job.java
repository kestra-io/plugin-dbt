package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;

@Value
@Jacksonized
@SuperBuilder
public class Job {
    @JsonProperty("id")
    Integer id;

    @JsonProperty("account_id")
    Integer accountId;

    @JsonProperty("project_id")
    Integer projectId;

    @JsonProperty("environment_id")
    Integer environmentId;

    @JsonProperty("name")
    String name;

    @JsonProperty("dbt_version")
    String dbtVersion;

    @JsonProperty("triggers")
    JobTriggers triggers;

    @JsonProperty("execute_steps")
    @Valid
    List<String> executeSteps = new ArrayList<>();

    @JsonProperty("settings")
    JobSettings settings;

    @JsonProperty("state")
    Integer state;

    @JsonProperty("generate_docs")
    Boolean generateDocs;

    @JsonProperty("schedule")
    JobSchedule schedule;
}
