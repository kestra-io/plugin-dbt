package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class JobTriggers {
    @JsonProperty("github_webhook")
    Boolean githubWebhook;

    @JsonProperty("git_provider_webhook")
    Boolean gitProviderWebhook;

    @JsonProperty("schedule")
    Boolean schedule;

    @JsonProperty("custom_branch_only")
    Boolean customBranchOnly;
}
