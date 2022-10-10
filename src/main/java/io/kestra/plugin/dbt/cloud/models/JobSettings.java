package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class JobSettings {
    @JsonProperty("threads")
    Integer threads;

    @JsonProperty("target_name")
    String targetName;
}
