package io.kestra.plugin.dbt.cloud.models;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class RunResponse {
    Run data;

    Status status;
}
