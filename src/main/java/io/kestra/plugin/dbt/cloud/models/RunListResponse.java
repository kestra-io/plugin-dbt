package io.kestra.plugin.dbt.cloud.models;

import java.util.List;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class RunListResponse {
    List<Run> data;

    Status status;
}
