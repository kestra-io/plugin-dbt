package io.kestra.plugin.dbt.cloud;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.Valid;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class JobScheduleTime {
    @JsonProperty("type")
    JobScheduleTimeType type;

    @JsonProperty("interval")
    Integer interval;

    @JsonProperty("hours")
    @Valid
    List<Integer> hours;
}
