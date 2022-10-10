package io.kestra.plugin.dbt.cloud;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import javax.validation.Valid;

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
