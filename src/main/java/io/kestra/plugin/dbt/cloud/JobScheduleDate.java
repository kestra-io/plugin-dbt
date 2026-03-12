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
public class JobScheduleDate {
    @JsonProperty("type")
    JobScheduleDateType type;

    @JsonProperty("days")
    @Valid
    List<Integer> days;

    @JsonProperty("cron")
    String cron;
}
