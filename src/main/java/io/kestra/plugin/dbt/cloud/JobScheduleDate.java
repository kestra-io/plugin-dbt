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
public class JobScheduleDate {
    @JsonProperty("type")
    JobScheduleDateType type;

    @JsonProperty("days")
    @Valid
    List<Integer> days;

    @JsonProperty("cron")
    String cron;
}
