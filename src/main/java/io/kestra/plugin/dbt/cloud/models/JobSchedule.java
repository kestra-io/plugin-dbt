package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class JobSchedule {
    @JsonProperty("cron")
    String cron;

    @JsonProperty("date")
    String date;

    @JsonProperty("time")
    String time;
}
