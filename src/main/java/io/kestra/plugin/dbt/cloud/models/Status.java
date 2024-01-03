package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class Status   {
  Integer code;

  @JsonProperty("is_success")
  Boolean isSuccess;

  @JsonProperty("user_message")
  String userMessage;

  @JsonProperty("developer_message")
  String developerMessage;
}
