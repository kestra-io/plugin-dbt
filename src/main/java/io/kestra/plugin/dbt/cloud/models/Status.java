package io.kestra.plugin.dbt.cloud.models;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.swagger.v3.oas.annotations.media.Schema;
import io.micronaut.validation.Validated;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import javax.validation.Valid;
import javax.validation.constraints.*;

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
