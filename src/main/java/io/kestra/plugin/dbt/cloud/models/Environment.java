package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class Environment   {
  @JsonProperty("id")
  Integer id;

  @JsonProperty("account_id")
  Integer accountId;

  @JsonProperty("deploy_key_id")
  Integer deployKeyId;

  @JsonProperty("created_by_id")
  Integer createdById;

  @JsonProperty("repository_id")
  Integer repositoryId;

  @JsonProperty("name")
  String name;

  @JsonProperty("dbt_version")
  String dbtVersion;

  @JsonProperty("use_custom_branch")
  Boolean useCustomBranch;

  @JsonProperty("custom_branch")
  String customBranch;

  @JsonProperty("supports_docs")
  Boolean supportsDocs;

  @JsonProperty("state")
  Integer state;
}
