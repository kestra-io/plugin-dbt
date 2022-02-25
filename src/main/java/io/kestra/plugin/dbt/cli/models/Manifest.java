package io.kestra.plugin.dbt.cli.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.core.annotation.Introspected;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.List;
import java.util.Map;

@Value
@Jacksonized
@SuperBuilder
public class Manifest {
    Map<String, Node> nodes;

    @Value
    @Jacksonized
    @SuperBuilder
    public static class Node {
        @JsonProperty("compiled_sql")
        String compiledSql;

        @JsonProperty("resource_type")
        String resourceType;

        @JsonProperty("depends_on")
        Map<String, List<String>> dependsOn;

        @JsonProperty("unique_id")
        String uniqueId;
    }
}

