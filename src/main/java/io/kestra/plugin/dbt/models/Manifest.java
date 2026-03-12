package io.kestra.plugin.dbt.models;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@SuperBuilder
public class Manifest {
    Map<String, Object> metadata;
    Map<String, Node> nodes;

    @JsonProperty("parent_map")
    Map<String, List<String>> parentMap;

    @Value
    @Jacksonized
    @SuperBuilder
    public static class Node {
        @JsonProperty("compiled_sql")
        String compiledSql;

        String database;

        String schema;

        String name;

        String alias;

        @JsonProperty("resource_type")
        String resourceType;

        @JsonProperty("depends_on")
        Map<String, List<String>> dependsOn;

        @JsonProperty("unique_id")
        String uniqueId;
    }
}
