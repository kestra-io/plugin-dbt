package io.kestra.plugin.dbt.cloud.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;
import java.util.List;

@Value
@Jacksonized
@SuperBuilder
public class ManifestArtifact {
    Map<String, Object> metadata;

    Map<String, Map<String, Object>> nodes;

    Map<String, Map<String, Object>> sources;

    Map<String, Map<String, Object>> macros;

    Map<String, Map<String, Object>> docs;

    Map<String, Map<String, Object>> exposures;

    Map<String, Map<String, Object>> metrics;

    Map<String, Map<String, Object>> groups;

    Map<String, Object> selectors;

    @JsonProperty("disabled")
    Map<String, List<Map<String, Object>>> disabled;

    @JsonProperty("parent_map")
    Map<String, List<String>> parentMap;

    @JsonProperty("child_map")
    Map<String, List<String>> childMap;

    @JsonProperty("group_map")
    Map<String, List<String>> groupMap;

    @JsonProperty("saved_queries")
    Map<String, Object> savedQueries;

    @JsonProperty("semantic_models")
    Map<String, Map<String, Object>> semanticModels;

    @JsonProperty("unit_tests")
    Map<String, Map<String, Object>> unitTests;
}
