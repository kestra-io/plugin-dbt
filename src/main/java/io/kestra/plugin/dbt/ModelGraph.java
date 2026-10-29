package io.kestra.plugin.dbt;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import io.kestra.core.runners.RunContext;
import io.kestra.plugin.dbt.models.Manifest;
import io.kestra.plugin.dbt.models.RunResult;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

/** The project DAG joined with one run's results, stored as `graph.json` for the Show details drawer. */
@Value
@Builder
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelGraph {
    static final String NOT_RUN = "not-run";
    private static final String NO_OP = "no-op";

    List<Node> nodes;
    List<Edge> edges;
    Summary summary;

    @Value
    @Builder
    @Jacksonized
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Node {
        String uniqueId;
        String name;
        String resourceType;
        String materialized;
        // database.schema.alias, the table dbt builds or reads
        String relation;
        List<String> tags;
        // success, error, warn, skipped or not-run, or the raw dbt status when it is not one this plugin knows
        String status;
        // Execution time in seconds as reported by dbt, absent when the run did not touch the node.
        Double executionTime;
        // Longest path from a root, so the drawer can lay nodes out top-down without its own layout pass.
        int level;
    }

    public record Edge(String from, String to) {
    }

    @Value
    @Builder
    @Jacksonized
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Summary {
        @Schema(title = "Number of nodes in the graph (models, seeds, snapshots and sources)")
        int nodes;

        @Schema(title = "Number of dependency edges")
        int edges;

        @Schema(title = "Number of nodes per run status, e.g. `{\"success\": 12, \"error\": 1, \"not-run\": 4}`")
        Map<String, Integer> byStatus;
    }

    /** @param uri internal storage URI of `graph.json` */
    public record Stored(URI uri, Summary summary) {
    }

    // Merges run results by unique_id in run order, a later result wins. One run result is returned as is.
    public static RunResult merge(RunResult... inRunOrder) {
        var present = Arrays.stream(inRunOrder).filter(runResult -> runResult != null && runResult.getResults() != null && !runResult.getResults().isEmpty()).toList();
        if (present.size() < 2) {
            return present.isEmpty() ? (inRunOrder.length == 0 ? null : inRunOrder[inRunOrder.length - 1]) : present.getFirst();
        }

        var byId = new LinkedHashMap<String, RunResult.Result>();
        present.forEach(
            runResult -> runResult.getResults().stream()
                .filter(result -> result.getUniqueId() != null)
                .forEach(result -> byId.put(result.getUniqueId(), result))
        );
        return RunResult.builder().results(new ArrayList<>(byId.values())).build();
    }

    /** Never throws: the graph is a view of the run, so failing to build it only logs a warning. */
    public static Stored store(RunContext runContext, Manifest manifest, RunResult runResult) {
        if (manifest == null) {
            return null;
        }

        try {
            var graph = from(manifest, runResult);
            var uri = runContext.storage().putFile(new ByteArrayInputStream(ResultParser.MAPPER.writeValueAsBytes(graph)), "graph.json");
            return new Stored(uri, graph.getSummary());
        } catch (Exception e) {
            runContext.logger().warn("Unable to build the dbt model graph, the `graph` output is skipped", e);
            return null;
        }
    }

    /** @param runResult null when run_results was not produced or not parsed: every node is then `not-run`. */
    static ModelGraph from(Manifest manifest, RunResult runResult) {
        var graphNodes = new TreeMap<String, Manifest.Node>();
        if (manifest.getNodes() != null) {
            manifest.getNodes().forEach((key, node) ->
            {
                if (node != null && ResultParser.PRODUCED_RESOURCE_TYPES.contains(ResultParser.lower(node.getResourceType()))) {
                    graphNodes.put(ResultParser.firstNonBlank(node.getUniqueId(), key), node);
                }
            });
        }
        var sources = new TreeMap<String, Manifest.Source>();
        if (manifest.getSources() != null) {
            manifest.getSources().forEach((key, source) ->
            {
                if (source != null) {
                    sources.put(ResultParser.firstNonBlank(source.getUniqueId(), key), source);
                }
            });
        }

        var ids = new HashSet<String>(graphNodes.keySet());
        ids.addAll(sources.keySet());

        var edges = new ArrayList<Edge>();
        graphNodes.forEach(
            (id, node) -> parents(manifest, node, id).stream()
                .filter(ids::contains)
                .distinct()
                .sorted()
                .forEach(parent -> edges.add(new Edge(parent, id)))
        );

        var levels = levels(ids, edges);
        var statuses = statuses(runResult);
        var times = executionTimes(runResult);

        var nodes = new ArrayList<Node>();
        graphNodes.forEach((id, node) ->
        {
            nodes.add(
                Node.builder()
                    .uniqueId(id)
                    .name(node.getName())
                    .resourceType(ResultParser.lower(node.getResourceType()))
                    .materialized(node.getConfig() == null ? null : node.getConfig().getMaterialized())
                    .relation(relation(node.getDatabase(), node.getSchema(), ResultParser.firstNonBlank(node.getAlias(), node.getName())))
                    .tags(node.getTags() == null || node.getTags().isEmpty() ? null : node.getTags())
                    .status(statuses.getOrDefault(id, NOT_RUN))
                    .executionTime(times.get(id))
                    .level(levels.getOrDefault(id, 0))
                    .build()
            );
        });
        sources.forEach(
            (id, source) -> nodes.add(
                Node.builder()
                    .uniqueId(id)
                    .name(source.getName())
                    .resourceType(ResultParser.RESOURCE_TYPE_SOURCE)
                    .relation(relation(source.getDatabase(), source.getSchema(), ResultParser.firstNonBlank(source.getIdentifier(), source.getName())))
                    .status(statuses.getOrDefault(id, NOT_RUN))
                    .level(levels.getOrDefault(id, 0))
                    .build()
            )
        );
        nodes.sort(Comparator.comparingInt(Node::getLevel).thenComparing(Node::getUniqueId));

        return ModelGraph.builder()
            .nodes(nodes)
            .edges(edges)
            .summary(
                Summary.builder()
                    .nodes(nodes.size())
                    .edges(edges.size())
                    .byStatus(count(nodes.stream().map(Node::getStatus)))
                    .build()
            )
            .build();
    }

    // parent_map first, depends_on as fallback, same resolution as the asset lineage.
    private static List<String> parents(Manifest manifest, Manifest.Node node, String id) {
        if (manifest.getParentMap() != null && manifest.getParentMap().get(id) != null) {
            return manifest.getParentMap().get(id);
        }
        if (node.getDependsOn() != null && node.getDependsOn().getNodes() != null) {
            return node.getDependsOn().getNodes();
        }
        return List.of();
    }

    // Longest path from a root (Kahn order). A node on a cycle, which dbt rejects anyway, stays at 0.
    private static Map<String, Integer> levels(Set<String> ids, List<Edge> edges) {
        var children = new HashMap<String, List<String>>();
        var inDegree = new HashMap<String, Integer>();
        ids.forEach(id -> inDegree.put(id, 0));
        edges.forEach(edge ->
        {
            children.computeIfAbsent(edge.from(), k -> new ArrayList<>()).add(edge.to());
            inDegree.merge(edge.to(), 1, Integer::sum);
        });

        var levels = new HashMap<String, Integer>();
        var ready = new ArrayDeque<String>();
        inDegree.forEach((id, degree) ->
        {
            if (degree == 0) {
                ready.add(id);
            }
        });
        while (!ready.isEmpty()) {
            var id = ready.poll();
            var level = levels.getOrDefault(id, 0);
            for (var child : children.getOrDefault(id, List.of())) {
                levels.merge(child, level + 1, Math::max);
                if (inDegree.merge(child, -1, Integer::sum) == 0) {
                    ready.add(child);
                }
            }
        }
        return levels;
    }

    private static Map<String, String> statuses(RunResult runResult) {
        if (runResult == null || runResult.getResults() == null) {
            return Map.of();
        }

        var statuses = new HashMap<String, String>();
        for (var result : runResult.getResults()) {
            if (result.getUniqueId() != null && result.getStatus() != null) {
                statuses.put(result.getUniqueId(), status(result));
            }
        }
        return statuses;
    }

    private static Map<String, Double> executionTimes(RunResult runResult) {
        if (runResult == null || runResult.getResults() == null) {
            return Map.of();
        }

        var times = new HashMap<String, Double>();
        for (var result : runResult.getResults()) {
            if (result.getUniqueId() != null && result.getExecutionTime() != null) {
                times.put(result.getUniqueId(), result.getExecutionTime());
            }
        }
        return times;
    }

    private static String status(RunResult.Result result) {
        // "no-op" counts as a success for the task state, but the node did nothing, so the graph keeps it raw.
        if (NO_OP.equals(result.getStatus())) {
            return NO_OP;
        }
        try {
            return switch (result.state()) {
                case SUCCESS -> "success";
                case FAILED -> "error";
                case WARNING -> "warn";
                case SKIPPED -> "skipped";
                default -> result.getStatus();
            };
        } catch (IllegalStateException e) {
            // Unknown status (e.g. "partial success"): kept raw so the drawer shows it neutral, never as a pass.
            return result.getStatus();
        }
    }

    private static String relation(String... parts) {
        var relation = Arrays.stream(parts)
            .filter(part -> part != null && !part.isBlank())
            .collect(Collectors.joining("."));
        return relation.isEmpty() ? null : relation;
    }

    private static Map<String, Integer> count(Stream<String> values) {
        return values.collect(Collectors.toMap(value -> value, value -> 1, Integer::sum, TreeMap::new));
    }
}
