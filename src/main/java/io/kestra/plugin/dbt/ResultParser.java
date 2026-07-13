package io.kestra.plugin.dbt;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.assets.Asset;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.assets.AssetsInOut;
import io.kestra.core.models.assets.Custom;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.DynamicTaskRunLog;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.dbt.models.Manifest;
import io.kestra.plugin.dbt.models.RunResult;

import org.slf4j.event.Level;

import static io.kestra.core.utils.Rethrow.throwConsumer;

public abstract class ResultParser {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson(false)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private static final String TABLE_ASSET_TYPE = "io.kestra.plugin.ee.assets.Table";
    private static final String RESOURCE_TYPE_MODEL = "model";

    public record ManifestResult(Manifest manifest, URI uri) {
    }

    public static ManifestResult parseManifestWithAssets(RunContext runContext, File file) throws IOException, IllegalVariableEvaluationException {
        Manifest manifest = MAPPER.readValue(file, Manifest.class);
        emitAssets(runContext, manifest);
        return new ManifestResult(manifest, runContext.storage().putFile(file));
    }

    public static URI parseRunResult(RunContext runContext, File file, Manifest manifest) throws IOException, IllegalVariableEvaluationException {
        RunResult result = MAPPER.readValue(
            file,
            RunResult.class
        );

        Map<String, ModelAsset> modelAssets = manifest == null ? Map.of() : extractModelAssets(manifest);

        // Emit one dynamic taskrun per dbt model (the UI timeline "bars"), attaching that model's
        // own status/message/failures as logs riding with its taskrun so they render inline under
        // its bar instead of all landing on the parent task root (issue #276).
        result
            .getResults()
            .stream()
            .forEach(throwConsumer(r ->
            {
                ArrayList<State.History> histories = new ArrayList<>();

                // List of status are not safe and can be not present on api calls
                r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .min()
                    .ifPresent(value ->
                    {
                        histories.add(
                            new State.History(
                                State.Type.CREATED,
                                Instant.ofEpochMilli(value)
                            )
                        );
                    });

                r.getTiming()
                    .stream()
                    .filter(timing -> timing.getName().equals("execute"))
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .min()
                    .ifPresent(value ->
                    {
                        histories.add(
                            new State.History(
                                State.Type.RUNNING,
                                Instant.ofEpochMilli(value)
                            )
                        );
                    });

                r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getCompletedAt().toEpochMilli())
                    .max()
                    .ifPresent(value ->
                    {
                        histories.add(
                            new State.History(
                                r.state(),
                                Instant.ofEpochMilli(value)
                            )
                        );
                    });

                State state = State.of(
                    r.state(),
                    histories
                );

                r.getAdapterResponse()
                    .entrySet()
                    .stream()
                    .map(e ->
                    {
                        return switch (e.getKey()) {
                            case "rows_affected" -> Counter.of("rows.affected", Double.valueOf(e.getValue()));
                            case "bytes_processed" -> Counter.of("bytes.processed", Double.valueOf(e.getValue()));
                            default -> null;
                        };
                    })
                    .filter(Objects::nonNull)
                    .forEach(runContext::metric);

                AssetsInOut assets = assetsFor(r.getUniqueId(), modelAssets);
                TaskRun.TaskRunBuilder taskRunBuilder = TaskRun.builder()
                    .id(IdUtils.create())
                    .namespace(runContext.render("{{ flow.namespace }}"))
                    .flowId(runContext.render("{{ flow.id }}"))
                    .taskId(r.getUniqueId())
                    .executionId(runContext.render("{{ execution.id }}"))
                    .parentTaskRunId(runContext.render("{{ taskrun.id }}"))
                    .state(state)
                    .attempts(
                        List.of(
                            TaskRunAttempt.builder()
                                .state(state)
                                .build()
                        )
                    );
                if (assets != null) {
                    taskRunBuilder.assets(assets);
                }

                // Register the dynamic taskrun together with its log lines in one call: the run
                // context builds the LogEntry, forcing execution/tenant/namespace/flow from itself,
                // fixing the attempt to 0 and masking secrets (the plugin never builds a LogEntry).
                runContext.dynamicWorkerResult(
                    WorkerTaskResult.builder().taskRun(taskRunBuilder.build()).build(),
                    modelLogs(r)
                );
            }));

        return runContext.storage().putFile(file);
    }

    /**
     * Build the log lines for a single dbt model, to be attached to that model's dynamic taskrun.
     * A concise summary line (`uniqueId => status`, execution time, and the failure count when any),
     * followed by the model's own message when present (SQL/compile errors, dbt status messages).
     * rows_affected / bytes_processed are intentionally left out — they are already emitted as metrics.
     */
    static List<DynamicTaskRunLog> modelLogs(RunResult.Result r) {
        Level level = switch (r.state()) {
            case FAILED -> Level.ERROR;
            case WARNING -> Level.WARN;
            default -> Level.INFO;
        };

        List<DynamicTaskRunLog> logs = new ArrayList<>();

        StringBuilder summary = new StringBuilder(r.getUniqueId() + " => " + r.getStatus());
        if (r.getExecutionTime() != null) {
            summary.append(String.format(Locale.ROOT, " in %.2fs", r.getExecutionTime()));
        }
        if (r.getFailures() != null && r.getFailures() > 0) {
            summary.append(" (").append(r.getFailures()).append(r.getFailures() == 1 ? " failure)" : " failures)");
        }
        logs.add(new DynamicTaskRunLog(level, summary.toString()));

        if (r.getMessage() != null && !r.getMessage().isBlank()) {
            logs.add(new DynamicTaskRunLog(level, r.getMessage()));
        }

        return logs;
    }

    private static AssetsInOut assetsFor(String uniqueId, Map<String, ModelAsset> modelAssets) {
        if (uniqueId == null) {
            return null;
        }

        ModelAsset modelAsset = modelAssets.get(uniqueId);
        if (modelAsset == null) {
            return null;
        }

        List<AssetIdentifier> inputs = inputIdentifiers(modelAsset, modelAssets);
        List<Asset> outputs = outputAssets(modelAsset, modelAssets);

        return new AssetsInOut(inputs, outputs);
    }

    private static void emitAssets(RunContext runContext, Manifest manifest) throws IllegalVariableEvaluationException {
        Map<String, ModelAsset> modelAssets = extractModelAssets(manifest);
        runContext.logger().info("dbt assets extracted from manifest: {}", modelAssets.size());

        for (ModelAsset asset : modelAssets.values()) {
            List<AssetIdentifier> inputs = inputIdentifiers(asset, modelAssets);
            List<Asset> outputs = outputAssets(asset, modelAssets);
            try {
                runContext.assets().emit(new AssetEmit(inputs, outputs));
            } catch (UnsupportedOperationException e) {
                // OSS edition or tests where EE assets are not available — silently skip.
                runContext.logger().debug("Asset emission is not supported in this edition, skipping.");
                break;
            } catch (QueueException e) {
                runContext.logger().warn("Unable to emit dbt asset '{}'", asset.assetId(), e);
            }
        }
    }

    private static List<Asset> outputAssets(ModelAsset modelAsset, Map<String, ModelAsset> modelAssets) {
        if (modelAsset.children() == null || modelAsset.children().isEmpty()) {
            return List.of();
        }

        return modelAsset.children().stream()
            .map(modelAssets::get)
            .filter(Objects::nonNull)
            .<Asset> map(
                child -> Custom.builder()
                    .id(child.assetId())
                    .type(TABLE_ASSET_TYPE)
                    .metadata(child.metadata())
                    .build()
            )
            .toList();
    }

    private static List<AssetIdentifier> inputIdentifiers(ModelAsset modelAsset, Map<String, ModelAsset> modelAssets) {
        if (modelAsset.dependsOn() == null || modelAsset.dependsOn().isEmpty()) {
            return List.of();
        }

        return modelAsset.dependsOn().stream()
            .map(modelAssets::get)
            .filter(Objects::nonNull)
            .map(dep -> new AssetIdentifier(null, null, dep.assetId(), TABLE_ASSET_TYPE))
            .toList();
    }

    private static Map<String, ModelAsset> extractModelAssets(Manifest manifest) {
        if (manifest == null || manifest.getNodes() == null || manifest.getNodes().isEmpty()) {
            return Map.of();
        }

        String system = adapterType(manifest);
        Map<String, ModelAsset> modelAssets = new HashMap<>();

        for (Map.Entry<String, Manifest.Node> entry : manifest.getNodes().entrySet()) {
            Manifest.Node node = entry.getValue();
            if (node == null || !RESOURCE_TYPE_MODEL.equalsIgnoreCase(node.getResourceType())) {
                continue;
            }

            String uniqueId = firstNonBlank(node.getUniqueId(), entry.getKey());
            if (uniqueId == null) {
                continue;
            }

            String name = firstNonBlank(node.getAlias(), node.getName(), uniqueId);
            String assetId = assetIdFor(node.getDatabase(), node.getSchema(), name, uniqueId);

            Map<String, Object> metadata = new HashMap<>();
            if (hasValue(system))
                metadata.put("system", system);
            if (hasValue(node.getDatabase()))
                metadata.put("database", node.getDatabase());
            if (hasValue(node.getSchema()))
                metadata.put("schema", node.getSchema());
            if (hasValue(name))
                metadata.put("name", name);

            // Use parent_map from manifest (the canonical DAG) when available,
            // falling back to node-level depends_on for older manifests.
            List<String> dependsOn;
            if (manifest.getParentMap() != null && manifest.getParentMap().containsKey(uniqueId)) {
                dependsOn = manifest.getParentMap().get(uniqueId);
            } else if (node.getDependsOn() != null) {
                dependsOn = node.getDependsOn().getOrDefault("nodes", List.of());
            } else {
                dependsOn = List.of();
            }

            modelAssets.put(uniqueId, new ModelAsset(assetId, metadata, dependsOn, List.of()));
        }

        Map<String, ModelAsset> filtered = new HashMap<>(modelAssets.size());
        for (Map.Entry<String, ModelAsset> e : modelAssets.entrySet()) {
            ModelAsset a = e.getValue();
            List<String> deps = a.dependsOn() == null ? List.of()
                : a.dependsOn().stream()
                    .filter(modelAssets::containsKey)
                    .toList();
            filtered.put(e.getKey(), new ModelAsset(a.assetId(), a.metadata(), deps, List.of()));
        }

        // Compute reverse dependencies (children: models that depend on this one)
        Map<String, List<String>> childrenMap = new HashMap<>();
        for (Map.Entry<String, ModelAsset> e : filtered.entrySet()) {
            for (String dep : e.getValue().dependsOn()) {
                childrenMap.computeIfAbsent(dep, k -> new ArrayList<>()).add(e.getKey());
            }
        }

        // Rebuild with children populated
        Map<String, ModelAsset> result = new HashMap<>(filtered.size());
        for (Map.Entry<String, ModelAsset> e : filtered.entrySet()) {
            ModelAsset a = e.getValue();
            List<String> children = childrenMap.getOrDefault(e.getKey(), List.of());
            result.put(e.getKey(), new ModelAsset(a.assetId(), a.metadata(), a.dependsOn(), children));
        }

        return result;
    }

    private static String adapterType(Manifest manifest) {
        if (manifest.getMetadata() == null) {
            return null;
        }
        Object adapterType = manifest.getMetadata().get("adapter_type");
        return adapterType == null ? null : adapterType.toString();
    }

    private static String assetIdFor(String database, String schema, String name, String fallback) {
        List<String> parts = new ArrayList<>();
        if (hasValue(database)) {
            parts.add(database);
        }
        if (hasValue(schema)) {
            parts.add(schema);
        }
        if (hasValue(name)) {
            parts.add(name);
        }
        if (!parts.isEmpty()) {
            return String.join(".", parts);
        }
        return fallback;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (hasValue(value)) {
                return value;
            }
        }
        return null;
    }

    private static boolean hasValue(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private record ModelAsset(String assetId, Map<String, Object> metadata, List<String> dependsOn, List<String> children) {
    }
}
