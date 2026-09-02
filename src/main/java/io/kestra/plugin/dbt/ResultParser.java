package io.kestra.plugin.dbt;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import org.slf4j.event.Level;

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
import io.kestra.core.models.executions.metrics.Timer;
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

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static java.lang.Math.max;

public abstract class ResultParser {
    static final protected ObjectMapper MAPPER = JacksonMapper.ofJson(false)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private static final String TABLE_ASSET_TYPE = "io.kestra.plugin.ee.assets.Table";
    private static final String RESOURCE_TYPE_MODEL = "model";
    private static final String RESOURCE_TYPE_SEED = "seed";
    private static final String RESOURCE_TYPE_SNAPSHOT = "snapshot";
    private static final String RESOURCE_TYPE_SOURCE = "source";

    // dbt node resource types that map to a physical table dbt builds.
    private static final Set<String> PRODUCED_RESOURCE_TYPES = Set.of(RESOURCE_TYPE_MODEL, RESOURCE_TYPE_SEED, RESOURCE_TYPE_SNAPSHOT);

    /**
     * @param fullyEmitted false when an emit failed part way, so a caller recording "this run is done" can
     *        tell that the lineage did not fully land.
     * @param assetIds populated whether or not lineage was emitted.
     */
    public record ManifestResult(Manifest manifest, URI uri, boolean fullyEmitted, List<String> assetIds) {
    }

    public static ManifestResult parseManifestWithAssets(RunContext runContext, File file) throws IOException, IllegalVariableEvaluationException {
        return parseManifestWithAssets(runContext, file, true, Map.of());
    }

    public static ManifestResult parseManifestWithAssets(RunContext runContext, File file, boolean emitLineage)
        throws IOException, IllegalVariableEvaluationException {
        return parseManifestWithAssets(runContext, file, emitLineage, Map.of());
    }

    /**
     * A caller that has already emitted for this dbt run passes {@code emitLineage} false: the run's
     * artifacts are immutable, so re-emitting only appends identical lineage events (issue #318). The asset
     * ids come back either way.
     *
     * {@code assetMetadata} is merged into every asset produced, carrying facts the manifest does not know,
     * such as the producing dbt Cloud job's schedule (issue #323).
     */
    public static ManifestResult parseManifestWithAssets(RunContext runContext, File file, boolean emitLineage, Map<String, Object> assetMetadata)
        throws IOException, IllegalVariableEvaluationException {
        Manifest manifest = null;
        boolean fullyEmitted = true;
        List<String> assetIds = List.of();

        // Asset lineage is metadata about the run, not the run itself. dbt adds fields to the
        // manifest between schema versions, so a manifest this plugin cannot map must not turn a
        // successful dbt run into a failed task. Store the raw file and carry on without assets.
        try {
            manifest = MAPPER.readValue(file, Manifest.class);

            Map<String, ModelAsset> assetNodes = extractAssetNodes(manifest);
            assetIds = assetNodes.values().stream()
                .filter(ModelAsset::produced)
                .map(ModelAsset::assetId)
                .sorted()
                .toList();

            if (emitLineage) {
                fullyEmitted = emitAssets(runContext, assetNodes, assetMetadata);
            } else {
                runContext.logger().debug("Lineage already emitted for this run, skipping {} assets", assetIds.size());
            }
        } catch (Exception e) {
            manifest = null;
            // Nothing was emitted, so the run must not be recorded as done.
            fullyEmitted = false;
            runContext.logger().warn("Unable to read the dbt manifest, assets will not be emitted. The manifest is still stored as an output file.", e);
        }

        return new ManifestResult(manifest, runContext.storage().putFile(file), fullyEmitted, assetIds);
    }

    public static URI parseRunResult(RunContext runContext, File file, Manifest manifest) throws IOException, IllegalVariableEvaluationException {
        return parseRunResult(runContext, file, manifest, true);
    }

    /**
     * With {@code attachAssets} false the per-model taskruns are still emitted, without their asset links: a
     * taskrun's assets become lineage events too, so both paths have to be suppressed together.
     */
    public static URI parseRunResult(RunContext runContext, File file, Manifest manifest, boolean attachAssets)
        throws IOException, IllegalVariableEvaluationException {
        RunResult result = MAPPER.readValue(
            file,
            RunResult.class
        );

        Map<String, ModelAsset> modelAssets = (manifest == null || !attachAssets) ? Map.of() : extractAssetNodes(manifest);

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

                // The terminal date is anchored on dbt's own execution_time rather than the end of the last
                // timing phase (issue #316). execution_time is the whole cost of the node, while the phases
                // cover only compile and execute, so the phase span understates the node and the Gantt showed
                // every model as near-instantaneous. Never earlier than the last phase ended, so the history
                // cannot invert if the two disagree.
                OptionalLong firstStartedAt = r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .min();
                OptionalLong lastCompletedAt = r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getCompletedAt().toEpochMilli())
                    .max();

                if (lastCompletedAt.isPresent()) {
                    long terminalAt = lastCompletedAt.getAsLong();

                    if (r.getExecutionTime() != null && firstStartedAt.isPresent()) {
                        long fromExecutionTime = firstStartedAt.getAsLong() + Math.round(r.getExecutionTime() * 1000);
                        terminalAt = max(terminalAt, fromExecutionTime);
                    }

                    histories.add(
                        new State.History(
                            r.state(),
                            Instant.ofEpochMilli(terminalAt)
                        )
                    );
                }

                State state = State.of(
                    r.state(),
                    histories
                );

                if (r.getExecutionTime() != null) {
                    runContext.metric(
                        Timer.of("node.execution.duration", Duration.ofMillis(Math.round(r.getExecutionTime() * 1000)), "node", r.getUniqueId())
                    );
                }

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
                    taskRunBuilder.assetEmits(List.of(assets));
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
        // A source can resolve here via `dbt source freshness` results and must never be this taskrun's output.
        if (modelAsset == null || !modelAsset.produced()) {
            return null;
        }

        List<AssetIdentifier> inputs = inputIdentifiers(modelAsset, modelAssets);
        List<Asset> outputs = List.of(selfAsset(modelAsset));

        return new AssetsInOut(inputs, outputs);
    }

    /** @return false if any asset failed to emit, so the caller never records a partial emit as complete. */
    private static boolean emitAssets(RunContext runContext, Map<String, ModelAsset> assetNodes, Map<String, Object> assetMetadata) throws IllegalVariableEvaluationException {
        runContext.logger().info("dbt assets extracted from manifest: {}", assetNodes.size());
        boolean fullyEmitted = true;

        for (ModelAsset asset : assetNodes.values()) {
            if (!asset.produced()) {
                continue;
            }

            // Bundle is {parents} -> {this node} only (never children) so each event is self-contained, no cartesian join.
            List<AssetIdentifier> inputs = inputIdentifiers(asset, assetNodes);
            List<Asset> outputs = List.of(selfAsset(asset, assetMetadata));
            try {
                runContext.assets().emit(new AssetEmit(inputs, outputs));
            } catch (UnsupportedOperationException e) {
                // OSS edition or tests where EE assets are not available — silently skip. Reported as an
                // incomplete emit so the caller never claims lineage landed nor records the run as done.
                runContext.logger().debug("Asset emission is not supported in this edition, skipping.");
                return false;
            } catch (QueueException e) {
                // Carry on so one bad asset does not drop the rest, but report back so the caller does not
                // mark the run done and skip the missing ones for good.
                fullyEmitted = false;
                runContext.logger().warn("Unable to emit dbt asset '{}'", asset.assetId(), e);
            }
        }

        return fullyEmitted;
    }

    private static Asset selfAsset(ModelAsset asset) {
        return selfAsset(asset, Map.of());
    }

    private static Asset selfAsset(ModelAsset asset, Map<String, Object> assetMetadata) {
        Map<String, Object> metadata = new HashMap<>(asset.metadata());
        metadata.putAll(assetMetadata);

        return Custom.builder()
            .id(asset.assetId())
            .type(TABLE_ASSET_TYPE)
            .metadata(metadata)
            .build();
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

    // Every model/seed/snapshot/source as an asset node keyed by unique_id, deps filtered to nodes in this set.
    private static Map<String, ModelAsset> extractAssetNodes(Manifest manifest) {
        if (manifest == null) {
            return Map.of();
        }

        String system = adapterType(manifest);
        Map<String, ModelAsset> assetNodes = new HashMap<>();

        // Table-producing nodes (models, seeds, snapshots) from `nodes`.
        if (manifest.getNodes() != null) {
            for (Map.Entry<String, Manifest.Node> entry : manifest.getNodes().entrySet()) {
                Manifest.Node node = entry.getValue();
                if (node == null || node.getResourceType() == null || !PRODUCED_RESOURCE_TYPES.contains(lower(node.getResourceType()))) {
                    continue;
                }

                String uniqueId = firstNonBlank(node.getUniqueId(), entry.getKey());
                if (uniqueId == null) {
                    continue;
                }

                String name = firstNonBlank(node.getAlias(), node.getName(), uniqueId);
                String assetId = assetIdFor(node.getDatabase(), node.getSchema(), name, uniqueId);

                // Use parent_map from manifest (the canonical DAG) when available,
                // falling back to node-level depends_on for older manifests.
                List<String> dependsOn;
                if (manifest.getParentMap() != null && manifest.getParentMap().containsKey(uniqueId)) {
                    dependsOn = manifest.getParentMap().get(uniqueId);
                } else if (node.getDependsOn() != null && node.getDependsOn().getNodes() != null) {
                    dependsOn = node.getDependsOn().getNodes();
                } else {
                    dependsOn = List.of();
                }

                assetNodes.put(uniqueId, new ModelAsset(assetId, metadataFor(system, node.getDatabase(), node.getSchema(), name), dependsOn, lower(node.getResourceType())));
            }
        }

        // Sources (raw tables the project reads but does not build). Referenced as parents; no dependencies.
        if (manifest.getSources() != null) {
            for (Map.Entry<String, Manifest.Source> entry : manifest.getSources().entrySet()) {
                Manifest.Source source = entry.getValue();
                if (source == null) {
                    continue;
                }

                String uniqueId = firstNonBlank(source.getUniqueId(), entry.getKey());
                if (uniqueId == null) {
                    continue;
                }

                // The physical table name is `identifier`; fall back to the dbt source name.
                String name = firstNonBlank(source.getIdentifier(), source.getName(), uniqueId);
                String assetId = assetIdFor(source.getDatabase(), source.getSchema(), name, uniqueId);

                assetNodes.put(uniqueId, new ModelAsset(assetId, metadataFor(system, source.getDatabase(), source.getSchema(), name), List.of(), RESOURCE_TYPE_SOURCE));
            }
        }

        // Keep only dependencies that resolve to a known asset node in this set.
        Map<String, ModelAsset> resolved = new HashMap<>(assetNodes.size());
        for (Map.Entry<String, ModelAsset> e : assetNodes.entrySet()) {
            ModelAsset a = e.getValue();
            List<String> deps = a.dependsOn() == null ? List.of()
                : a.dependsOn().stream()
                    .filter(assetNodes::containsKey)
                    .toList();
            resolved.put(e.getKey(), new ModelAsset(a.assetId(), a.metadata(), deps, a.resourceType()));
        }

        return resolved;
    }

    private static Map<String, Object> metadataFor(String system, String database, String schema, String name) {
        Map<String, Object> metadata = new HashMap<>();
        if (hasValue(system)) {
            metadata.put("system", system);
        }
        if (hasValue(database)) {
            metadata.put("database", database);
        }
        if (hasValue(schema)) {
            metadata.put("schema", schema);
        }
        if (hasValue(name)) {
            metadata.put("name", name);
        }
        return metadata;
    }

    private static String lower(String value) {
        return value == null ? null : value.toLowerCase(Locale.ROOT);
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

    private record ModelAsset(String assetId, Map<String, Object> metadata, List<String> dependsOn, String resourceType) {
        // True for nodes dbt builds (models, seeds, snapshots), not read-only sources.
        boolean produced() {
            return PRODUCED_RESOURCE_TYPES.contains(resourceType);
        }
    }
}
