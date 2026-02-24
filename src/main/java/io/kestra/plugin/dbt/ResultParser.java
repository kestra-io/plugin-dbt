package io.kestra.plugin.dbt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.assets.AssetsInOut;
import io.kestra.core.models.assets.Custom;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.executions.TaskRunAttempt;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.flows.State;
import io.kestra.core.queues.QueueException;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkerTaskResult;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.dbt.models.Manifest;
import io.kestra.plugin.dbt.models.RunResult;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwFunction;

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

        java.util.List<WorkerTaskResult> workerTaskResults = result
            .getResults()
            .stream()
            .map(throwFunction(r -> {
                ArrayList<State.History> histories = new ArrayList<>();

                // List of status are not safe and can be not present on api calls
                r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .min()
                    .ifPresent(value -> {
                        histories.add(new State.History(
                            State.Type.CREATED,
                            Instant.ofEpochMilli(value)
                        ));
                    });

                r.getTiming()
                    .stream()
                    .filter(timing -> timing.getName().equals("execute"))
                    .mapToLong(timing -> timing.getStartedAt().toEpochMilli())
                    .min()
                    .ifPresent(value -> {
                        histories.add(new State.History(
                            State.Type.RUNNING,
                            Instant.ofEpochMilli(value)
                        ));
                    });

                r.getTiming()
                    .stream()
                    .mapToLong(timing -> timing.getCompletedAt().toEpochMilli())
                    .max()
                    .ifPresent(value -> {
                        histories.add(new State.History(
                            r.state(),
                            Instant.ofEpochMilli(value)
                        ));
                    });

                State state = State.of(
                    r.state(),
                    histories
                );

                r.getAdapterResponse()
                    .entrySet()
                    .stream()
                    .map(e -> {
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
                    .value(runContext.render("{{ taskrun.id }}"))
                    .executionId(runContext.render("{{ execution.id }}"))
                    .parentTaskRunId(runContext.render("{{ taskrun.id }}"))
                    .state(state)
                    .attempts(List.of(TaskRunAttempt.builder()
                        .state(state)
                        .build()
                    ));
                if (assets != null) {
                    taskRunBuilder.assets(assets);
                }

                return WorkerTaskResult.builder()
                    .taskRun(taskRunBuilder.build())
                    .build();
            }))
            .toList();

        runContext.dynamicWorkerResult(workerTaskResults);

        return runContext.storage().putFile(file);
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

        return new AssetsInOut(
            inputs,
            List.of(Custom.builder()
                .id(modelAsset.assetId())
                .type(TABLE_ASSET_TYPE)
                .metadata(modelAsset.metadata())
                .build()
            )
        );
    }

    private static void emitAssets(RunContext runContext, Manifest manifest) throws IllegalVariableEvaluationException {
        Map<String, ModelAsset> modelAssets = extractModelAssets(manifest);
        runContext.logger().info("dbt assets extracted from manifest: {}", modelAssets.size());

        for (ModelAsset asset : modelAssets.values()) {
            List<AssetIdentifier> inputs = inputIdentifiers(asset, modelAssets);
            try {
                runContext.assets().emit(new AssetEmit(
                    inputs,
                    List.of(Custom.builder()
                        .id(asset.assetId())
                        .type(TABLE_ASSET_TYPE)
                        .metadata(asset.metadata())
                        .build()
                    )
                )
                );
            } catch (UnsupportedOperationException | QueueException e) {
                // UnsupportedOperationException for OSS or tests where EE is not configured (assets are EE only)
                runContext.logger().warn("Unable to emit dbt asset '{}'", asset.assetId(), e);
            }
        }
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
            if (hasValue(system)) metadata.put("system", system);
            if (hasValue(node.getDatabase())) metadata.put("database", node.getDatabase());
            if (hasValue(node.getSchema())) metadata.put("schema", node.getSchema());
            if (hasValue(name)) metadata.put("name", name);

            List<String> dependsOn = List.of();
            if (node.getDependsOn() != null) {
                dependsOn = node.getDependsOn().getOrDefault("nodes", List.of());
            }

            modelAssets.put(uniqueId, new ModelAsset(assetId, metadata, dependsOn));
        }

        Map<String, ModelAsset> filtered = new HashMap<>(modelAssets.size());
        for (Map.Entry<String, ModelAsset> e : modelAssets.entrySet()) {
            ModelAsset a = e.getValue();
            List<String> deps = a.dependsOn() == null ? List.of() : a.dependsOn().stream()
                .filter(modelAssets::containsKey)
                .toList();
            filtered.put(e.getKey(), new ModelAsset(a.assetId(), a.metadata(), deps));
        }

        return filtered;
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

    private record ModelAsset(String assetId, Map<String, Object> metadata, List<String> dependsOn) {
    }
}
