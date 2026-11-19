package io.kestra.plugin.dbt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.kestra.core.assets.AssetEmitContext;
import io.kestra.core.assets.AssetManagerFactory;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.AssetEmitter;
import io.kestra.core.runners.RunContext;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Singleton;

@Singleton
@Replaces(AssetManagerFactory.class)
public class TestAssetManagerFactory extends AssetManagerFactory {
    private final List<AssetEmit> allEmitted = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, List<AssetEmit>> allEmittedFor = new ConcurrentHashMap<>();

    @Override
    public AssetEmitter of(RunContext runContext, @Nullable AssetEmitContext context, boolean enabled) {
        return new TrackingAssetEmitter(allEmitted, allEmittedFor);
    }

    /** All assets emitted across all RunContexts (for runner/integration tests). */
    public List<AssetEmit> allEmitted() {
        return List.copyOf(allEmitted);
    }

    /** The emits sent on behalf of a dynamic task run, empty when none was. */
    public List<AssetEmit> emittedFor(String dynamicTaskRunId) {
        return List.copyOf(allEmittedFor.getOrDefault(dynamicTaskRunId, List.of()));
    }

    /** Every emit sent on behalf of a dynamic task run. */
    public List<AssetEmit> allEmittedFor() {
        return allEmittedFor.values().stream().flatMap(List::stream).toList();
    }

    public void clear() {
        allEmitted.clear();
        allEmittedFor.clear();
    }

    private static final class TrackingAssetEmitter implements AssetEmitter {
        private final List<AssetEmit> shared;
        private final Map<String, List<AssetEmit>> sharedFor;
        private final List<AssetEmit> local = new ArrayList<>();

        TrackingAssetEmitter(List<AssetEmit> shared, Map<String, List<AssetEmit>> sharedFor) {
            this.shared = shared;
            this.sharedFor = sharedFor;
        }

        @Override
        public void emit(AssetEmit assetEmit) {
            local.add(assetEmit);
            shared.add(assetEmit);
        }

        @Override
        public void emitFor(TaskRun dynamicTaskRun, AssetEmit assetEmit) {
            sharedFor.computeIfAbsent(dynamicTaskRun.getId(), id -> Collections.synchronizedList(new ArrayList<>())).add(assetEmit);
        }

        @Override
        public List<AssetEmit> emitted() {
            return List.copyOf(local);
        }
    }
}
