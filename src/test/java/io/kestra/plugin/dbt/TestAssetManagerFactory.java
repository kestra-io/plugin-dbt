package io.kestra.plugin.dbt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.kestra.core.assets.AssetManagerFactory;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.AssetEmitter;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

@Singleton
@Replaces(AssetManagerFactory.class)
public class TestAssetManagerFactory extends AssetManagerFactory {
    private final List<AssetEmit> allEmitted = Collections.synchronizedList(new ArrayList<>());

    /**
     * Off by default: most tests never set `assets.enableAuto` and rely on emission working anyway. Turn it
     * on to exercise what production does when emission is disabled, which is to drop every emit silently.
     */
    private volatile boolean honourEnable = false;

    public void honourEnable(boolean honourEnable) {
        this.honourEnable = honourEnable;
    }

    @Override
    public AssetEmitter of(boolean enable) {
        return new TrackingAssetEmitter(allEmitted, !honourEnable || enable);
    }

    /** All assets emitted across all RunContexts (for runner/integration tests). */
    public List<AssetEmit> allEmitted() {
        return List.copyOf(allEmitted);
    }

    public void clear() {
        allEmitted.clear();
    }

    private static final class TrackingAssetEmitter implements AssetEmitter {
        private final List<AssetEmit> shared;
        private final List<AssetEmit> local = new ArrayList<>();
        private final boolean enabled;

        TrackingAssetEmitter(List<AssetEmit> shared, boolean enabled) {
            this.shared = shared;
            this.enabled = enabled;
        }

        @Override
        public void emit(AssetEmit assetEmit) {
            // Mirrors EE's InMemoryAssetEmitter: disabled drops silently, it does not throw.
            if (!enabled) {
                return;
            }
            local.add(assetEmit);
            shared.add(assetEmit);
        }

        @Override
        public List<AssetEmit> emitted() {
            return List.copyOf(local);
        }
    }
}
