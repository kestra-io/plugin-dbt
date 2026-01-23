package io.kestra.plugin.dbt;

import io.kestra.core.assets.AssetManagerFactory;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.AssetEmitter;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.List;

@Singleton
@Replaces(AssetManagerFactory.class)
class TestAssetManagerFactory extends AssetManagerFactory {
    @Override
    public AssetEmitter of(boolean enable) {
        return new InMemoryAssetEmitter();
    }

    private static final class InMemoryAssetEmitter implements AssetEmitter {
        private final List<AssetEmit> assets = new ArrayList<>();

        @Override
        public void emit(AssetEmit assetEmit) {
            assets.add(assetEmit);
        }

        @Override
        public List<AssetEmit> emitted() {
            return List.copyOf(assets);
        }
    }
}
