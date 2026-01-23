package io.kestra.plugin.dbt;

import io.kestra.core.assets.AssetManagerFactory;
import io.kestra.core.models.assets.Asset;
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
        private final List<Asset> assets = new ArrayList<>();

        @Override
        public void upsert(Asset asset) {
            assets.add(asset);
        }

        @Override
        public List<Asset> outputs() {
            return List.copyOf(assets);
        }
    }
}
