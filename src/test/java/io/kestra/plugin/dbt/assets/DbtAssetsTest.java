package io.kestra.plugin.dbt.assets;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.Asset;
import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class DbtAssetsTest {
    // Kestra reads stored assets back through the plugin registry: an unregistered type would come back as Custom.
    @ParameterizedTest
    @ValueSource(classes = { DbtModel.class, DbtSeed.class, DbtSnapshot.class, DbtTest.class, DbtExposure.class })
    void readsBackAsItsOwnClassThroughTheRegistry(Class<? extends Asset> type) throws Exception {
        var json = "{\"id\": \"analytics.marts.fct_orders\", \"type\": \"%s\"}".formatted(type.getName());

        var asset = JacksonMapper.ofJson().readValue(json, Asset.class);

        assertThat(asset.getClass(), is(type));
        assertThat(asset.getType(), is(type.getName()));
    }
}
