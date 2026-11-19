package io.kestra.plugin.dbt.assets;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.assets.Asset;
import io.kestra.core.serializers.JacksonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseLocatorTest {
    @Test
    @SuppressWarnings("unchecked")
    void shouldBuildTheIdsOfTheSharedVectors() throws IOException {
        Map<String, Object> vectors;
        try (InputStream file = DatabaseLocatorTest.class.getResourceAsStream("/locator/vectors.json")) {
            vectors = JacksonMapper.ofJson().readValue(file, Map.class);
        }

        for (Map<String, String> vector : (List<Map<String, String>>) vectors.get("databases")) {
            Optional<String> id = DatabaseLocator.id(vector.get("system"), vector.get("host"), vector.get("port"), vector.get("database"));
            String fullId = vector.get("fullId");
            if (fullId == null) {
                assertThat(id).as("%s", vector).isEmpty();
                continue;
            }
            assertThat(id).as("%s", vector).contains(vector.getOrDefault("id", fullId));
            assertThat(Asset.normalizeId(fullId)).as("%s", vector).isEqualTo(vector.getOrDefault("id", fullId));
        }
        for (Map<String, String> vector : (List<Map<String, String>>) vectors.get("tables")) {
            assertThat(DatabaseLocator.tableId(vector.get("database"), vector.get("schema"), vector.get("table"))).as("%s", vector).isEqualTo(vector.get("fullId"));
        }
    }
}
