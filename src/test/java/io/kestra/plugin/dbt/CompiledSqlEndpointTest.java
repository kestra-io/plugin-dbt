package io.kestra.plugin.dbt;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.plugins.endpoint.DefaultPluginEndpointContext;
import io.kestra.core.plugins.endpoint.PluginEndpointResponse;
import io.kestra.core.plugins.endpoint.ScopedStorage;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.FileAttributes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class CompiledSqlEndpointTest {
    private static final URI MANIFEST = URI.create("kestra:///shop/flow/executions/e1/tasks/dbt/t1/manifest.json");

    private static final String MANIFEST_JSON = """
        {
          "metadata": {"dbt_version": "1.8.0", "nested": {"nodes": {"model.shop.orders": {"compiled_code": "decoy"}}}},
          "nodes": {
            "model.shop.customers": {"compiled_code": "select 1", "config": {"materialized": "view"}},
            "model.shop.orders": {"depends_on": {"nodes": ["model.shop.customers"]}, "compiled_code": "select * from analytics.customers"},
            "model.shop.legacy": {"compiled_sql": "select 'old'"},
            "model.shop.ephemeral": {"compiled": false}
          },
          "sources": {}
        }
        """;

    private static Map<?, ?> call(Map<String, Object> outputs, String uniqueId, String manifestJson) throws Exception {
        ScopedStorage storage = new ScopedStorage() {
            @Override
            public InputStream getFile(URI uri) throws FileNotFoundException {
                if (!uri.equals(MANIFEST)) {
                    throw new FileNotFoundException(uri.toString());
                }
                return new ByteArrayInputStream(manifestJson.getBytes(StandardCharsets.UTF_8));
            }

            @Override
            public List<FileAttributes> list(URI uri) {
                return List.of();
            }

            @Override
            public boolean exists(URI uri) {
                return uri.equals(MANIFEST);
            }

            @Override
            public URI putFile(URI uri, InputStream data) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean deleteFile(URI uri) {
                throw new UnsupportedOperationException();
            }
        };
        var context = new DefaultPluginEndpointContext(
            uniqueId == null ? Map.of() : Map.of("uniqueId", List.of(uniqueId)),
            Map.of(),
            "e1",
            "t1",
            level -> List.<LogEntry> of(),
            () -> outputs,
            storage
        );

        PluginEndpointResponse response = new CompiledSqlEndpoint().handle(context);
        assertThat(response.contentType(), is("application/json"));
        return JacksonMapper.ofJson().readValue(response.body(), Map.class);
    }

    private static Map<String, Object> cliOutputs(boolean enabled) {
        return Map.of("compiledSqlEnabled", enabled, "outputFiles", Map.of("manifest.json", MANIFEST.toString()));
    }

    @Test
    void returnsTheCompiledSqlOfTheRequestedNodeOnly() throws Exception {
        var body = call(cliOutputs(true), "model.shop.orders", MANIFEST_JSON);

        assertThat(body.get("uniqueId"), is("model.shop.orders"));
        assertThat(body.get("compiledSql"), is("select * from analytics.customers"));
        assertThat(body.get("truncated"), is(false));
    }

    @Test
    void readsPre13CompiledSqlAndTheDbtCloudManifestOutput() throws Exception {
        var body = call(Map.of("compiledSqlEnabled", true, "manifest", MANIFEST.toString()), "model.shop.legacy", MANIFEST_JSON);

        assertThat(body.get("compiledSql"), is("select 'old'"));
    }

    @Test
    void refusesWhenCompiledSqlIsOff() throws Exception {
        assertThat(call(cliOutputs(false), "model.shop.orders", MANIFEST_JSON).get("error"), is("disabled"));
        assertThat(call(Map.of("outputFiles", Map.of("manifest.json", MANIFEST.toString())), "model.shop.orders", MANIFEST_JSON).get("error"), is("disabled"));
    }

    @Test
    void reportsUnknownOrUncompiledNodes() throws Exception {
        assertThat(call(cliOutputs(true), "model.shop.missing", MANIFEST_JSON).get("error"), is("not_found"));
        assertThat(call(cliOutputs(true), "model.shop.ephemeral", MANIFEST_JSON).get("error"), is("not_found"));
        assertThat(call(cliOutputs(true), null, MANIFEST_JSON).get("error"), is("missing_unique_id"));
        assertThat(call(Map.of("compiledSqlEnabled", true), "model.shop.orders", MANIFEST_JSON).get("error"), is("no_manifest"));
    }

    @Test
    void capsVeryLargeSql() throws Exception {
        String sql = "x".repeat(CompiledSqlEndpoint.MAX_SQL_CHARS + 10);
        String manifest = "{\"nodes\": {\"model.shop.big\": {\"compiled_code\": \"" + sql + "\"}}}";

        var body = call(cliOutputs(true), "model.shop.big", manifest);

        assertThat(((String) body.get("compiledSql")).length(), is(CompiledSqlEndpoint.MAX_SQL_CHARS));
        assertThat(body.get("truncated"), is(true));
    }
}
