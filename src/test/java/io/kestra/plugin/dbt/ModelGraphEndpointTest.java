package io.kestra.plugin.dbt;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.plugins.endpoint.DefaultPluginEndpointContext;
import io.kestra.core.plugins.endpoint.PluginEndpointResponse;
import io.kestra.core.serializers.JacksonMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class ModelGraphEndpointTest {
    private static final URI GRAPH = URI.create("kestra:///shop/flow/executions/e1/tasks/dbt/t1/graph.json");
    private static final String GRAPH_JSON = "{\"nodes\":[{\"uniqueId\":\"model.shop.orders\",\"status\":\"success\",\"level\":0}],\"edges\":[]}";

    private static PluginEndpointResponse call(Map<String, Object> outputs, String storedJson) {
        var context = new DefaultPluginEndpointContext(
            Map.of(),
            Map.of(),
            "e1",
            "t1",
            level -> List.<LogEntry> of(),
            () -> outputs,
            EndpointTestStorage.singleFile(GRAPH, storedJson)
        );
        return new ModelGraphEndpoint().handle(context);
    }

    private static Object error(PluginEndpointResponse response) throws Exception {
        assertThat(response.contentType(), is("application/json"));
        return JacksonMapper.ofJson().readValue(response.body(), Map.class).get("error");
    }

    @Test
    void servesTheStoredGraphAsJson() {
        var response = call(Map.of("graph", GRAPH.toString()), GRAPH_JSON);

        assertThat(response.contentType(), is("application/json"));
        assertThat(new String(response.body(), StandardCharsets.UTF_8), is(GRAPH_JSON));
    }

    @Test
    void reportsARunWithoutGraph() throws Exception {
        assertThat(error(call(Map.of(), GRAPH_JSON)), is("no_graph"));
    }

    @Test
    void reportsAGraphOutputWhoseFileIsGone() throws Exception {
        var purged = "kestra:///shop/flow/executions/e1/tasks/dbt/t1/purged.json";

        assertThat(error(call(Map.of("graph", purged), GRAPH_JSON)), is("no_graph"));
    }

    @Test
    void refusesAGraphOverTheSizeCap() throws Exception {
        var huge = "x".repeat(ModelGraphEndpoint.MAX_GRAPH_BYTES + 1);

        assertThat(error(call(Map.of("graph", GRAPH.toString()), huge)), is("too_large"));
    }
}
