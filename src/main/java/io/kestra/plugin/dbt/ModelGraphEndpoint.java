package io.kestra.plugin.dbt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.plugins.endpoint.PluginEndpoint;
import io.kestra.core.plugins.endpoint.PluginEndpointContext;
import io.kestra.core.plugins.endpoint.PluginEndpointResponse;

/** Serves the stored graph.json so the plugin UI never calls the Kestra API itself (kestra-ee#10486). */
@Plugin
public class ModelGraphEndpoint implements PluginEndpoint {
    static final String NAME = "model-graph";
    // The whole graph is held in webserver memory for each request, so the read is capped.
    static final int MAX_GRAPH_BYTES = 10 * 1024 * 1024;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public PluginEndpointResponse handle(PluginEndpointContext context) {
        var graph = context.outputs().get().get("graph");
        if (graph == null) {
            return EndpointError.of("no_graph", "This task run stored no model graph.");
        }

        try (var in = context.storage().getFile(URI.create(graph.toString()))) {
            var bytes = in.readNBytes(MAX_GRAPH_BYTES + 1);
            if (bytes.length > MAX_GRAPH_BYTES) {
                return EndpointError.of("too_large", "The model graph is larger than %d MB.".formatted(MAX_GRAPH_BYTES / 1024 / 1024));
            }
            return PluginEndpointResponse.ofBytes(bytes, "application/json");
        } catch (FileNotFoundException e) {
            // The output can outlive its file, for example after a storage purge.
            return EndpointError.of("no_graph", "The stored model graph no longer exists.");
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read the stored dbt model graph", e);
        }
    }
}
