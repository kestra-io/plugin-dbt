package io.kestra.plugin.dbt;

import java.util.Map;

import io.kestra.core.plugins.endpoint.PluginEndpointResponse;

// PluginEndpointResponse carries no status code, so refusals come back as a 200 with an `error` code.
final class EndpointError {
    private EndpointError() {
    }

    static PluginEndpointResponse of(String code, String message) {
        return PluginEndpointResponse.of(Map.of("error", code, "message", message));
    }
}
