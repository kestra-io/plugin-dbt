package io.kestra.plugin.dbt;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.plugins.endpoint.PluginEndpoint;
import io.kestra.core.plugins.endpoint.PluginEndpointContext;
import io.kestra.core.plugins.endpoint.PluginEndpointResponse;

/**
 * Serves the compiled SQL of one dbt node from the task run's stored `manifest.json`, so the Show details
 * drawer never downloads the whole manifest. The response cannot carry a status code, so refusals come back
 * as {@code {"error": ...}}.
 */
@Plugin
public class CompiledSqlEndpoint implements PluginEndpoint {
    static final String NAME = "compiled-sql";
    static final int MAX_SQL_CHARS = 200_000;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public PluginEndpointResponse handle(PluginEndpointContext context) {
        String uniqueId = context.param("uniqueId");
        if (uniqueId == null || uniqueId.isBlank()) {
            return error("missing_unique_id", "Pass the dbt node to read as `uniqueId`, e.g. `model.shop.orders`.");
        }

        Map<String, Object> outputs = context.outputs().get();
        // Compiled SQL can embed literal `--vars` values, so it is only served when the task opted in.
        if (!Boolean.TRUE.equals(outputs.get("compiledSqlEnabled"))) {
            return error("disabled", "Compiled SQL is off for this task. Set `outputCompiledSql: true` to show it.");
        }

        URI manifest = manifestUri(outputs);
        if (manifest == null) {
            return error("no_manifest", "This task run stored no manifest.json.");
        }

        String sql;
        try (InputStream in = context.storage().getFile(manifest)) {
            sql = compiledSql(in, uniqueId);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read the stored dbt manifest", e);
        }
        if (sql == null) {
            return error("not_found", "No compiled SQL for `%s` in this run's manifest.".formatted(uniqueId));
        }

        boolean truncated = sql.length() > MAX_SQL_CHARS;
        return PluginEndpointResponse.of(
            Map.of(
                "uniqueId", uniqueId,
                "compiledSql", truncated ? sql.substring(0, MAX_SQL_CHARS) : sql,
                "truncated", truncated
            )
        );
    }

    // DbtCLI keeps the manifest under outputFiles, the dbt Cloud tasks under `manifest`.
    private static URI manifestUri(Map<String, Object> outputs) {
        Object uri = outputs.get("manifest");
        if (uri == null && outputs.get("outputFiles") instanceof Map<?, ?> files) {
            uri = files.get("manifest.json");
        }
        return uri == null ? null : URI.create(uri.toString());
    }

    /** Streams the manifest and keeps only `nodes[uniqueId].compiled_code`, so a large manifest is never held in memory. */
    static String compiledSql(InputStream manifest, String uniqueId) throws IOException {
        try (JsonParser parser = ResultParser.MAPPER.getFactory().createParser(manifest)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String field = parser.currentName();
                parser.nextToken();
                if (!"nodes".equals(field) || parser.currentToken() != JsonToken.START_OBJECT) {
                    parser.skipChildren();
                    continue;
                }
                while (parser.nextToken() == JsonToken.FIELD_NAME) {
                    String id = parser.currentName();
                    parser.nextToken();
                    if (!uniqueId.equals(id)) {
                        parser.skipChildren();
                        continue;
                    }
                    return nodeSql(parser);
                }
                return null;
            }
            return null;
        }
    }

    private static String nodeSql(JsonParser parser) throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            return null;
        }
        String legacy = null;
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String field = parser.currentName();
            parser.nextToken();
            // dbt 1.3 renamed compiled_sql to compiled_code
            if ("compiled_code".equals(field) && parser.currentToken() == JsonToken.VALUE_STRING) {
                return parser.getText();
            }
            if ("compiled_sql".equals(field) && parser.currentToken() == JsonToken.VALUE_STRING) {
                legacy = parser.getText();
            } else {
                parser.skipChildren();
            }
        }
        return legacy;
    }

    private static PluginEndpointResponse error(String code, String message) {
        return PluginEndpointResponse.of(Map.of("error", code, "message", message));
    }
}
