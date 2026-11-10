package io.kestra.plugin.dbt;

import java.io.FileNotFoundException;
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

/** Serves one dbt node's compiled SQL from the stored `manifest.json`, so the UI never downloads the whole manifest. */
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
        var uniqueId = context.param("uniqueId");
        if (uniqueId == null || uniqueId.isBlank()) {
            return EndpointError.of("missing_unique_id", "Pass the dbt node to read as `uniqueId`, e.g. `model.shop.orders`.");
        }

        var outputs = context.outputs().get();
        if (!Boolean.TRUE.equals(outputs.get("compiledSqlEnabled"))) {
            return EndpointError.of("disabled", "Compiled SQL is off for this task. Set `outputCompiledSql: true` to show it.");
        }

        var manifest = manifestUri(outputs);
        if (manifest == null) {
            return EndpointError.of("no_manifest", "This task run stored no manifest.json.");
        }

        String sql;
        try (var in = context.storage().getFile(manifest)) {
            sql = compiledSql(in, uniqueId);
        } catch (FileNotFoundException e) {
            // The output can outlive its file, for example after a storage purge.
            return EndpointError.of("no_manifest", "The stored manifest.json no longer exists.");
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read the stored dbt manifest", e);
        }
        if (sql == null) {
            return EndpointError.of("not_found", "No compiled SQL for `%s` in this run's manifest.".formatted(uniqueId));
        }

        var truncated = sql.length() > MAX_SQL_CHARS;
        return PluginEndpointResponse.of(
            Map.of(
                "uniqueId", uniqueId,
                "compiledSql", truncated ? truncate(sql) : sql,
                "truncated", truncated
            )
        );
    }

    // DbtCLI keeps the manifest under outputFiles, the dbt Cloud tasks under `manifest`.
    private static URI manifestUri(Map<String, Object> outputs) {
        var uri = outputs.get("manifest");
        if (uri == null && outputs.get("outputFiles") instanceof Map<?, ?> files) {
            uri = files.get("manifest.json");
        }
        return uri == null ? null : URI.create(uri.toString());
    }

    // Never cuts between the two halves of a surrogate pair, which would leave an invalid character.
    private static String truncate(String sql) {
        var end = Character.isHighSurrogate(sql.charAt(MAX_SQL_CHARS - 1)) ? MAX_SQL_CHARS - 1 : MAX_SQL_CHARS;
        return sql.substring(0, end);
    }

    /** Streams the manifest and keeps only `nodes[uniqueId].compiled_code`, so a large manifest is never held in memory. */
    static String compiledSql(InputStream manifest, String uniqueId) throws IOException {
        try (var parser = ResultParser.MAPPER.getFactory().createParser(manifest)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                var field = parser.currentName();
                parser.nextToken();
                if (!"nodes".equals(field) || parser.currentToken() != JsonToken.START_OBJECT) {
                    parser.skipChildren();
                    continue;
                }
                while (parser.nextToken() == JsonToken.FIELD_NAME) {
                    var id = parser.currentName();
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
            var field = parser.currentName();
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
}
