package io.kestra.plugin.dbt.assets;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.models.assets.Asset;

/**
 * Builds the id of a Database asset, {@code <system>:<host>[:<port>]:<database>}. The Terraform and OpenTofu plugins
 * and the Terraform state import of the Deployments page build the same id with the same rules and the same test
 * vectors ({@code locator/vectors.json}), which is what makes the tables dbt builds land under the database Terraform
 * provisioned: the host in lower case without a trailing dot, the port left out when it is the engine's default, one
 * prefix per engine.
 */
public final class DatabaseLocator {
    private static final Map<String, String> PREFIXES = Map.ofEntries(
        Map.entry("postgres", "postgres"),
        Map.entry("postgresql", "postgres"),
        Map.entry("aurora-postgresql", "postgres"),
        Map.entry("mysql", "mysql"),
        Map.entry("aurora", "mysql"),
        Map.entry("aurora-mysql", "mysql"),
        Map.entry("mariadb", "mysql"),
        Map.entry("sqlserver", "sqlserver"),
        Map.entry("sqlserver-ee", "sqlserver"),
        Map.entry("sqlserver-se", "sqlserver"),
        Map.entry("sqlserver-ex", "sqlserver"),
        Map.entry("sqlserver-web", "sqlserver"),
        Map.entry("redshift", "redshift"),
        Map.entry("snowflake", "snowflake"),
        Map.entry("bigquery", "bigquery")
    );

    private static final Map<String, String> DEFAULT_PORTS = Map.of(
        "postgres", "5432",
        "mysql", "3306",
        "sqlserver", "1433",
        "redshift", "5439"
    );

    private DatabaseLocator() {
    }

    /**
     * @return the engine prefix of {@code system}: an alias of a known engine is folded into it, any other engine is
     *         kept in lower case
     */
    public static String prefix(String system) {
        String engine = system.trim().toLowerCase(Locale.ROOT);
        return PREFIXES.getOrDefault(engine, engine);
    }

    /**
     * @param host the server host; the account for Snowflake and the project for BigQuery
     * @param port {@code null} or blank when unknown
     * @param database {@code null} for BigQuery, whose datasets are the schemas
     * @return the id, empty when a value the engine needs is missing
     */
    public static Optional<String> id(String system, String host, String port, String database) {
        if (isBlank(system) || isBlank(host)) {
            return Optional.empty();
        }
        String prefix = prefix(system);
        String normalizedHost = host.trim().toLowerCase(Locale.ROOT).replaceAll("\\.+$", "");
        if ("bigquery".equals(prefix)) {
            return Optional.of(normalize(prefix + ":" + normalizedHost));
        }
        if (isBlank(database)) {
            return Optional.empty();
        }
        String normalizedPort = isBlank(port) || port.trim().equals(DEFAULT_PORTS.get(prefix)) || "snowflake".equals(prefix) ? null : port.trim();
        return Optional.of(normalize(prefix + ":" + normalizedHost + (normalizedPort == null ? "" : ":" + normalizedPort) + ":" + database.trim()));
    }

    /**
     * @return the id of a table of the database {@code databaseId}, {@code <database id>.<schema>.<table>}
     */
    public static String tableId(String databaseId, String schema, String table) {
        return normalize(databaseId + "." + schema + "." + table);
    }

    /**
     * The id before {@link Asset#normalizeId} shortens it, as the Deployments page builds it: the server shortens it the
     * same way when it is written.
     */
    static String slugify(String id) {
        return id.replaceAll("[^a-zA-Z0-9._:/-]+", "_")
            .replaceAll("_{2,}", "_")
            .replaceAll("^[^a-zA-Z0-9]+", "")
            .replaceAll("[^a-zA-Z0-9]+$", "");
    }

    private static String normalize(String id) {
        return Asset.normalizeId(slugify(id));
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
