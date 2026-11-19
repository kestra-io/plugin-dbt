package io.kestra.plugin.dbt.assets;

import java.util.Optional;

import io.kestra.core.models.assets.Asset;

import jakarta.annotation.Nullable;

/**
 * The Database the tables of a run belong to: the one the task names, or the one its profile connects to. A model built
 * in another database than the profile's, as on Snowflake and BigQuery, belongs to that database on the same server.
 *
 * @param databaseAsset the id the task names, which wins over the profile
 * @param target where the profile connects, {@code null} when unknown
 */
public record TableScope(@Nullable String databaseAsset, @Nullable ProfileLocator.Target target) {
    public static final TableScope NONE = new TableScope(null, null);

    public Optional<String> databaseId(@Nullable String nodeDatabase) {
        if (databaseAsset != null && !databaseAsset.isBlank()) {
            return Optional.of(Asset.normalizeId(databaseAsset.trim()));
        }
        if (target == null) {
            return Optional.empty();
        }
        boolean hasNodeDatabase = nodeDatabase != null && !nodeDatabase.isBlank();
        if ("bigquery".equals(DatabaseLocator.prefix(target.system()))) {
            return DatabaseLocator.id(target.system(), hasNodeDatabase ? nodeDatabase : target.host(), null, null);
        }
        return DatabaseLocator.id(target.system(), target.host(), target.port(), hasNodeDatabase ? nodeDatabase : target.database());
    }
}
