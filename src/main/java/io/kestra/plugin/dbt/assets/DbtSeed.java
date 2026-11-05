package io.kestra.plugin.dbt.assets;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.assets.Asset;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Plugin
@Schema(
    title = "dbt seed",
    description = """
        A CSV file dbt loads into a table with `dbt seed`."""
)
public class DbtSeed extends Asset {
    public static final String ASSET_TYPE = DbtSeed.class.getName();

    @Builder
    @JsonCreator
    public DbtSeed(
        String tenantId,
        String namespace,
        String id,
        String displayName,
        String description,
        Map<String, Object> metadata,
        Instant created,
        Instant updated,
        boolean deleted) {
        super(tenantId, namespace, id, ASSET_TYPE, displayName, description, metadata, created, updated, deleted);
    }
}
