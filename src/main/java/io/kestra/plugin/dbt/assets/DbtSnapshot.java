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
    title = "dbt snapshot",
    description = """
        A table where dbt keeps the history of another table with `dbt snapshot`."""
)
public class DbtSnapshot extends Asset {
    public static final String ASSET_TYPE = DbtSnapshot.class.getName();

    @Builder
    @JsonCreator
    public DbtSnapshot(
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
