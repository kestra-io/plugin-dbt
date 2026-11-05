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
    title = "dbt model",
    description = """
        A model dbt builds into a table or view."""
)
public class DbtModel extends Asset {
    public static final String ASSET_TYPE = DbtModel.class.getName();

    @Builder
    @JsonCreator
    public DbtModel(
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
