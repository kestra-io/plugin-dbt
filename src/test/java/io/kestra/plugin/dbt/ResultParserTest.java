package io.kestra.plugin.dbt;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.dbt.cli.DbtCLI;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class ResultParserTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void parseManifestWithAssets_shouldEmitModelAssets() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        Files.writeString(manifestFile, """
            {
              "metadata": {
                "adapter_type": "postgres"
              },
              "nodes": {
                "model.analytics.stg_orders": {
                  "resource_type": "model",
                  "database": "analytics",
                  "schema": "staging",
                  "name": "stg_orders",
                  "unique_id": "model.analytics.stg_orders",
                  "depends_on": {
                    "nodes": []
                  }
                },
                "model.analytics.fct_orders": {
                  "resource_type": "model",
                  "database": "analytics",
                  "schema": "marts",
                  "name": "fct_orders",
                  "unique_id": "model.analytics.fct_orders",
                  "depends_on": {
                    "nodes": [
                      "model.analytics.stg_orders"
                    ]
                  }
                }
              }
            }
            """);

        var manifestResult = ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        assertThat(manifestResult.manifest(), is(notNullValue()));
        assertThat(runContext.assets().emitted(), hasSize(2));

        var outputAssets = runContext.assets().emitted().stream()
            .flatMap(assetEmit -> assetEmit.outputs().stream())
            .toList();

        assertThat(outputAssets, hasSize(2));
        var byId = new HashMap<String, io.kestra.core.models.assets.Asset>();
        outputAssets.forEach(asset -> byId.put(asset.getId(), asset));

        assertThat(byId.containsKey("analytics.staging.stg_orders"), is(true));
        assertThat(byId.containsKey("analytics.marts.fct_orders"), is(true));

        var stgOrders = byId.get("analytics.staging.stg_orders");
        assertThat(stgOrders.getMetadata().get("system"), is("postgres"));
        assertThat(stgOrders.getMetadata().get("database"), is("analytics"));
        assertThat(stgOrders.getMetadata().get("schema"), is("staging"));
        assertThat(stgOrders.getMetadata().get("name"), is("stg_orders"));
    }

    private RunContext mockRunContext() {
        ensureFactorySecretKey();

        var task = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .commands(Property.ofValue(List.of("dbt run")))
            .build();

        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of(), null);
        var taskRun = TestsUtils.mockTaskRun(execution, task);
        var runContext = runContextFactory.of(flow, task, execution, taskRun, false);

        ensureSecretKey(runContext);
        return runContext;
    }

    private void ensureFactorySecretKey() {
        try {
            Class<?> type = runContextFactory.getClass();
            java.lang.reflect.Field field = null;
            while (type != null && field == null) {
                try {
                    field = type.getDeclaredField("secretKey");
                } catch (NoSuchFieldException e) {
                    type = type.getSuperclass();
                }
            }
            if (field == null) {
                return;
            }
            field.setAccessible(true);
            if (field.get(runContextFactory) == null) {
                field.set(runContextFactory, Optional.empty());
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Failed to initialize run context factory secret key", e);
        }
    }

    private void ensureSecretKey(RunContext runContext) {
        try {
            Class<?> type = runContext.getClass();
            java.lang.reflect.Field field = null;
            while (type != null && field == null) {
                try {
                    field = type.getDeclaredField("secretKey");
                } catch (NoSuchFieldException e) {
                    type = type.getSuperclass();
                }
            }
            if (field == null) {
                return;
            }
            field.setAccessible(true);
            if (field.get(runContext) == null) {
                field.set(runContext, Optional.empty());
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Failed to initialize run context secret key", e);
        }
    }
}
