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

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
              },
              "parent_map": {
                "model.analytics.stg_orders": [],
                "model.analytics.fct_orders": [
                  "model.analytics.stg_orders"
                ]
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

    @Test
    void parseManifestWithAssets_shouldEmitLineageInputs() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        Files.writeString(manifestFile, """
            {
              "metadata": {
                "adapter_type": "postgres"
              },
              "nodes": {
                "model.analytics.my_first_dbt_model": {
                  "resource_type": "model",
                  "database": "analytics",
                  "schema": "marts",
                  "name": "my_first_dbt_model",
                  "unique_id": "model.analytics.my_first_dbt_model",
                  "depends_on": {
                    "nodes": []
                  }
                },
                "model.analytics.my_second_dbt_model": {
                  "resource_type": "model",
                  "database": "analytics",
                  "schema": "marts",
                  "name": "my_second_dbt_model",
                  "unique_id": "model.analytics.my_second_dbt_model",
                  "depends_on": {
                    "nodes": [
                      "model.analytics.my_first_dbt_model"
                    ]
                  }
                }
              },
              "parent_map": {
                "model.analytics.my_first_dbt_model": [],
                "model.analytics.my_second_dbt_model": [
                  "model.analytics.my_first_dbt_model"
                ]
              }
            }
            """);

        ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        var emittedByOutputId = runContext.assets().emitted().stream()
            .collect(toMap(
                assetEmit -> assetEmit.outputs().getFirst().getId(),
                assetEmit -> assetEmit
            ));

        assertThat(emittedByOutputId, hasKey("analytics.marts.my_first_dbt_model"));
        assertThat(emittedByOutputId, hasKey("analytics.marts.my_second_dbt_model"));

        var secondModelEmit = emittedByOutputId.get("analytics.marts.my_second_dbt_model");
        assertThat(secondModelEmit.inputs(), hasSize(1));
        assertThat(secondModelEmit.inputs().getFirst().id(), is("analytics.marts.my_first_dbt_model"));
    }

    @Test
    void parseManifestWithAssets_shouldUseParentMapForLineage() throws Exception {
        // Simulate a case where depends_on.nodes includes transitive deps
        // but parent_map only has the direct edges (the real DAG).
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        Files.writeString(manifestFile, """
            {
              "metadata": {
                "adapter_type": "duckdb"
              },
              "nodes": {
                "model.project.stg_orders": {
                  "resource_type": "model",
                  "database": "dev",
                  "schema": "staging",
                  "name": "stg_orders",
                  "unique_id": "model.project.stg_orders",
                  "depends_on": {
                    "nodes": ["source.project.raw.orders"]
                  }
                },
                "model.project.int_orders": {
                  "resource_type": "model",
                  "database": "dev",
                  "schema": "intermediate",
                  "name": "int_orders",
                  "unique_id": "model.project.int_orders",
                  "depends_on": {
                    "nodes": ["model.project.stg_orders"]
                  }
                },
                "model.project.fct_orders": {
                  "resource_type": "model",
                  "database": "dev",
                  "schema": "marts",
                  "name": "fct_orders",
                  "unique_id": "model.project.fct_orders",
                  "depends_on": {
                    "nodes": ["model.project.stg_orders", "model.project.int_orders"]
                  }
                }
              },
              "parent_map": {
                "model.project.stg_orders": ["source.project.raw.orders"],
                "model.project.int_orders": ["model.project.stg_orders"],
                "model.project.fct_orders": ["model.project.int_orders"]
              }
            }
            """);

        ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        var emittedByOutputId = runContext.assets().emitted().stream()
            .collect(toMap(
                assetEmit -> assetEmit.outputs().getFirst().getId(),
                assetEmit -> assetEmit
            ));

        // fct_orders should only depend on int_orders (from parent_map),
        // NOT on both stg_orders and int_orders (from depends_on.nodes)
        var fctOrdersEmit = emittedByOutputId.get("dev.marts.fct_orders");
        assertThat(fctOrdersEmit.inputs(), hasSize(1));
        assertThat(fctOrdersEmit.inputs().getFirst().id(), is("dev.intermediate.int_orders"));

        // int_orders should depend on stg_orders
        var intOrdersEmit = emittedByOutputId.get("dev.intermediate.int_orders");
        assertThat(intOrdersEmit.inputs(), hasSize(1));
        assertThat(intOrdersEmit.inputs().getFirst().id(), is("dev.staging.stg_orders"));

        // stg_orders has no model dependencies (source is filtered out)
        var stgOrdersEmit = emittedByOutputId.get("dev.staging.stg_orders");
        assertThat(stgOrdersEmit.inputs(), hasSize(0));
    }

    private RunContext mockRunContext() {
        var task = DbtCLI.builder()
            .id(IdUtils.create())
            .type(DbtCLI.class.getName())
            .commands(Property.ofValue(List.of("dbt run")))
            .build();

        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of(), null);
        var taskRun = TestsUtils.mockTaskRun(execution, task);
        return runContextFactory.of(flow, task, execution, taskRun, false);
    }
}
