package io.kestra.plugin.dbt;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.dbt.cli.DbtCLI;

import jakarta.inject.Inject;

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

        // stg_orders has no inputs and 1 output (fct_orders, its child)
        // fct_orders has 1 input (stg_orders) and no outputs (leaf node)
        var stgOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.marts.fct_orders");
        assertThat("stg_orders emission should exist", stgOrdersEmit, is(notNullValue()));
        assertThat(stgOrdersEmit.inputs(), hasSize(0));
        assertThat(stgOrdersEmit.outputs(), hasSize(1));

        var fctOrdersOutput = stgOrdersEmit.outputs().getFirst();
        assertThat(fctOrdersOutput.getMetadata().get("system"), is("postgres"));
        assertThat(fctOrdersOutput.getMetadata().get("database"), is("analytics"));
        assertThat(fctOrdersOutput.getMetadata().get("schema"), is("marts"));
        assertThat(fctOrdersOutput.getMetadata().get("name"), is("fct_orders"));

        var fctOrdersEmit = findEmitWithInput(runContext.assets().emitted(), "analytics.staging.stg_orders");
        assertThat("fct_orders emission should exist", fctOrdersEmit, is(notNullValue()));
        assertThat(fctOrdersEmit.inputs(), hasSize(1));
        assertThat(fctOrdersEmit.outputs(), hasSize(0));
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

        assertThat(runContext.assets().emitted(), hasSize(2));

        // my_first_dbt_model: no inputs, 1 output (my_second_dbt_model)
        var firstModelEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.marts.my_second_dbt_model");
        assertThat(firstModelEmit, is(notNullValue()));
        assertThat(firstModelEmit.inputs(), hasSize(0));
        assertThat(firstModelEmit.outputs(), hasSize(1));

        // my_second_dbt_model: 1 input (my_first_dbt_model), no outputs (leaf)
        var secondModelEmit = findEmitWithInput(runContext.assets().emitted(), "analytics.marts.my_first_dbt_model");
        assertThat(secondModelEmit, is(notNullValue()));
        assertThat(secondModelEmit.inputs(), hasSize(1));
        assertThat(secondModelEmit.inputs().getFirst().id(), is("analytics.marts.my_first_dbt_model"));
        assertThat(secondModelEmit.outputs(), hasSize(0));
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

        assertThat(runContext.assets().emitted(), hasSize(3));

        // DAG: stg_orders → int_orders → fct_orders (parent_map, no transitive edges)
        // Inputs = upstream deps, Outputs = downstream children

        // stg_orders: no model inputs (source filtered out), 1 output (int_orders)
        var stgOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "dev.intermediate.int_orders");
        assertThat(stgOrdersEmit, is(notNullValue()));
        assertThat(stgOrdersEmit.inputs(), hasSize(0));
        assertThat(stgOrdersEmit.outputs(), hasSize(1));

        // int_orders: 1 input (stg_orders), 1 output (fct_orders)
        var intOrdersEmit = findEmitWithInputAndOutput(
            runContext.assets().emitted(),
            "dev.staging.stg_orders", "dev.marts.fct_orders"
        );
        assertThat(intOrdersEmit, is(notNullValue()));
        assertThat(intOrdersEmit.inputs(), hasSize(1));
        assertThat(intOrdersEmit.inputs().getFirst().id(), is("dev.staging.stg_orders"));
        assertThat(intOrdersEmit.outputs(), hasSize(1));
        assertThat(intOrdersEmit.outputs().getFirst().getId(), is("dev.marts.fct_orders"));

        // fct_orders: 1 input (int_orders only, from parent_map), no outputs (leaf)
        var fctOrdersEmit = findEmitWithInput(runContext.assets().emitted(), "dev.intermediate.int_orders");
        assertThat(fctOrdersEmit, is(notNullValue()));
        assertThat(fctOrdersEmit.inputs(), hasSize(1));
        assertThat(fctOrdersEmit.inputs().getFirst().id(), is("dev.intermediate.int_orders"));
        assertThat(fctOrdersEmit.outputs(), hasSize(0));
    }

    private static AssetEmit findEmitWithOutput(List<AssetEmit> emitted, String outputId) {
        return emitted.stream()
            .filter(e -> e.outputs().stream().anyMatch(o -> o.getId().equals(outputId)))
            .findFirst()
            .orElse(null);
    }

    private static AssetEmit findEmitWithInput(List<AssetEmit> emitted, String inputId) {
        return emitted.stream()
            .filter(e -> e.inputs().stream().anyMatch(i -> i.id().equals(inputId)))
            .filter(e -> e.outputs().isEmpty())
            .findFirst()
            .orElse(null);
    }

    private static AssetEmit findEmitWithInputAndOutput(List<AssetEmit> emitted, String inputId, String outputId) {
        return emitted.stream()
            .filter(e -> e.inputs().stream().anyMatch(i -> i.id().equals(inputId)))
            .filter(e -> e.outputs().stream().anyMatch(o -> o.getId().equals(outputId)))
            .findFirst()
            .orElse(null);
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
