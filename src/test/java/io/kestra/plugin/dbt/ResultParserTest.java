package io.kestra.plugin.dbt;

import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.dbt.cli.DbtCLI;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class ResultParserTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    @Named(QueueFactoryInterface.WORKERTASKLOG_NAMED)
    private QueueInterface<LogEntry> logQueue;

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

    @Test
    void parseRunResult_shouldEmitModelLogsUnderDynamicTaskRuns() throws Exception {
        var runContext = mockRunContext();
        var runResultsFile = runContext.workingDir().path(true).resolve("run_results.json");
        Files.writeString(runResultsFile, """
            {
              "metadata": {
                "dbt_version": "1.8.0"
              },
              "results": [
                {
                  "status": "success",
                  "message": "CREATE VIEW",
                  "failures": null,
                  "unique_id": "model.my_project.stg_orders",
                  "execution_time": 0.42,
                  "adapter_response": {
                    "rows_affected": "10"
                  },
                  "timing": [
                    {"name": "compile", "started_at": "2024-01-01T00:00:00Z", "completed_at": "2024-01-01T00:00:01Z"},
                    {"name": "execute", "started_at": "2024-01-01T00:00:01Z", "completed_at": "2024-01-01T00:00:02Z"}
                  ]
                },
                {
                  "status": "error",
                  "message": "Database Error in model fct_orders\\n  relation \\"raw_orders\\" does not exist",
                  "failures": 1,
                  "unique_id": "model.my_project.fct_orders",
                  "execution_time": 0.13,
                  "adapter_response": {},
                  "timing": [
                    {"name": "compile", "started_at": "2024-01-01T00:00:02Z", "completed_at": "2024-01-01T00:00:03Z"},
                    {"name": "execute", "started_at": "2024-01-01T00:00:03Z", "completed_at": "2024-01-01T00:00:04Z"}
                  ]
                }
              ],
              "elapsed_time": 1.23
            }
            """);

        List<LogEntry> logs = new CopyOnWriteArrayList<>();
        Flux<LogEntry> receive = TestsUtils.receive(logQueue, l -> logs.add(l.getLeft()));

        ResultParser.parseRunResult(runContext, runResultsFile.toFile(), null);

        // The model logs are attributed to each model's own dynamic taskrun, never the parent root.
        String parentTaskRunId = runContext.render("{{ taskrun.id }}");
        Set<String> modelTaskRunIds = runContext.dynamicWorkerResults().stream()
            .map(r -> r.getTaskRun().getId())
            .collect(Collectors.toSet());
        assertThat(modelTaskRunIds, hasSize(2));
        assertThat(modelTaskRunIds, not(hasItem(parentTaskRunId)));

        TestsUtils.awaitLog(logs, l -> l.getTaskRunId() != null && modelTaskRunIds.contains(l.getTaskRunId()));
        receive.blockLast();

        List<LogEntry> modelLogs = List.copyOf(logs).stream()
            .filter(l -> l.getTaskRunId() != null && modelTaskRunIds.contains(l.getTaskRunId()))
            .toList();

        assertThat(modelLogs, is(not(empty())));
        // single-attempt dynamic taskruns: their logs live under attempt 0
        assertThat(modelLogs.stream().allMatch(l -> l.getAttemptNumber() != null && l.getAttemptNumber() == 0), is(true));

        // success model: a summary line carrying its uniqueId + status, logged at INFO under its own bar
        List<LogEntry> successLogs = modelLogs.stream()
            .filter(l -> "model.my_project.stg_orders".equals(l.getTaskId()))
            .toList();
        assertThat(successLogs, is(not(empty())));
        assertThat(successLogs.stream().allMatch(l -> l.getLevel() == Level.INFO), is(true));
        assertThat(
            successLogs.stream().anyMatch(l -> l.getMessage().contains("success")),
            is(true)
        );

        // failing model: ERROR level, the failure count and the dbt error message under its own bar
        List<LogEntry> errorLogs = modelLogs.stream()
            .filter(l -> l.getLevel() == Level.ERROR)
            .toList();
        assertThat(errorLogs, is(not(empty())));
        assertThat(errorLogs.stream().allMatch(l -> "model.my_project.fct_orders".equals(l.getTaskId())), is(true));
        assertThat(errorLogs.stream().anyMatch(l -> l.getMessage().contains("1 failure")), is(true));
        assertThat(errorLogs.stream().anyMatch(l -> l.getMessage().contains("Database Error")), is(true));
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
