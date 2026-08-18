package io.kestra.plugin.dbt;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.LogEntry;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.WorkerTaskResult;
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

    // One model reading one source, shared by the source-lineage tests.
    private static final String SOURCE_MANIFEST_JSON = """
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
                "nodes": ["source.analytics.raw.orders"]
              }
            }
          },
          "sources": {
            "source.analytics.raw.orders": {
              "database": "analytics",
              "schema": "raw",
              "name": "orders",
              "identifier": "orders",
              "resource_type": "source",
              "unique_id": "source.analytics.raw.orders"
            }
          },
          "parent_map": {
            "model.analytics.stg_orders": ["source.analytics.raw.orders"]
          }
        }
        """;

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

        // Each model emits one bundle: {its parents} -> {the model itself}.
        // stg_orders: no parents, output is stg_orders.
        var stgOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.staging.stg_orders");
        assertThat("stg_orders emission should exist", stgOrdersEmit, is(notNullValue()));
        assertThat(stgOrdersEmit.inputs(), hasSize(0));
        assertThat(stgOrdersEmit.outputs(), hasSize(1));

        var stgOrdersOutput = stgOrdersEmit.outputs().getFirst();
        assertThat(stgOrdersOutput.getMetadata().get("system"), is("postgres"));
        assertThat(stgOrdersOutput.getMetadata().get("database"), is("analytics"));
        assertThat(stgOrdersOutput.getMetadata().get("schema"), is("staging"));
        assertThat(stgOrdersOutput.getMetadata().get("name"), is("stg_orders"));

        // fct_orders: parent stg_orders -> model fct_orders.
        var fctOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.marts.fct_orders");
        assertThat("fct_orders emission should exist", fctOrdersEmit, is(notNullValue()));
        assertThat(fctOrdersEmit.inputs(), hasSize(1));
        assertThat(fctOrdersEmit.inputs().getFirst().id(), is("analytics.staging.stg_orders"));
        assertThat(fctOrdersEmit.outputs(), hasSize(1));

        // Every event has exactly one output (the model), so nothing can cross-join downstream.
        assertThat(runContext.assets().emitted().stream().allMatch(e -> e.outputs().size() == 1), is(true));
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

        // my_first_dbt_model: no parents, output is itself.
        var firstModelEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.marts.my_first_dbt_model");
        assertThat(firstModelEmit, is(notNullValue()));
        assertThat(firstModelEmit.inputs(), hasSize(0));
        assertThat(firstModelEmit.outputs(), hasSize(1));

        // my_second_dbt_model: parent my_first_dbt_model -> model my_second_dbt_model.
        var secondModelEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.marts.my_second_dbt_model");
        assertThat(secondModelEmit, is(notNullValue()));
        assertThat(secondModelEmit.inputs(), hasSize(1));
        assertThat(secondModelEmit.inputs().getFirst().id(), is("analytics.marts.my_first_dbt_model"));
        assertThat(secondModelEmit.outputs(), hasSize(1));
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

        // DAG from parent_map (no transitive edges): stg_orders -> int_orders -> fct_orders.
        // Each event is {parents} -> {the model itself}, one output per event.

        // stg_orders: its only parent is a source that this manifest never defines in `sources`,
        // so it is dropped, leaving stg_orders with no resolvable inputs.
        var stgOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "dev.staging.stg_orders");
        assertThat(stgOrdersEmit, is(notNullValue()));
        assertThat(stgOrdersEmit.inputs(), hasSize(0));
        assertThat(stgOrdersEmit.outputs(), hasSize(1));

        // int_orders: parent stg_orders -> model int_orders.
        var intOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "dev.intermediate.int_orders");
        assertThat(intOrdersEmit, is(notNullValue()));
        assertThat(intOrdersEmit.inputs(), hasSize(1));
        assertThat(intOrdersEmit.inputs().getFirst().id(), is("dev.staging.stg_orders"));
        assertThat(intOrdersEmit.outputs(), hasSize(1));

        // fct_orders: parent int_orders only (parent_map is the direct DAG) -> model fct_orders.
        var fctOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "dev.marts.fct_orders");
        assertThat(fctOrdersEmit, is(notNullValue()));
        assertThat(fctOrdersEmit.inputs(), hasSize(1));
        assertThat(fctOrdersEmit.inputs().getFirst().id(), is("dev.intermediate.int_orders"));
        assertThat(fctOrdersEmit.outputs(), hasSize(1));
    }

    @Test
    void parseManifestWithAssets_shouldEmitModelToSourceEdges() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        Files.writeString(manifestFile, SOURCE_MANIFEST_JSON);

        ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        // Only the model is emitted (a source is never built, so it emits no event of its own),
        // but the source is preserved as the model's input, giving a real source -> model edge.
        assertThat(runContext.assets().emitted(), hasSize(1));

        var stgOrdersEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.staging.stg_orders");
        assertThat(stgOrdersEmit, is(notNullValue()));
        assertThat(stgOrdersEmit.outputs(), hasSize(1));
        assertThat(stgOrdersEmit.inputs(), hasSize(1));
        // source assetId is database.schema.identifier
        assertThat(stgOrdersEmit.inputs().getFirst().id(), is("analytics.raw.orders"));
    }

    @Test
    void parseManifestWithAssets_shouldSkipNodeWithMissingResourceType() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        // A node with no `resource_type` (partial/hand-edited manifest or dbt schema drift) must be skipped, not crash.
        Files.writeString(manifestFile, """
            {
              "nodes": {
                "model.p.good": {"unique_id": "model.p.good", "resource_type": "model", "database": "db", "schema": "s", "name": "good"},
                "model.p.bad":  {"unique_id": "model.p.bad", "database": "db", "schema": "s", "name": "bad"}
              },
              "parent_map": {"model.p.good": [], "model.p.bad": []}
            }
            """);

        ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        // The node missing resource_type is skipped; only the valid model is emitted, no NPE.
        assertThat(runContext.assets().emitted(), hasSize(1));
        assertThat(findEmitWithOutput(runContext.assets().emitted(), "db.s.good"), is(notNullValue()));
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

    @Test
    void parseRunResult_shouldAttachModelAssetsButNotClaimSourceAsOutput() throws Exception {
        var runContext = mockRunContext();

        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        Files.writeString(manifestFile, SOURCE_MANIFEST_JSON);
        var manifest = ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile()).manifest();

        // run_results with a `dbt source freshness` entry for the source alongside the model build.
        var runResultsFile = runContext.workingDir().path(true).resolve("run_results.json");
        Files.writeString(runResultsFile, """
            {
              "metadata": {"dbt_version": "1.8.0"},
              "results": [
                {
                  "status": "pass",
                  "unique_id": "source.analytics.raw.orders",
                  "execution_time": 0.1,
                  "adapter_response": {},
                  "timing": [
                    {"name": "execute", "started_at": "2024-01-01T00:00:00Z", "completed_at": "2024-01-01T00:00:01Z"}
                  ]
                },
                {
                  "status": "success",
                  "unique_id": "model.analytics.stg_orders",
                  "execution_time": 0.2,
                  "adapter_response": {},
                  "timing": [
                    {"name": "execute", "started_at": "2024-01-01T00:00:01Z", "completed_at": "2024-01-01T00:00:02Z"}
                  ]
                }
              ],
              "elapsed_time": 0.3
            }
            """);

        ResultParser.parseRunResult(runContext, runResultsFile.toFile(), manifest);

        Map<String, TaskRun> byTaskId = runContext.dynamicWorkerResults().stream()
            .map(WorkerTaskResult::getTaskRun)
            .collect(Collectors.toMap(TaskRun::getTaskId, tr -> tr));

        // the model's dynamic taskrun carries {source} -> {the model itself}
        TaskRun modelTaskRun = byTaskId.get("model.analytics.stg_orders");
        assertThat(modelTaskRun, is(notNullValue()));
        assertThat(modelTaskRun.getAssets(), is(notNullValue()));
        assertThat(modelTaskRun.getAssets().getOutputs(), hasSize(1));
        assertThat(modelTaskRun.getAssets().getOutputs().getFirst().getId(), is("analytics.staging.stg_orders"));
        assertThat(modelTaskRun.getAssets().getInputs(), hasSize(1));
        assertThat(modelTaskRun.getAssets().getInputs().getFirst().id(), is("analytics.raw.orders"));

        // the source-freshness taskrun must NOT claim it produced the source table
        TaskRun sourceTaskRun = byTaskId.get("source.analytics.raw.orders");
        assertThat(sourceTaskRun, is(notNullValue()));
        assertThat(sourceTaskRun.getAssets(), is(nullValue()));
    }

    private static AssetEmit findEmitWithOutput(List<AssetEmit> emitted, String outputId) {
        return emitted.stream()
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

    @Test
    void parseRunResult_shouldNotFailOnANodeWithNoTimings() throws Exception {
        var runContext = mockRunContext();
        var runResultsFile = runContext.workingDir().path(true).resolve("run_results.json");
        // dbt emits an empty `timing` for a node it never ran: a skipped model, or one that failed
        // before it compiled. Its taskrun must still carry a usable state history.
        Files.writeString(runResultsFile, """
            {
              "metadata": {
                "dbt_version": "1.8.0",
                "generated_at": "2024-01-01T00:00:10Z"
              },
              "results": [
                {
                  "status": "skipped",
                  "message": null,
                  "failures": null,
                  "unique_id": "model.my_project.downstream",
                  "execution_time": 0.0,
                  "adapter_response": {},
                  "timing": []
                },
                {
                  "status": "success",
                  "message": "CREATE VIEW",
                  "failures": null,
                  "unique_id": "model.my_project.stg_orders",
                  "execution_time": 0.42,
                  "adapter_response": {},
                  "timing": [
                    {"name": "compile", "started_at": "2024-01-01T00:00:00Z", "completed_at": "2024-01-01T00:00:01Z"},
                    {"name": "execute", "started_at": "2024-01-01T00:00:01Z", "completed_at": "2024-01-01T00:00:02Z"}
                  ]
                }
              ],
              "elapsed_time": 1.23
            }
            """);

        ResultParser.parseRunResult(runContext, runResultsFile.toFile(), null);

        Map<String, TaskRun> byNode = runContext.dynamicWorkerResults().stream()
            .map(WorkerTaskResult::getTaskRun)
            .collect(Collectors.toMap(TaskRun::getTaskId, t -> t));
        assertThat(byNode.keySet(), hasSize(2));

        // the skipped node: reading its dates must not throw, and it is anchored on the run's generated_at
        var skipped = byNode.get("model.my_project.downstream").getState();
        assertThat(skipped.getCurrent(), is(State.Type.SKIPPED));
        assertThat(skipped.getStartDate(), is(Instant.parse("2024-01-01T00:00:10Z")));
        assertThat(skipped.getDuration(), is(java.util.Optional.of(Duration.ZERO)));

        // the node dbt did run still reports dbt's own timings, not Kestra's materialization time
        var ran = byNode.get("model.my_project.stg_orders").getState();
        assertThat(ran.getStartDate(), is(Instant.parse("2024-01-01T00:00:00Z")));
        assertThat(ran.getEndDate(), is(java.util.Optional.of(Instant.parse("2024-01-01T00:00:02Z"))));
        assertThat(ran.getDuration(), is(java.util.Optional.of(Duration.ofSeconds(2))));
    }

}
