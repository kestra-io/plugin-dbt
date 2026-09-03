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
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.WorkerTaskResult;
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

    @Inject
    private DispatchQueueInterface<LogEntry> logQueue;

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
    void parseManifestWithAssets_shouldIgnoreUnknownDependsOnKeys() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        // dbt adds keys under `depends_on` between manifest schema versions. `nodes_with_ref_location`
        // holds arrays, not strings: reading it must not break lineage for the keys we do use.
        Files.writeString(manifestFile, """
            {
              "metadata": {
                "adapter_type": "postgres"
              },
              "nodes": {
                "model.analytics.parent": {
                  "resource_type": "model",
                  "database": "analytics",
                  "schema": "marts",
                  "name": "parent",
                  "unique_id": "model.analytics.parent",
                  "depends_on": {
                    "macros": [],
                    "nodes": [],
                    "nodes_with_ref_location": []
                  }
                },
                "model.analytics.child": {
                  "resource_type": "model",
                  "database": "analytics",
                  "schema": "marts",
                  "name": "child",
                  "unique_id": "model.analytics.child",
                  "depends_on": {
                    "macros": [],
                    "nodes": ["model.analytics.parent"],
                    "nodes_with_ref_location": [
                      ["model.analytics.parent", {"file": "models/child.sql", "line": 3}]
                    ]
                  }
                }
              }
            }
            """);

        var manifestResult = ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        assertThat(manifestResult.manifest(), is(notNullValue()));
        assertThat(runContext.assets().emitted(), hasSize(2));

        // depends_on.nodes is still read (no parent_map here), so the edge survives the unknown key.
        var childEmit = findEmitWithOutput(runContext.assets().emitted(), "analytics.marts.child");
        assertThat(childEmit, is(notNullValue()));
        assertThat(childEmit.inputs(), hasSize(1));
        assertThat(childEmit.inputs().getFirst().id(), is("analytics.marts.parent"));
    }

    @Test
    void parseManifestWithAssets_shouldStoreTheManifestFileUntouched() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        // The Manifest POJO drops keys it does not declare. The stored artifact must not: it is the
        // file users download, so it has to stay byte-for-byte what dbt wrote.
        var original = """
            {
              "nodes": {
                "model.p.child": {
                  "unique_id": "model.p.child",
                  "resource_type": "model",
                  "database": "db",
                  "schema": "s",
                  "name": "child",
                  "depends_on": {
                    "nodes": ["model.p.parent"],
                    "nodes_with_ref_location": [["model.p.parent", {"file": "models/child.sql", "line": 3}]],
                    "some_future_key": ["anything"]
                  }
                }
              }
            }
            """;
        Files.writeString(manifestFile, original);

        var manifestResult = ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        // putFile stores a copy and deletes the local file, so read it back from internal storage:
        // the bytes must be what dbt wrote, undeclared keys and all.
        var stored = new String(runContext.storage().getFile(manifestResult.uri()).readAllBytes());
        assertThat(stored, is(original));
        assertThat(stored, containsString("nodes_with_ref_location"));
        assertThat(stored, containsString("some_future_key"));
    }

    @Test
    void parseManifestWithAssets_shouldStoreManifestWhenItCannotBeRead() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        // An unreadable manifest is metadata we lose, not a reason to fail a dbt run that succeeded.
        Files.writeString(manifestFile, "{ this is not json");

        var manifestResult = ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        assertThat(manifestResult.manifest(), is(nullValue()));
        assertThat(manifestResult.uri(), is(notNullValue()));
        assertThat(runContext.assets().emitted(), hasSize(0));
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
        logQueue.addListener(logs::add);

        ResultParser.parseRunResult(runContext, runResultsFile.toFile(), null);

        // The model logs are attributed to each model's own dynamic taskrun, never the parent root.
        String parentTaskRunId = runContext.render("{{ taskrun.id }}");
        Set<String> modelTaskRunIds = runContext.dynamicWorkerResults().stream()
            .map(r -> r.getTaskRun().getId())
            .collect(Collectors.toSet());
        assertThat(modelTaskRunIds, hasSize(2));
        assertThat(modelTaskRunIds, not(hasItem(parentTaskRunId)));

        // 2.0 has no Flux to drain, so wait for each line asserted below before snapshotting.
        TestsUtils.awaitLog(logs, l -> isModelLog(l, modelTaskRunIds, "success"));
        TestsUtils.awaitLog(logs, l -> isModelLog(l, modelTaskRunIds, "Database Error"));

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
        assertThat(modelTaskRun.getAssetEmits(), hasSize(1));
        var modelBundle = modelTaskRun.getAssetEmits().getFirst();
        assertThat(modelBundle.getOutputs(), hasSize(1));
        assertThat(modelBundle.getOutputs().getFirst().getId(), is("analytics.staging.stg_orders"));
        assertThat(modelBundle.getInputs(), hasSize(1));
        assertThat(modelBundle.getInputs().getFirst().id(), is("analytics.raw.orders"));

        // the source-freshness taskrun must NOT claim it produced the source table
        TaskRun sourceTaskRun = byTaskId.get("source.analytics.raw.orders");
        assertThat(sourceTaskRun, is(notNullValue()));
        assertThat(sourceTaskRun.getAssetEmits(), is(nullValue()));
    }

    private static boolean isModelLog(LogEntry log, Set<String> modelTaskRunIds, String messagePart) {
        return log.getTaskRunId() != null
            && modelTaskRunIds.contains(log.getTaskRunId())
            && log.getMessage() != null
            && log.getMessage().contains(messagePart);
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

    @Test
    void parseRunResult_withFusionRunStatus_shouldSucceed() throws Exception {
        var runContext = mockRunContext();
        var runResultsFile = runContext.workingDir().path(true).resolve("run_results.json");
        // Fusion v2.0 emits "run" as the status for a successfully executed model
        Files.writeString(runResultsFile, """
            {
              "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6/run-results.json",
                "dbt_version": "2.0.0"
              },
              "results": [
                {
                  "status": "run",
                  "unique_id": "model.my_project.my_model",
                  "timing": [
                    {
                      "name": "compile",
                      "started_at": "2024-01-01T00:00:00.000000Z",
                      "completed_at": "2024-01-01T00:00:01.000000Z"
                    },
                    {
                      "name": "execute",
                      "started_at": "2024-01-01T00:00:01.000000Z",
                      "completed_at": "2024-01-01T00:00:02.000000Z"
                    }
                  ],
                  "thread_id": "Thread-1",
                  "execution_time": 2.0,
                  "adapter_response": {},
                  "message": "OK",
                  "failures": null
                }
              ],
              "elapsed_time": 2.5
            }
            """);

        var uri = ResultParser.parseRunResult(runContext, runResultsFile.toFile(), null);

        assertThat(uri, is(notNullValue()));
    }

    @Test
    void parseRunResult_withUnknownTopLevelFields_shouldNotFail() throws Exception {
        var runContext = mockRunContext();
        var runResultsFile = runContext.workingDir().path(true).resolve("run_results.json");
        // Fusion may add unknown top-level fields; @JsonIgnoreProperties ensures stability
        Files.writeString(runResultsFile, """
            {
              "metadata": {},
              "results": [
                {
                  "status": "success",
                  "unique_id": "model.my_project.stg_orders",
                  "timing": [],
                  "thread_id": "Thread-1",
                  "execution_time": 1.0,
                  "adapter_response": {},
                  "message": null,
                  "failures": null,
                  "fusion_extra_field": "ignored"
                }
              ],
              "elapsed_time": 1.0,
              "fusion_run_id": "some-uuid-from-fusion"
            }
            """);

        var uri = ResultParser.parseRunResult(runContext, runResultsFile.toFile(), null);

        assertThat(uri, is(notNullValue()));
    }

    /**
     * The Gantt showed every model as near-instantaneous because the terminal date came from the
     * end of the last timing phase, and those phases cover only compile and execute. dbt's execution_time is
     * the whole cost of the node, so the duration is anchored on that instead.
     */
    @Test
    void parseRunResult_shouldReportDbtExecutionTimeAsTheTaskRunDuration() throws Exception {
        var runContext = mockRunContext();
        var runResultsFile = runContext.workingDir().path(true).resolve("run_results.json");
        Files.writeString(runResultsFile, """
            {
              "metadata": {"dbt_version": "1.8.0"},
              "results": [
                {
                  "status": "success",
                  "unique_id": "model.p.slow_model",
                  "execution_time": 5.0,
                  "adapter_response": {},
                  "timing": [
                    {"name": "compile", "started_at": "2024-01-01T00:00:00Z", "completed_at": "2024-01-01T00:00:01Z"},
                    {"name": "execute", "started_at": "2024-01-01T00:00:01Z", "completed_at": "2024-01-01T00:00:02Z"}
                  ]
                },
                {
                  "status": "success",
                  "unique_id": "model.p.no_execution_time",
                  "adapter_response": {},
                  "timing": [
                    {"name": "compile", "started_at": "2024-01-01T00:00:00Z", "completed_at": "2024-01-01T00:00:01Z"},
                    {"name": "execute", "started_at": "2024-01-01T00:00:01Z", "completed_at": "2024-01-01T00:00:02Z"}
                  ]
                }
              ]
            }
            """);

        ResultParser.parseRunResult(runContext, runResultsFile.toFile(), null);

        Map<String, TaskRun> byTaskId = runContext.dynamicWorkerResults().stream()
            .map(WorkerTaskResult::getTaskRun)
            .collect(Collectors.toMap(TaskRun::getTaskId, tr -> tr));

        // The phases span 2s, but dbt charged the node 5s, so the taskrun reports 5s.
        TaskRun slow = byTaskId.get("model.p.slow_model");
        assertThat(slow.getState().getStartDate(), is(Instant.parse("2024-01-01T00:00:00Z")));
        assertThat(slow.getState().getDuration().orElseThrow(), is(Duration.ofSeconds(5)));

        // Without execution_time it still falls back to the end of the last phase.
        TaskRun fallback = byTaskId.get("model.p.no_execution_time");
        assertThat(fallback.getState().getDuration().orElseThrow(), is(Duration.ofSeconds(2)));
    }

    /**
     * Issue #319 reports that only models become assets. Seeds and snapshots are in
     * PRODUCED_RESOURCE_TYPES since #311, so this pins that: a seed feeding a model produces its own
     * asset and is the model's parent, and a snapshot produces one too. dbt tests are deliberately
     * absent, they describe no table.
     */
    @Test
    void parseManifestWithAssets_shouldEmitSeedAndSnapshotAssetsNotTests() throws Exception {
        var runContext = mockRunContext();
        var manifestFile = runContext.workingDir().path(true).resolve("manifest.json");
        Files.writeString(manifestFile, """
            {
              "metadata": {"adapter_type": "postgres"},
              "nodes": {
                "seed.p.raw_customers": {
                  "resource_type": "seed",
                  "database": "analytics",
                  "schema": "raw",
                  "name": "raw_customers",
                  "unique_id": "seed.p.raw_customers",
                  "depends_on": {"nodes": []}
                },
                "model.p.stg_customers": {
                  "resource_type": "model",
                  "database": "analytics",
                  "schema": "staging",
                  "name": "stg_customers",
                  "unique_id": "model.p.stg_customers",
                  "depends_on": {"nodes": ["seed.p.raw_customers"]}
                },
                "snapshot.p.customers_snap": {
                  "resource_type": "snapshot",
                  "database": "analytics",
                  "schema": "snapshots",
                  "name": "customers_snap",
                  "unique_id": "snapshot.p.customers_snap",
                  "depends_on": {"nodes": ["model.p.stg_customers"]}
                },
                "test.p.not_null_stg_customers_id.abc123": {
                  "resource_type": "test",
                  "database": "analytics",
                  "schema": "dbt_test__audit",
                  "name": "not_null_stg_customers_id",
                  "unique_id": "test.p.not_null_stg_customers_id.abc123",
                  "depends_on": {"nodes": ["model.p.stg_customers"]}
                }
              },
              "parent_map": {
                "seed.p.raw_customers": [],
                "model.p.stg_customers": ["seed.p.raw_customers"],
                "snapshot.p.customers_snap": ["model.p.stg_customers"],
                "test.p.not_null_stg_customers_id.abc123": ["model.p.stg_customers"]
              }
            }
            """);

        ResultParser.parseManifestWithAssets(runContext, manifestFile.toFile());

        var emitted = runContext.assets().emitted();

        // the seed is an asset in its own right, with no parents
        var seedEmit = findEmitWithOutput(emitted, "analytics.raw.raw_customers");
        assertThat("seed emission should exist", seedEmit, is(notNullValue()));
        assertThat(seedEmit.inputs(), hasSize(0));

        // and it is the model's parent, so lineage reaches the raw layer
        var modelEmit = findEmitWithOutput(emitted, "analytics.staging.stg_customers");
        assertThat(modelEmit.inputs(), hasSize(1));
        assertThat(modelEmit.inputs().getFirst().id(), is("analytics.raw.raw_customers"));

        // snapshots too
        var snapshotEmit = findEmitWithOutput(emitted, "analytics.snapshots.customers_snap");
        assertThat("snapshot emission should exist", snapshotEmit, is(notNullValue()));
        assertThat(snapshotEmit.inputs().getFirst().id(), is("analytics.staging.stg_customers"));

        // the dbt test is not an asset: it describes no table
        assertThat(findEmitWithOutput(emitted, "analytics.dbt_test__audit.not_null_stg_customers_id"), is(nullValue()));
        assertThat(emitted, hasSize(3));
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
