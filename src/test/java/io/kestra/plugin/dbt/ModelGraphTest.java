package io.kestra.plugin.dbt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.dbt.models.Manifest;
import io.kestra.plugin.dbt.models.RunResult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ModelGraphTest {
    private static Manifest.Node node(String uniqueId, String resourceType, String... dependsOn) {
        return Manifest.Node.builder()
            .uniqueId(uniqueId)
            .name(uniqueId.substring(uniqueId.lastIndexOf('.') + 1))
            .resourceType(resourceType)
            .database("analytics")
            .schema("prod")
            .dependsOn(Manifest.DependsOn.builder().nodes(List.of(dependsOn)).build())
            .build();
    }

    private static Manifest manifest(Map<String, List<String>> parentMap, Manifest.Node... nodes) {
        var source = Manifest.Source.builder()
            .uniqueId("source.shop.raw.orders").name("orders").identifier("raw_orders").database("raw").schema("shop")
            .build();
        return Manifest.builder()
            .nodes(Arrays.stream(nodes).collect(Collectors.toMap(Manifest.Node::getUniqueId, Function.identity())))
            .sources(Map.of(source.getUniqueId(), source))
            .parentMap(parentMap)
            .build();
    }

    private static RunResult.Result result(String uniqueId, String status) {
        return RunResult.Result.builder().uniqueId(uniqueId).status(status).build();
    }

    private static RunResult.Result result(String uniqueId, String status, double executionTime) {
        return RunResult.Result.builder().uniqueId(uniqueId).status(status).executionTime(executionTime).build();
    }

    private static RunResult runResult(RunResult.Result... results) {
        return RunResult.builder().results(List.of(results)).build();
    }

    private static Map<String, ModelGraph.Node> byId(ModelGraph graph) {
        return graph.getNodes().stream().collect(Collectors.toMap(ModelGraph.Node::getUniqueId, Function.identity()));
    }

    @Test
    void joinsRunStatusAndLevelsOntoEveryNode() {
        var manifest = manifest(
            null,
            Manifest.Node.builder()
                .uniqueId("model.shop.stg_orders").name("stg_orders").alias("stg_orders_v2").resourceType("model")
                .database("analytics").schema("staging")
                .config(Manifest.Config.builder().materialized("view").build())
                .tags(List.of("nightly"))
                .dependsOn(Manifest.DependsOn.builder().nodes(List.of("source.shop.raw.orders")).build())
                .build(),
            node("model.shop.orders", "model", "model.shop.stg_orders", "seed.shop.countries"),
            node("model.shop.revenue", "model", "model.shop.orders"),
            node("model.shop.ephemeral", "model", "model.shop.orders"),
            node("model.shop.untouched", "model"),
            node("seed.shop.countries", "seed"),
            node("snapshot.shop.orders_snapshot", "snapshot", "model.shop.orders"),
            node("operation.shop.on-run-end-0", "operation"),
            node("analysis.shop.adhoc", "analysis", "model.shop.orders")
        );
        var runResult = runResult(
            result("model.shop.stg_orders", "success", 1.5),
            // Fusion v2.0 emits "run" for a successfully executed model
            result("model.shop.orders", "run"),
            result("model.shop.revenue", "error"),
            result("model.shop.ephemeral", "no-op"),
            result("snapshot.shop.orders_snapshot", "skipped"),
            result("seed.shop.countries", "warn")
        );

        var graph = ModelGraph.from(manifest, runResult);
        var nodes = byId(graph);

        assertThat(
            nodes.keySet(), containsInAnyOrder(
                "source.shop.raw.orders", "seed.shop.countries", "model.shop.stg_orders", "model.shop.orders",
                "model.shop.revenue", "model.shop.ephemeral", "model.shop.untouched", "snapshot.shop.orders_snapshot"
            )
        );

        var stgOrders = nodes.get("model.shop.stg_orders");
        assertThat(stgOrders.getStatus(), is("success"));
        assertThat(stgOrders.getMaterialized(), is("view"));
        assertThat(stgOrders.getRelation(), is("analytics.staging.stg_orders_v2"));
        assertThat(stgOrders.getTags(), contains("nightly"));
        assertThat(stgOrders.getExecutionTime(), is(1.5));
        assertThat(nodes.get("model.shop.untouched").getExecutionTime(), nullValue());
        assertThat(nodes.get("model.shop.orders").getStatus(), is("success"));
        assertThat(nodes.get("model.shop.revenue").getStatus(), is("error"));
        assertThat(nodes.get("seed.shop.countries").getStatus(), is("warn"));
        assertThat(nodes.get("snapshot.shop.orders_snapshot").getStatus(), is("skipped"));
        // an unknown status stays raw, never reported as a success
        assertThat(nodes.get("model.shop.ephemeral").getStatus(), is("no-op"));
        assertThat(nodes.get("model.shop.untouched").getStatus(), is("not-run"));
        assertThat(nodes.get("source.shop.raw.orders").getStatus(), is("not-run"));
        assertThat(nodes.get("source.shop.raw.orders").getRelation(), is("raw.shop.raw_orders"));

        assertThat(nodes.get("source.shop.raw.orders").getLevel(), is(0));
        assertThat(nodes.get("model.shop.stg_orders").getLevel(), is(1));
        // longest path: source -> stg_orders -> orders, not seed -> orders
        assertThat(nodes.get("model.shop.orders").getLevel(), is(2));
        assertThat(nodes.get("model.shop.revenue").getLevel(), is(3));
        assertThat(graph.getNodes().getFirst().getLevel(), is(0));
        assertThat(graph.getNodes().getLast().getLevel(), is(3));

        // edges to nodes outside the graph (the analysis) are dropped
        assertThat(
            graph.getEdges(), containsInAnyOrder(
                new ModelGraph.Edge("source.shop.raw.orders", "model.shop.stg_orders"),
                new ModelGraph.Edge("model.shop.stg_orders", "model.shop.orders"),
                new ModelGraph.Edge("seed.shop.countries", "model.shop.orders"),
                new ModelGraph.Edge("model.shop.orders", "model.shop.revenue"),
                new ModelGraph.Edge("model.shop.orders", "model.shop.ephemeral"),
                new ModelGraph.Edge("model.shop.orders", "snapshot.shop.orders_snapshot")
            )
        );

        var summary = graph.getSummary();
        assertThat(summary.getNodes(), is(8));
        assertThat(summary.getEdges(), is(6));
        assertThat(
            summary.getByStatus(), is(
                Map.of(
                    "success", 2, "error", 1, "warn", 1, "skipped", 1, "no-op", 1, "not-run", 2
                )
            )
        );
    }

    @Test
    void mapsFailureAndPassAliasesToErrorAndSuccess() {
        var manifest = manifest(null, node("model.shop.a", "model"), node("model.shop.b", "model"), node("model.shop.c", "model"));
        var runResult = runResult(result("model.shop.a", "fail"), result("model.shop.b", "runtime_error"), result("model.shop.c", "pass"));

        var nodes = byId(ModelGraph.from(manifest, runResult));

        assertThat(nodes.get("model.shop.a").getStatus(), is("error"));
        assertThat(nodes.get("model.shop.b").getStatus(), is("error"));
        assertThat(nodes.get("model.shop.c").getStatus(), is("success"));
    }

    @Test
    void parentMapWinsOverDependsOn() {
        var manifest = manifest(
            Map.of("model.shop.orders", List.of("model.shop.customers")),
            node("model.shop.orders", "model", "model.shop.stg_orders"),
            node("model.shop.stg_orders", "model"),
            node("model.shop.customers", "model")
        );

        var graph = ModelGraph.from(manifest, null);

        assertThat(graph.getEdges(), contains(new ModelGraph.Edge("model.shop.customers", "model.shop.orders")));
    }

    @Test
    void cycleTerminatesWithFiniteLevels() {
        var manifest = manifest(
            null,
            node("model.shop.a", "model", "model.shop.b"),
            node("model.shop.b", "model", "model.shop.a"),
            node("model.shop.self", "model", "model.shop.self")
        );

        var graph = ModelGraph.from(manifest, null);

        assertThat(graph.getNodes(), hasSize(4));
        assertThat(graph.getNodes(), everyItem(hasProperty("level", lessThan(4))));
    }

    @Test
    void manifestWithoutNodesStillGraphsItsSources() {
        var source = Manifest.Source.builder().uniqueId("source.shop.raw.orders").name("orders").build();
        var manifest = Manifest.builder().sources(Map.of(source.getUniqueId(), source)).build();

        var graph = ModelGraph.from(manifest, runResult(result("test.shop.unique_orders_id", "fail")));

        assertThat(graph.getNodes(), contains(hasProperty("uniqueId", is("source.shop.raw.orders"))));
    }

    @Test
    void withoutRunResultsEveryNodeIsNotRun() {
        var manifest = manifest(null, node("model.shop.orders", "model", "source.shop.raw.orders"));

        var graph = ModelGraph.from(manifest, null);

        assertThat(graph.getNodes(), everyItem(hasProperty("status", is("not-run"))));
        assertThat(graph.getSummary().getByStatus(), is(Map.of("not-run", 2)));
    }

    @Test
    void graphRoundTripsThroughJson() throws Exception {
        var manifest = manifest(null, node("model.shop.orders", "model", "source.shop.raw.orders"));
        var graph = ModelGraph.from(manifest, runResult(result("model.shop.orders", "success")));

        var json = ResultParser.MAPPER.writeValueAsString(graph);

        assertThat(JacksonMapper.ofJson().readValue(json, ModelGraph.class), is(graph));
    }
}
