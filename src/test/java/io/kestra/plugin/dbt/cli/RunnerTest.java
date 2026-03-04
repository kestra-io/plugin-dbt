package io.kestra.plugin.dbt.cli;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.AssetEmit;
import io.kestra.plugin.dbt.TestAssetManagerFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true)
class RunnerTest {

    @Inject
    private TestAssetManagerFactory assetManagerFactory;

    @BeforeEach
    void setUp() {
        assetManagerFactory.clear();
    }

    @Test
    @ExecuteFlow("sanity-checks/dbt_cli_assets_kv_test.yaml")
    void flow_name(Execution execution) {
        assertThat(execution.getTaskRunList(), hasSize(10));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow("sanity-checks/dbt_cli_complex_dag_test.yaml")
    void complexDagLineage(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        // Build a map of output asset id -> emitted asset (from the emitAssets() path that feeds the UI lineage graph)
        Map<String, AssetEmit> emittedByOutputId = assetManagerFactory.allEmitted().stream()
            .collect(toMap(
                assetEmit -> assetEmit.outputs().getFirst().getId(),
                assetEmit -> assetEmit,
                // dbt run produces one emission per model; take the last if duplicated
                (a, b) -> b
            ));

        assertThat("should emit exactly 8 model assets", emittedByOutputId.keySet(), hasSize(8));

        /*
         * Expected lineage:
         *
         ┌────────────────┐   ┌──────────────┐   ┌────────────────┐
         │ stg_customers  │   │  stg_orders  │   │  stg_payments  │
         │    (table)     │   │   (table)    │   │    (table)     │
         └───────┬────────┘   └──┬───┬───┬───┘   └──┬────┬───────┘
                 │               │   │   │           │    │
                 │   ┌───────────┘   │   └───────┐   │    │
                 ▼   ▼               │           ▼   ▼    │
         ┌──────────────────┐        │   ┌──────────────────┐
         │int_customer_orders│       │   │int_order_payments │
         │     (view)       │        │   │     (view)        │
         └────────┬─────────┘        │   └────────┬──────────┘
                  │                  │            │
                  │                  ▼            │
                  │┌─────────────────┐            │
                  ││int_daily_revenue│            │
                  ││     (view)      │            │
                  │└────────┬────────┘            │
                  │         │                     │
                  ▼         │                     ▼
         ┌──────────────────────────────────────────┐
         │         fct_customer_summary             │
         │              (table)                     │
         └─────────────────┬────────────────────────┘
                           │
                           ▼
         ┌──────────────────────────────────────────┐
         │        fct_revenue_by_customer           │
         │              (table)                     │
         └──────────────────────────────────────────┘
         */

        // Staging models: no parent model inputs (leaf nodes)
        assertEmittedInputs(emittedByOutputId, "memory.main.stg_customers");
        assertEmittedInputs(emittedByOutputId, "memory.main.stg_orders");
        assertEmittedInputs(emittedByOutputId, "memory.main.stg_payments");

        // Intermediate models: direct refs to staging only
        assertEmittedInputs(emittedByOutputId, "memory.main.int_customer_orders",
            "memory.main.stg_customers", "memory.main.stg_orders");
        assertEmittedInputs(emittedByOutputId, "memory.main.int_order_payments",
            "memory.main.stg_orders", "memory.main.stg_payments");
        assertEmittedInputs(emittedByOutputId, "memory.main.int_daily_revenue",
            "memory.main.stg_orders", "memory.main.stg_payments");

        // Mart models: direct refs only (no transitive edges to staging)
        assertEmittedInputs(emittedByOutputId, "memory.main.fct_customer_summary",
            "memory.main.int_customer_orders", "memory.main.int_order_payments");
        assertEmittedInputs(emittedByOutputId, "memory.main.fct_revenue_by_customer",
            "memory.main.fct_customer_summary", "memory.main.int_daily_revenue");
    }

    private static void assertEmittedInputs(Map<String, AssetEmit> emittedByOutputId, String assetId, String... expectedInputIds) {
        assertThat(assetId + " should be emitted", emittedByOutputId, hasKey(assetId));

        AssetEmit emit = emittedByOutputId.get(assetId);
        List<String> actualInputIds = emit.inputs().stream()
            .map(AssetIdentifier::id)
            .sorted()
            .toList();

        if (expectedInputIds.length == 0) {
            assertThat(assetId + " should have no inputs", actualInputIds, is(empty()));
        } else {
            assertThat(assetId + " inputs", actualInputIds, containsInAnyOrder(expectedInputIds));
        }
    }
}
