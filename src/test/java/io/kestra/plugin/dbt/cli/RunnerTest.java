package io.kestra.plugin.dbt.cli;

import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.Asset;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.AssetEmit;
import io.kestra.plugin.dbt.TestAssetManagerFactory;

import jakarta.inject.Inject;

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
        assertThat(execution.getTaskRunList(), hasSize(9));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @ExecuteFlow("sanity-checks/dbt_cli_complex_dag_test.yaml")
    void complexDagLineage(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<AssetEmit> allEmitted = assetManagerFactory.allEmitted();
        assertThat("should emit exactly 8 model assets", allEmitted, hasSize(8));

        /*
         * Each model emits one bundle: {its direct parents} -> {the model itself}. The model is the
         * single output, so no downstream cartesian join is possible. Expected edges:
         *
         * stg_customers parents=[] -> stg_customers
         * stg_orders parents=[] -> stg_orders
         * stg_payments parents=[] -> stg_payments
         * int_customer_orders parents=[stg_customers, stg_orders] -> int_customer_orders
         * int_order_payments parents=[stg_orders, stg_payments] -> int_order_payments
         * int_daily_revenue parents=[stg_orders, stg_payments] -> int_daily_revenue
         * fct_customer_summary parents=[int_customer_orders, int_order_payments] -> fct_customer_summary
         * fct_revenue_by_customer parents=[fct_customer_summary, int_daily_revenue] -> fct_revenue_by_customer
         */

        // Every event has exactly one output (the model itself).
        assertThat(allEmitted.stream().allMatch(e -> e.outputs().size() == 1), is(true));

        // Staging models: no parents.
        assertEmission(allEmitted, "stg_customers", List.of());
        assertEmission(allEmitted, "stg_orders", List.of());
        assertEmission(allEmitted, "stg_payments", List.of());

        // Intermediate models: parents are the staging models they read.
        assertEmission(
            allEmitted, "int_customer_orders",
            List.of("memory.main.stg_customers", "memory.main.stg_orders")
        );
        assertEmission(
            allEmitted, "int_order_payments",
            List.of("memory.main.stg_orders", "memory.main.stg_payments")
        );
        assertEmission(
            allEmitted, "int_daily_revenue",
            List.of("memory.main.stg_orders", "memory.main.stg_payments")
        );

        // Mart models: parents are the intermediate/mart models they read.
        assertEmission(
            allEmitted, "fct_customer_summary",
            List.of("memory.main.int_customer_orders", "memory.main.int_order_payments")
        );
        assertEmission(
            allEmitted, "fct_revenue_by_customer",
            List.of("memory.main.fct_customer_summary", "memory.main.int_daily_revenue")
        );
    }

    /**
     * Assert exactly one emission exists whose single output is the given model and whose inputs are
     * exactly the given parents.
     */
    private static void assertEmission(List<AssetEmit> allEmitted, String modelName, List<String> expectedInputIds) {
        String outputId = "memory.main." + modelName;
        AssetEmit matched = allEmitted.stream()
            .filter(emit ->
            {
                Set<String> outputs = emit.outputs().stream().map(Asset::getId).collect(Collectors.toSet());
                Set<String> inputs = emit.inputs().stream().map(AssetIdentifier::id).collect(Collectors.toSet());
                return outputs.equals(Set.of(outputId)) && inputs.equals(Set.copyOf(expectedInputIds));
            })
            .findFirst()
            .orElse(null);

        assertThat(modelName + " should have a matching emission", matched, is(notNullValue()));
    }
}
