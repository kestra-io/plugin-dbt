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
         * Expected lineage (inputs = upstream deps, outputs = downstream children):
         *
         * stg_customers: inputs=[] outputs=[int_customer_orders]
         * stg_orders: inputs=[] outputs=[int_customer_orders, int_order_payments, int_daily_revenue]
         * stg_payments: inputs=[] outputs=[int_order_payments, int_daily_revenue]
         * int_customer_orders: inputs=[stg_customers, stg_orders] outputs=[fct_customer_summary]
         * int_order_payments: inputs=[stg_orders, stg_payments] outputs=[fct_customer_summary]
         * int_daily_revenue: inputs=[stg_orders, stg_payments] outputs=[fct_revenue_by_customer]
         * fct_customer_summary: inputs=[int_customer_orders, int_order_payments] outputs=[fct_revenue_by_customer]
         * fct_revenue_by_customer: inputs=[fct_customer_summary, int_daily_revenue] outputs=[]
         */

        // Staging models: no inputs, outputs are the intermediate models they feed
        assertEmission(
            allEmitted, "stg_customers",
            List.of(),
            List.of("memory.main.int_customer_orders")
        );
        assertEmission(
            allEmitted, "stg_orders",
            List.of(),
            List.of("memory.main.int_customer_orders", "memory.main.int_order_payments", "memory.main.int_daily_revenue")
        );
        assertEmission(
            allEmitted, "stg_payments",
            List.of(),
            List.of("memory.main.int_order_payments", "memory.main.int_daily_revenue")
        );

        // Intermediate models: inputs from staging, outputs to marts
        assertEmission(
            allEmitted, "int_customer_orders",
            List.of("memory.main.stg_customers", "memory.main.stg_orders"),
            List.of("memory.main.fct_customer_summary")
        );
        assertEmission(
            allEmitted, "int_order_payments",
            List.of("memory.main.stg_orders", "memory.main.stg_payments"),
            List.of("memory.main.fct_customer_summary")
        );
        assertEmission(
            allEmitted, "int_daily_revenue",
            List.of("memory.main.stg_orders", "memory.main.stg_payments"),
            List.of("memory.main.fct_revenue_by_customer")
        );

        // Mart models
        assertEmission(
            allEmitted, "fct_customer_summary",
            List.of("memory.main.int_customer_orders", "memory.main.int_order_payments"),
            List.of("memory.main.fct_revenue_by_customer")
        );
        assertEmission(
            allEmitted, "fct_revenue_by_customer",
            List.of("memory.main.fct_customer_summary", "memory.main.int_daily_revenue"),
            List.of()
        );
    }

    /**
     * Find the emission matching the expected inputs and outputs, then verify it.
     */
    private static void assertEmission(List<AssetEmit> allEmitted, String modelName,
        List<String> expectedInputIds, List<String> expectedOutputIds) {
        // Find emission by matching expected inputs size and expected outputs
        AssetEmit matched = allEmitted.stream()
            .filter(emit ->
            {
                Set<String> inputs = emit.inputs().stream().map(AssetIdentifier::id).collect(Collectors.toSet());
                Set<String> outputs = emit.outputs().stream().map(Asset::getId).collect(Collectors.toSet());
                return inputs.equals(Set.copyOf(expectedInputIds)) && outputs.equals(Set.copyOf(expectedOutputIds));
            })
            .findFirst()
            .orElse(null);

        assertThat(modelName + " should have a matching emission", matched, is(notNullValue()));
    }
}
