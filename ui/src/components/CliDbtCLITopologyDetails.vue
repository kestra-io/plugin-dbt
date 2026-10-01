<script setup lang="ts">
import { computed } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { KsTopologyDetails } from "@kestra-io/design-system";
import { REV_KEY, counts, formatSeconds, shortName, useDbtRunSummary } from "../composables/useDbtRunSummary";

const props = defineProps<KnownSlotProps["topology-details"]>();

const { runSummary, testSummary, hasFailures, status } = useDbtRunSummary(
    () => props.task?.id as string | undefined,
    () => props.execution as Record<string, any> | undefined,
    () => props.fetchOutputs as any,
);

const rows = computed(() => {
    const result = [{ label: "Engine", value: String((props.task as any)?.engine ?? "CORE") }];
    if (status.value === "error") {
        result.push({ label: "Results", value: "unavailable" });
        return result;
    }
    const run = runSummary.value;
    const tests = testSummary.value;
    if (run) {
        result.push({
            label: "Models",
            value: counts([
                [run.success, "ok"],
                [run.error, "error"],
                [run.warn, "warn"],
                [run.skipped, "skipped"],
            ]),
        });
    }
    if (tests) {
        result.push({
            label: "Tests",
            value: tests.total === 0 ? "none" : counts([
                [tests.pass, "pass"],
                [tests.fail, "fail"],
                [tests.warn, "warn"],
                [tests.error, "error"],
                [tests.skipped, "skipped"],
            ]),
        });
    }
    if (run) {
        result.push({ label: "Duration", value: formatSeconds(run.elapsedTime) });
        const slowest = run.slowest?.[0];
        if (slowest) {
            result.push({
                label: "Slowest",
                value: `${shortName(slowest.uniqueId)} (${formatSeconds(slowest.executionTime)})`,
            });
        }
    }
    return result;
});
</script>

<template>
    <div
        class="dbt-cli-details"
        :class="{ 'dbt-cli-details--failed': hasFailures }"
        :data-dbt-rev="(execution as any)?.[REV_KEY]"
    >
        <KsTopologyDetails :rows="rows" />
    </div>
</template>

<!-- Not scoped: across the Module Federation boundary the plugin's scope id is not applied. Classes are dbt-namespaced instead. -->
<style>
.dbt-cli-details--failed {
    border-left: 3px solid var(--ks-border-error, #e5484d);
}
</style>
