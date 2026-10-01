<script setup lang="ts">
import { computed } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import { KsTopologyDetails } from "@kestra-io/design-system";
import { REV_KEY, formatSeconds, shortName, useDbtRunSummary } from "../composables/useDbtRunSummary";

const props = defineProps<KnownSlotProps["topology-task-modal"]>();

const { runSummary, testSummary, cloudRun, runUrl, status, isRunning } = useDbtRunSummary(
    () => (props.task as any)?.id as string | undefined,
    () => props.execution as Record<string, any> | undefined,
    () => (props as any).fetchOutputs,
);

const hasExecution = computed(() => !!(props.execution as any)?.id);

const overviewRows = computed(() => {
    const cloud = cloudRun.value;
    if (!cloud) {
        return [
            { label: "Engine", value: String((props.task as any)?.engine ?? "CORE") },
            { label: "Duration", value: formatSeconds(runSummary.value?.elapsedTime) },
        ];
    }
    return [
        { label: "Job", value: cloud.jobName ?? String(cloud.jobId ?? "-") },
        { label: "Environment", value: cloud.environmentName ?? "-" },
        { label: "Branch", value: cloud.gitBranch ?? "-" },
        { label: "dbt version", value: cloud.dbtVersion ?? "-" },
        { label: "Status", value: cloud.status ?? "-" },
        { label: "Duration", value: cloud.duration ?? formatSeconds(runSummary.value?.elapsedTime) },
        { label: "Queued", value: cloud.queuedDuration ?? "-" },
        { label: "Run time", value: cloud.runDuration ?? "-" },
    ];
});

const modelRows = computed(() => {
    const run = runSummary.value;
    if (!run) return [];
    return [
        { label: "Total", value: String(run.total) },
        { label: "Success", value: String(run.success) },
        { label: "Error", value: String(run.error) },
        { label: "Warn", value: String(run.warn) },
        { label: "Skipped", value: String(run.skipped) },
    ];
});

const testRows = computed(() => {
    const tests = testSummary.value;
    if (!tests) return [];
    return [
        { label: "Total", value: String(tests.total) },
        { label: "Pass", value: String(tests.pass) },
        { label: "Fail", value: String(tests.fail) },
        { label: "Warn", value: String(tests.warn) },
        { label: "Error", value: String(tests.error) },
        { label: "Skipped", value: String(tests.skipped) },
    ];
});

const slowestRows = computed(() =>
    (runSummary.value?.slowest ?? []).map((n) => ({
        label: shortName(n.uniqueId),
        value: formatSeconds(n.executionTime),
    })),
);
</script>

<template>
    <div class="dbt-cli-modal" :data-dbt-rev="(execution as any)?.[REV_KEY]">
        <p v-if="!hasExecution" class="dbt-cli-modal__empty">Run the flow to see dbt model and test results.</p>
        <p v-else-if="isRunning" class="dbt-cli-modal__empty">dbt is still running. Results appear when the task finishes.</p>
        <p v-else-if="status === 'error'" class="dbt-cli-modal__empty">Could not load dbt results.</p>
        <p v-else-if="status !== 'loaded'" class="dbt-cli-modal__empty">Loading dbt results...</p>
        <p v-else-if="!runSummary && !testSummary && !cloudRun" class="dbt-cli-modal__empty">No run results for this task yet.</p>

        <template v-else>
            <section class="dbt-cli-modal__section">
                <KsTopologyDetails :rows="overviewRows" />
                <p v-if="cloudRun?.statusMessage" class="dbt-cli-modal__empty">{{ cloudRun.statusMessage }}</p>
                <a v-if="runUrl" :href="runUrl" target="_blank" rel="noopener noreferrer" class="dbt-cli-modal__link">Open run in dbt Cloud</a>
            </section>
            <section v-if="modelRows.length" class="dbt-cli-modal__section">
                <h4 class="dbt-cli-modal__title">Models</h4>
                <KsTopologyDetails :rows="modelRows" />
            </section>
            <section v-if="testRows.length" class="dbt-cli-modal__section">
                <h4 class="dbt-cli-modal__title">Tests</h4>
                <KsTopologyDetails :rows="testRows" />
            </section>
            <section v-if="slowestRows.length" class="dbt-cli-modal__section">
                <h4 class="dbt-cli-modal__title">Slowest nodes</h4>
                <KsTopologyDetails :rows="slowestRows" />
            </section>
        </template>
    </div>
</template>

<!-- Not scoped: across the Module Federation boundary the plugin's scope id is not applied. Classes are dbt-namespaced instead. -->
<style>
.dbt-cli-modal__section + .dbt-cli-modal__section {
    margin-top: var(--ks-spacing-4, 1rem);
}

.dbt-cli-modal__title {
    margin: 0 0 var(--ks-spacing-2, 0.5rem);
    font-size: var(--ks-font-size-xs);
    font-weight: 600;
    color: var(--ks-text-secondary);
}

.dbt-cli-modal__link {
    display: inline-block;
    margin-top: var(--ks-spacing-2, 0.5rem);
}

.dbt-cli-modal__empty {
    margin: 0;
    color: var(--ks-text-secondary);
}
</style>
