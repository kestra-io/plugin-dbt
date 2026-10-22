<script setup lang="ts">
import { ref } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import DbtCompiledSql, { type FetchEndpoint } from "./DbtCompiledSql.vue";
import DbtModelGraph from "./DbtModelGraph.vue";
import { type GraphNode, REV_KEY, useModelGraph } from "../composables/useModelGraph";

// `fetchEndpoint` is a newer host prop than the published artifact-sdk types.
const props = defineProps<KnownSlotProps["topology-task-drawer"] & { fetchEndpoint?: FetchEndpoint }>();

const { graph, message } = useModelGraph(
    () => props.task?.id,
    () => props.execution,
    () => props.tenant,
    () => props.fetchOutputs,
);

// One tab per artifact. Tabs are drawn here rather than with KsTabs, whose styles do not reach the host.
const TABS = [
    { key: "graph", label: "Graph" },
    { key: "sql", label: "Compiled SQL" },
] as const;
const tab = ref<(typeof TABS)[number]["key"]>("graph");

// The model selected in the graph, shared with the Compiled SQL tab.
const selected = ref<GraphNode | null>(null);
function openSql(node: GraphNode) {
    selected.value = node;
    tab.value = "sql";
}
</script>

<template>
    <div class="dbt-drawer" :data-dbt-rev="(execution as Record<string, unknown> | undefined)?.[REV_KEY]">
        <nav class="dbt-drawer__tabs" role="tablist">
            <button
                v-for="t in TABS"
                :key="t.key"
                type="button"
                role="tab"
                :aria-selected="tab === t.key"
                :class="['dbt-drawer__tab', { 'dbt-drawer__tab--active': tab === t.key }]"
                @click="tab = t.key"
            >
                {{ t.label }}
            </button>
        </nav>

        <p v-if="message || !graph" class="dbt-drawer__empty">{{ message }}</p>
        <template v-else>
            <!-- v-show keeps the graph mounted, so its zoom and selection survive a trip to the other tab. -->
            <DbtModelGraph v-show="tab === 'graph'" :graph="graph" :selectedId="selected?.uniqueId" sqlAction @select="selected = $event" @open-sql="openSql" />
            <DbtCompiledSql v-if="tab === 'sql'" :node="selected" :nodes="graph.nodes" :fetchEndpoint="fetchEndpoint" @select="selected = $event" />
        </template>
    </div>
</template>

<style>
/* Same look as KsTabs `type="box"` (execution page tabs). */
.dbt-drawer__tabs {
    display: flex;
    align-items: center;
    gap: var(--ks-spacing-1, 4px);
    height: 44px;
    margin-bottom: 16px;
    border-bottom: 1px solid var(--ks-border-default);
}

.dbt-drawer__tab {
    min-width: 45px;
    height: 28px;
    padding: 4px 8px;
    border: 0.5px solid transparent;
    border-radius: 6px;
    background: transparent;
    color: var(--ks-text-secondary);
    font: inherit;
    /* One step above the execution tabs (xs), which read too small next to the drawer's 14px body. */
    font-size: var(--ks-font-size-sm, 0.75rem);
    font-weight: var(--ks-font-weight-regular, 400);
    cursor: pointer;
    transition: background-color 0.15s ease, color 0.15s ease, border-color 0.15s ease;
}

.dbt-drawer__tab:hover {
    color: var(--ks-text-primary);
}

.dbt-drawer__tab--active {
    background: var(--ks-btn-secondary-bg-active);
    border-color: var(--ks-btn-secondary-border-active, #8405ff);
    color: var(--ks-text-primary);
}

.dbt-drawer__empty {
    margin: 1rem 0;
    color: var(--ks-text-secondary);
}
</style>
