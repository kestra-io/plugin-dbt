<script lang="ts">
type ArtifactContext = { fetchOutputs?: () => Promise<Record<string, unknown>> };

/** The line under the artifact title in the Artifacts tab, read from the small `graphSummary` output. */
export async function getSubtitle({ fetchOutputs }: ArtifactContext): Promise<string | undefined> {
    const summary = (await fetchOutputs?.())?.graphSummary as { nodes?: number; edges?: number } | undefined;
    if (summary?.nodes === undefined) return undefined;
    const edges = summary.edges ?? 0;
    return `${summary.nodes} ${summary.nodes === 1 ? "node" : "nodes"} · ${edges} ${edges === 1 ? "dependency" : "dependencies"}`;
}
</script>

<script setup lang="ts">
import { ref } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import DbtModelGraph from "./DbtModelGraph.vue";
import { type GraphNode, REV_KEY, useModelGraph } from "../composables/useModelGraph";

// The execution-artifact slot gets the topology-details props, which the published SDK types already cover.
const props = defineProps<KnownSlotProps["topology-details"]>();

const { graph, message } = useModelGraph(
    () => props.task?.id,
    () => props.execution,
    () => props.tenant,
    () => props.fetchOutputs,
);
const selected = ref<GraphNode | null>(null);
</script>

<template>
    <div class="dbt-artifact" :data-dbt-rev="(execution as Record<string, unknown> | undefined)?.[REV_KEY]">
        <p v-if="message || !graph" class="dbt-artifact__empty">{{ message }}</p>
        <DbtModelGraph v-else :graph="graph" :selectedId="selected?.uniqueId" embedded @select="selected = $event" />
    </div>
</template>

<style>
.dbt-artifact {
    height: 100%;
}

.dbt-artifact__empty {
    margin: 1rem;
    color: var(--ks-text-secondary);
}
</style>
