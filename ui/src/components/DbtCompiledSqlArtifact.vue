<script lang="ts">
type ArtifactContext = { fetchOutputs?: () => Promise<Record<string, unknown>> };

/** Tells from the Artifacts list that compiled SQL is off, before the user opens it. */
export async function getSubtitle({ fetchOutputs }: ArtifactContext): Promise<string | undefined> {
    const outputs = await fetchOutputs?.();
    return outputs && outputs.compiledSqlEnabled !== true ? "Off, set outputCompiledSql: true" : undefined;
}
</script>

<script setup lang="ts">
import { ref, watch } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import DbtCompiledSql, { type FetchEndpoint } from "./DbtCompiledSql.vue";
import { type GraphNode, REV_KEY, useModelGraph } from "../composables/useModelGraph";

// `fetchEndpoint` is a newer host prop than the published artifact-sdk types.
const props = defineProps<KnownSlotProps["topology-details"] & { fetchEndpoint?: FetchEndpoint }>();

// The graph lists the models to pick from and their run status.
const { graph, message } = useModelGraph(
    () => props.task?.id,
    () => props.execution,
    () => props.tenant,
    () => props.fetchOutputs,
);

// Opens on the first failed model, else the first model, like the graph does.
const selected = ref<GraphNode | null>(null);
watch(graph, (g) => {
    const withSql = g?.nodes.filter((n) => n.resourceType === "model" || n.resourceType === "snapshot") ?? [];
    selected.value = withSql.find((n) => n.status === "error") ?? withSql[0] ?? null;
}, { immediate: true });
</script>

<template>
    <div :data-dbt-rev="(execution as Record<string, unknown> | undefined)?.[REV_KEY]">
        <p v-if="message || !graph" class="dbt-artifact__empty">{{ message }}</p>
        <DbtCompiledSql v-else :node="selected" :nodes="graph.nodes" :fetchEndpoint="fetchEndpoint" @select="selected = $event" />
    </div>
</template>
