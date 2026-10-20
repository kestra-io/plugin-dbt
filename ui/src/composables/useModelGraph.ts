import { computed, ref, shallowRef, watch } from "vue";
import type { Execution, TaskRun } from "@kestra-io/kestra-sdk";
import { downloadFileFromExecution } from "@kestra-io/kestra-sdk/executions";
// The plugin icon Kestra shows on the dbt task node, so the drawer uses the same logo as the topology.
import dbtLogo from "../../../src/main/resources/icons/plugin-icon.svg?raw";

/** One node of `graph.json`, see ModelGraph.java. */
export interface GraphNode {
    uniqueId: string;
    name?: string;
    resourceType?: string;
    materialized?: string;
    relation?: string;
    tags?: string[];
    status: string;
    executionTime?: number;
    level: number;
}

export interface GraphEdge {
    from: string;
    to: string;
}

export interface ModelGraph {
    nodes: GraphNode[];
    edges: GraphEdge[];
}

type FetchOutputs = (query?: { taskRunId?: string }) => Promise<Record<string, unknown>>;

// The host may render these slots under its own Vue runtime, so plugin-side refs do not always repaint
// them. Reading this key off the shared `execution` prop in the template, then bumping it after the
// fetch, forces the host re-render (same workaround as plugin-ee-gcp).
export const REV_KEY = "__dbtGraphRev";

const TERMINAL_STATES = ["SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED", "SKIPPED"];

export function useModelGraph(
    taskId: () => string | undefined,
    execution: () => Execution | undefined,
    tenant: () => string | undefined,
    fetchOutputs: () => FetchOutputs | undefined,
) {
    // Per-model child task runs carry the dbt unique_id as taskId, so this only matches the dbt task itself.
    const taskRun = computed<TaskRun | undefined>(() => {
        const runs = execution()?.taskRunList?.filter((tr) => tr.taskId === taskId()) ?? [];
        return runs[runs.length - 1];
    });

    // Thousands of nodes are read-only data, deep reactivity would only add proxy overhead.
    const graph = shallowRef<ModelGraph | null>(null);
    const status = ref<"idle" | "loading" | "loaded" | "missing" | "error">("idle");
    let requestId = 0;

    function repaint() {
        const ex = execution() as (Execution & Record<string, unknown>) | undefined;
        if (!ex) return;
        try {
            ex[REV_KEY] = ((ex[REV_KEY] as number | undefined) ?? 0) + 1;
        } catch {
            /* frozen execution prop */
        }
    }

    async function load(executionId: string) {
        // Drop responses from an earlier request that resolves after a newer one.
        const current = ++requestId;
        status.value = "loading";
        try {
            const outputs = await fetchOutputs()?.({ taskRunId: taskRun.value?.id });
            const uri = outputs?.graph;
            if (typeof uri !== "string") {
                if (current === requestId) status.value = "missing";
                return;
            }
            const file = await downloadFileFromExecution({ executionId, path: uri, tenant: tenant() });
            const parsed = JSON.parse(await file.text()) as Partial<ModelGraph>;
            if (current !== requestId) return;
            if (!Array.isArray(parsed.nodes) || !Array.isArray(parsed.edges)) {
                throw new Error("graph.json has no nodes or edges");
            }
            graph.value = parsed as ModelGraph;
            status.value = "loaded";
        } catch (e) {
            if (current !== requestId) return;
            console.warn("[plugin-dbt] Unable to load the dbt model graph", e);
            status.value = "error";
        } finally {
            repaint();
        }
    }

    const isRunning = computed(() => {
        const state = taskRun.value?.state?.current;
        return !!state && !TERMINAL_STATES.includes(state);
    });

    // Refetch when the task run reaches a terminal state, since the graph only exists once it is done.
    watch(
        () => [execution()?.id, taskRun.value?.state?.current] as const,
        ([id]) => {
            // No task run yet means the task has not started, so there is nothing to fetch.
            if (id && taskRun.value && !isRunning.value) load(id);
        },
        { immediate: true },
    );

    return { graph, status, isRunning };
}

// The logo on a white rounded tile, like the icon of a topology task node. Built once: every card shares it.
export const DBT_ICON = (() => {
    const viewBox = dbtLogo.match(/viewBox="([^"]+)"/)?.[1] ?? "0 0 24 24";
    const inner = dbtLogo.replace(/^[\s\S]*?<svg[^>]*>/, "").replace(/<\/svg>\s*$/, "");
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 36 36"><rect width="36" height="36" rx="8" fill="#ffffff"/><svg x="7" y="7" width="22" height="22" viewBox="${viewBox}">${inner}</svg></svg>`;
    return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
})();

// dbt run status -> the Kestra state KsExecutionStatus draws, so the drawer shows status the way Kestra does.
const KESTRA_STATES: Record<string, string> = { success: "SUCCESS", error: "FAILED", warn: "WARNING", skipped: "SKIPPED" };
const STATUS_TITLES: Record<string, string> = { success: "Success", error: "Error", warn: "Warning", skipped: "Skipped", "not-run": "Not run" };
export function kestraState(status: string): string {
    return KESTRA_STATES[status] ?? "CREATED";
}
export function statusTitle(status: string): string {
    return STATUS_TITLES[status] ?? status;
}
