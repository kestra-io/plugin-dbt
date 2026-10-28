import { computed, ref, shallowRef, watch } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import type { Execution, TaskRun } from "@kestra-io/kestra-sdk";
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

/** The host-bound call to one of this plugin's endpoints, see ModelGraphEndpoint.java and CompiledSqlEndpoint.java. */
export type FetchEndpoint = NonNullable<KnownSlotProps["topology-details"]["fetchEndpoint"]>;

// Bumped on the shared `execution` prop after a fetch, since the host's Vue runtime does not always repaint plugin refs (as in plugin-ee-gcp).
export const REV_KEY = "__dbtGraphRev";

// Same set as core State.Type.isTerminated().
const TERMINAL_STATES = ["SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED", "SKIPPED", "RETRIED", "RESUBMITTED"];

export function useModelGraph(
    // artifact-sdk types `task` as a plain record, so its id is `unknown`.
    taskId: () => unknown,
    slotExecution: () => KnownSlotProps["topology-details"]["execution"],
    fetchEndpoint: () => FetchEndpoint | undefined,
) {
    // The host passes a real Execution, but artifact-sdk types it as a plain record.
    const execution = () => slotExecution() as Execution | undefined;

    // Per-model child task runs carry the dbt unique_id as taskId, so this only matches the dbt task itself.
    const taskRun = computed<TaskRun | undefined>(() => {
        const runs = execution()?.taskRunList?.filter((tr) => tr.taskId === taskId()) ?? [];
        return runs[runs.length - 1];
    });

    // Thousands of nodes are read-only data, deep reactivity would only add proxy overhead.
    const graph = shallowRef<ModelGraph | null>(null);
    const status = ref<"idle" | "loading" | "loaded" | "missing" | "too_large" | "unsupported" | "error">("idle");
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

    async function load() {
        const call = fetchEndpoint();
        if (!call) {
            status.value = "unsupported";
            repaint();
            return;
        }
        // Drop responses from an earlier request that resolves after a newer one.
        const current = ++requestId;
        status.value = "loading";
        try {
            const parsed = (await call("model-graph", {}, { taskRunId: taskRun.value?.id })) as Partial<ModelGraph> & { error?: string };
            if (current !== requestId) return;
            if (parsed.error === "no_graph" || parsed.error === "too_large") {
                status.value = parsed.error === "no_graph" ? "missing" : "too_large";
                return;
            }
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
            if (id && taskRun.value && !isRunning.value) load();
        },
        { immediate: true },
    );

    // Why no graph is shown, or null once it loaded. Shared by the drawer and the Artifacts tab views.
    const message = computed(() => {
        if (!execution()?.id) return "Run the flow to see the dbt model graph.";
        if (isRunning.value) return "dbt is still running. The graph appears when the task finishes.";
        if (status.value === "idle") return "This task has not run in this execution.";
        if (status.value === "missing") return "This run produced no model graph (no readable manifest.json).";
        if (status.value === "too_large") return "This model graph is too large to show here.";
        if (status.value === "unsupported") return "This Kestra version cannot load the model graph yet.";
        if (status.value === "error") return "Could not load the dbt model graph.";
        if (status.value !== "loaded" || !graph.value) return "Loading the dbt model graph...";
        return null;
    });

    return { graph, status, isRunning, message };
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

// Status text colours, readable on cards and lists in light and dark (the topology border greens are too dim as text).
const STATUS_TEXT: Record<string, string> = {
    success: "var(--ks-text-success, #0f8a5f)",
    error: "var(--ks-text-error, #d0021b)",
    warn: "var(--ks-text-warning, #b36b00)",
};
export function statusColor(status: string): string {
    return STATUS_TEXT[status] ?? "var(--ks-text-secondary, #6b6b7b)";
}

// Only models and snapshots have SQL: seeds are CSV files, sources are raw tables.
export function hasSql(node: GraphNode): boolean {
    return node.resourceType === "model" || node.resourceType === "snapshot";
}

// The node every view opens on: the first failed model, else the first model, so all tabs agree.
export function initialNode(nodes: GraphNode[]): GraphNode | null {
    const withSql = nodes.filter(hasSql);
    return withSql.find((n) => n.status === "error") ?? withSql[0] ?? nodes[0] ?? null;
}
