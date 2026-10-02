import { computed, ref, watch } from "vue";

export interface RunSummary {
    total: number;
    success: number;
    error: number;
    warn: number;
    skipped: number;
    elapsedTime?: number;
    slowest?: { uniqueId?: string; executionTime?: number }[];
}

export interface TestSummary {
    total: number;
    pass: number;
    fail: number;
    warn: number;
    error: number;
    skipped: number;
}

type FetchOutputs = (query?: { taskRunId?: string }) => Promise<Record<string, any> | undefined>;

// The host may render these slots under its own Vue runtime, so plugin-side refs do not always repaint
// them. Reading this key off the shared `execution` prop in the template, then bumping it after the
// fetch, forces the host re-render (same workaround as plugin-ee-gcp).
export const REV_KEY = "__dbtCliRev";

// "197 ok · 1 error": zero counts are dropped so the row stays short.
export function counts(parts: [number | undefined, string][]): string {
    const text = parts
        .filter(([n]) => (n ?? 0) > 0)
        .map(([n, label]) => `${n} ${label}`)
        .join(" · ");
    return text || "none";
}

export function formatSeconds(s?: number): string {
    if (s === undefined || s === null) return "-";
    if (s < 1) return `${Math.round(s * 1000)} ms`;
    if (s < 60) return `${s.toFixed(1)} s`;
    const total = Math.round(s);
    return `${Math.floor(total / 60)} min ${total % 60} s`;
}

// model.shop.orders -> orders
export function shortName(uniqueId?: string): string {
    return uniqueId?.split(".").at(-1) ?? "-";
}

export function useDbtRunSummary(
    taskId: () => string | undefined,
    execution: () => Record<string, any> | undefined,
    fetchOutputs: () => FetchOutputs | undefined,
) {
    // Per-model child task runs carry the dbt unique_id as taskId, so this only matches the dbt task itself.
    const taskRun = computed(() => {
        const list = execution()?.taskRunList as any[] | undefined;
        return list?.filter((tr: any) => tr.taskId === taskId()).at(-1);
    });

    const outputs = ref<Record<string, any> | null>(null);
    const status = ref<"idle" | "loading" | "loaded" | "error">("idle");
    let requestId = 0;

    function repaint() {
        const ex = execution();
        if (!ex) return;
        try {
            ex[REV_KEY] = ((ex[REV_KEY] as number) ?? 0) + 1;
        } catch {
            /* frozen execution prop */
        }
    }

    async function load() {
        // Drop responses from an earlier request that resolves after a newer one.
        const current = ++requestId;
        status.value = "loading";
        try {
            const result = (await fetchOutputs()?.({ taskRunId: taskRun.value?.id })) ?? null;
            if (current !== requestId) return;
            outputs.value = result;
            status.value = "loaded";
        } catch (e) {
            if (current !== requestId) return;
            console.warn("[plugin-dbt] Unable to load dbt task outputs", e);
            status.value = "error";
        }
        repaint();
    }

    // Refetch when the task run changes state, since outputs only exist once it is terminal.
    watch(
        () => [execution()?.id, taskRun.value?.state?.current],
        ([id]) => {
            if (id) load();
        },
        { immediate: true },
    );

    const TERMINAL_STATES = ["SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED", "SKIPPED"];
    const isRunning = computed(() => !!taskRun.value && !TERMINAL_STATES.includes(taskRun.value.state?.current));

    const runSummary = computed(() => outputs.value?.runSummary as RunSummary | undefined);
    const testSummary = computed(() => outputs.value?.testSummary as TestSummary | undefined);
    const hasFailures = computed(
        () =>
            (runSummary.value?.error ?? 0) > 0 ||
            (testSummary.value?.fail ?? 0) > 0 ||
            (testSummary.value?.error ?? 0) > 0,
    );

    return { runSummary, testSummary, hasFailures, status, isRunning };
}
