<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref, watch } from "vue";
import ContentCopy from "vue-material-design-icons/ContentCopy.vue";
import ChevronDown from "vue-material-design-icons/ChevronDown.vue";
import CodeBraces from "vue-material-design-icons/CodeBraces.vue";
import Magnify from "vue-material-design-icons/Magnify.vue";
import { KsExecutionStatus } from "@kestra-io/design-system";
import { DBT_ICON, type GraphNode, kestraState, statusTitle } from "../composables/useModelGraph";

/** The host-bound call to one of this plugin's endpoints (core `fetchEndpoint`), absent on older Kestra. */
export type FetchEndpoint = (name: string, params?: Record<string, string>) => Promise<unknown>;

const props = defineProps<{ node: GraphNode | null; nodes: GraphNode[]; fetchEndpoint?: FetchEndpoint }>();
const emit = defineEmits<{ select: [node: GraphNode] }>();

interface CompiledSql {
    uniqueId?: string;
    compiledSql?: string;
    truncated?: boolean;
    error?: string;
    message?: string;
}

// The endpoint answers refusals as `{error, message}`, so they are shown as text, not as failures.
const MESSAGES: Record<string, string> = {
    disabled: "Compiled SQL is off for this task. Set outputCompiledSql: true on the task to show it.",
    not_found: "dbt did not compile this model in this run.",
    no_manifest: "This run stored no manifest.json.",
};

const STATUS_COLORS: Record<string, string> = {
    success: "var(--ks-text-success, #0f8a5f)",
    error: "var(--ks-text-error, #d0021b)",
    warn: "var(--ks-text-warning, #b36b00)",
};
const STATUS_LABELS: Record<string, string> = { success: "Success", error: "Error", warn: "Warning", skipped: "Skipped", "not-run": "Not run" };
const statusColor = (status: string) => STATUS_COLORS[status] ?? "var(--ks-text-secondary, #6b6b7b)";
const statusLabel = (status: string) => STATUS_LABELS[status] ?? status;

// --- model list: only models and snapshots have SQL (seeds are CSV files, sources are raw tables), grouped by
// type, sorted by name and filtered by the search.
const GROUPS = [
    { type: "model", label: "Models" },
    { type: "snapshot", label: "Snapshots" },
];
const query = ref("");
const groups = computed(() => {
    const q = query.value.trim().toLowerCase();
    return GROUPS
        .map((g) => ({
            ...g,
            nodes: props.nodes
                .filter((n) => n.resourceType === g.type && (!q || (n.name ?? n.uniqueId).toLowerCase().includes(q)))
                .sort((a, b) => (a.name ?? a.uniqueId).localeCompare(b.name ?? b.uniqueId)),
        }))
        .filter((g) => g.nodes.length);
});
const collapsed = ref(new Set<string>());
function toggle(type: string) {
    const next = new Set(collapsed.value);
    if (!next.delete(type)) next.add(type);
    collapsed.value = next;
}
const total = computed(() => props.nodes.filter((n) => GROUPS.some((g) => g.type === n.resourceType)).length);
// Keyboard navigation walks what is on screen, so collapsed groups are skipped.
const visible = computed(() => groups.value.filter((g) => !collapsed.value.has(g.type) || query.value).flatMap((g) => g.nodes));

// The name split around the search match, so the match can be highlighted without building HTML.
function parts(name: string): { text: string; match: boolean }[] {
    const q = query.value.trim();
    const at = q ? name.toLowerCase().indexOf(q.toLowerCase()) : -1;
    if (at < 0) return [{ text: name, match: false }];
    return [
        { text: name.slice(0, at), match: false },
        { text: name.slice(at, at + q.length), match: true },
        { text: name.slice(at + q.length), match: false },
    ].filter((p) => p.text);
}

const list = ref<HTMLElement | null>(null);
const search = ref<HTMLInputElement | null>(null);
const switcher = ref<HTMLElement | null>(null);

// The list opens as a panel under the current model, so the code keeps the full width.
const open = ref(false);
function onOutside(event: MouseEvent) {
    if (switcher.value && !switcher.value.contains(event.target as Node)) close();
}
function close() {
    open.value = false;
    query.value = "";
    document.removeEventListener("mousedown", onOutside);
}
function toggleOpen() {
    if (open.value) return close();
    open.value = true;
    document.addEventListener("mousedown", onOutside);
    nextTick(() => {
        search.value?.focus();
        list.value?.querySelector<HTMLElement>(`[data-id="${CSS.escape(props.node?.uniqueId ?? "")}"]`)?.scrollIntoView({ block: "nearest" });
    });
}
onBeforeUnmount(() => document.removeEventListener("mousedown", onOutside));

function choose(node: GraphNode) {
    emit("select", node);
    close();
}


// In the panel: arrows move through the list, Enter keeps the choice and closes, Esc clears then closes.
function onKeydown(event: KeyboardEvent) {
    if (event.key === "Escape") {
        if (query.value) query.value = "";
        else close();
        return;
    }
    if (event.key === "Enter") {
        const first = visible.value[0];
        if (query.value && first && !visible.value.some((n) => n.uniqueId === props.node?.uniqueId)) emit("select", first);
        close();
        return;
    }
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") return;
    event.preventDefault();
    const items = visible.value;
    if (!items.length) return;
    const at = items.findIndex((n) => n.uniqueId === props.node?.uniqueId);
    const next = event.key === "ArrowDown" ? Math.min(items.length - 1, at + 1) : Math.max(0, at - 1);
    emit("select", items[at < 0 ? 0 : next]);
}
watch(() => props.node?.uniqueId, (id) => {
    nextTick(() => list.value?.querySelector<HTMLElement>(`[data-id="${CSS.escape(id ?? "")}"]`)?.scrollIntoView({ block: "nearest" }));
});

// --- compiled SQL, one request per model.
const cache = new Map<string, CompiledSql>();
const result = ref<CompiledSql | null>(null);
const loading = ref(false);
const failed = ref(false);
let requestId = 0;

async function load(node: GraphNode | null) {
    result.value = null;
    failed.value = false;
    if (!node || !props.fetchEndpoint) return;
    const cached = cache.get(node.uniqueId);
    if (cached) {
        result.value = cached;
        return;
    }
    const current = ++requestId;
    loading.value = true;
    try {
        const body = (await props.fetchEndpoint("compiled-sql", { uniqueId: node.uniqueId })) as CompiledSql;
        if (current !== requestId) return;
        cache.set(node.uniqueId, body);
        result.value = body;
    } catch (e) {
        if (current !== requestId) return;
        console.warn("[plugin-dbt] Unable to load the compiled SQL", e);
        failed.value = true;
    } finally {
        if (current === requestId) loading.value = false;
    }
}
watch(() => props.node?.uniqueId, () => load(props.node), { immediate: true });

// --- light SQL colouring: the text is split into typed tokens rendered as text, never as HTML.
const KEYWORDS = new Set(("select from where and or not as with join left right inner outer full cross on using group by order " +
    "having limit offset union all distinct case when then else end in is null like ilike between exists insert into values " +
    "update set delete create replace table view materialized cast over partition window qualify asc desc true false " +
    "coalesce count sum avg min max lateral rows range interval date timestamp").split(" "));
type Token = { text: string; kind: "keyword" | "string" | "comment" | "number" | "plain" };
const TOKEN = /(--[^\n]*|\/\*[\s\S]*?\*\/)|('(?:[^']|'')*')|\b(\d+(?:\.\d+)?)\b|([A-Za-z_][A-Za-z0-9_]*)/g;

const lines = computed<Token[][]>(() => {
    const sql = result.value?.compiledSql ?? "";
    const tokens: Token[] = [];
    let last = 0;
    for (const m of sql.matchAll(TOKEN)) {
        const at = m.index ?? 0;
        if (at > last) tokens.push({ text: sql.slice(last, at), kind: "plain" });
        const kind: Token["kind"] = m[1] ? "comment" : m[2] ? "string" : m[3] ? "number" : KEYWORDS.has(m[0].toLowerCase()) ? "keyword" : "plain";
        tokens.push({ text: m[0], kind });
        last = at + m[0].length;
    }
    if (last < sql.length) tokens.push({ text: sql.slice(last), kind: "plain" });
    // Split tokens on line breaks so every line gets its own number.
    const out: Token[][] = [[]];
    for (const token of tokens) {
        token.text.split("\n").forEach((piece, i) => {
            if (i > 0) out.push([]);
            if (piece) out[out.length - 1].push({ text: piece, kind: token.kind });
        });
    }
    return out;
});

// Why there is no SQL to show, explained in the space the code would take.
const empty = computed<{ title: string; text: string; snippet?: string } | null>(() => {
    const node = props.node;
    if (!node) return { title: "No model selected", text: "Pick a model above to see the SQL dbt ran for it." };
    if (loading.value) return { title: "Loading", text: "Reading this model's compiled SQL from the run's manifest." };
    if (failed.value) return { title: "Could not load the compiled SQL", text: "The request failed. Try again, or check the browser console." };
    const error = result.value?.error;
    if (error === "disabled") return {
        title: "Compiled SQL is off for this task",
        text: "It can contain values passed with --vars, so it is only shown when the task opts in:",
        snippet: "outputCompiledSql: true",
    };
    if (error === "not_found") {
        if (node.status === "skipped") return { title: "Not compiled", text: "dbt skipped this model because a model it depends on failed, so it was never compiled." };
        if (node.status === "error") return { title: "Not compiled", text: "dbt failed before it could compile this model. The error is in the task logs." };
        return { title: "Not compiled", text: "dbt did not compile this model in this run." };
    }
    if (error) return { title: "No compiled SQL", text: MESSAGES[error] ?? result.value?.message ?? error };
    return null;
});

const copied = ref(false);
function copy() {
    const sql = result.value?.compiledSql;
    if (!sql) return;
    navigator.clipboard?.writeText(sql).then(() => {
        copied.value = true;
        setTimeout(() => (copied.value = false), 2000);
    }).catch(() => undefined);
}
</script>

<template>
    <p v-if="!fetchEndpoint" class="dbt-sql__message">This Kestra version cannot load compiled SQL yet.</p>
    <div v-else class="dbt-sql">
        <div class="dbt-sql__bar">
            <div ref="switcher" class="dbt-sql__switcher">
                <button type="button" class="dbt-sql__current" aria-haspopup="listbox" :aria-expanded="open" @click="toggleOpen">
                    <img :src="DBT_ICON" alt="" class="dbt-sql__item-icon">
                    <span v-if="node" class="dbt-sql__item-text">
                        <span class="dbt-sql__item-name">{{ node.name ?? node.uniqueId }}</span>
                        <span class="dbt-sql__item-meta"><template v-if="node.materialized">{{ node.materialized }} · </template><span :style="{ color: statusColor(node.status) }">{{ statusLabel(node.status) }}</span></span>
                    </span>
                    <span v-else class="dbt-sql__item-text"><span class="dbt-sql__item-name">Select a model</span></span>
                    <ChevronDown :size="18" :class="['dbt-sql__chevron', { 'dbt-sql__chevron--open': open }]" />
                </button>

                <div v-if="open" class="dbt-sql__popover">
                    <header class="dbt-sql__side-head">
                        <h5>Models <span class="dbt-sql__count">{{ total }}</span></h5>
                        <label class="dbt-sql__search">
                            <Magnify :size="16" />
                            <input ref="search" v-model="query" type="search" placeholder="Search by name" aria-label="Search models" @keydown="onKeydown">
                        </label>
                    </header>
                    <div ref="list" class="dbt-sql__list" role="listbox" tabindex="0" aria-label="Models" @keydown="onKeydown">
                        <section v-for="g in groups" :key="g.type" class="dbt-sql__group">
                            <button type="button" class="dbt-sql__group-head" :aria-expanded="!collapsed.has(g.type) || !!query" @click="toggle(g.type)">
                                <ChevronDown :size="16" :class="['dbt-sql__chevron', { 'dbt-sql__chevron--closed': collapsed.has(g.type) && !query }]" />
                                {{ g.label }}
                                <span class="dbt-sql__count">{{ g.nodes.length }}</span>
                            </button>
                            <template v-if="!collapsed.has(g.type) || query">
                                <button
                                    v-for="n in g.nodes"
                                    :key="n.uniqueId"
                                    type="button"
                                    role="option"
                                    :data-id="n.uniqueId"
                                    :aria-selected="n.uniqueId === node?.uniqueId"
                                    :class="['dbt-sql__item', { 'dbt-sql__item--active': n.uniqueId === node?.uniqueId }]"
                                    :title="n.uniqueId"
                                    @click="choose(n)"
                                >
                                    <img :src="DBT_ICON" alt="" class="dbt-sql__item-icon">
                                    <span class="dbt-sql__item-text">
                                        <span class="dbt-sql__item-name"><span v-for="(p, i) in parts(n.name ?? n.uniqueId)" :key="i" :class="{ 'dbt-sql__match': p.match }">{{ p.text }}</span></span>
                                        <span class="dbt-sql__item-meta">
                                            <template v-if="n.materialized">{{ n.materialized }} · </template><span :style="{ color: statusColor(n.status) }">{{ statusLabel(n.status) }}</span>
                                        </span>
                                    </span>
                                </button>
                            </template>
                        </section>
                        <p v-if="!groups.length" class="dbt-sql__message dbt-sql__none">No model matches "{{ query }}".</p>
                        <p v-if="!groups.length" class="dbt-sql__message dbt-sql__none">No model matches "{{ query }}".</p>
                    </div>
                </div>
            </div>

        </div>

        <section class="dbt-sql__main">
            <header v-if="node" class="dbt-sql__head">
                <p class="dbt-sql__title">
                    <KsExecutionStatus :status="kestraState(node.status)" :title="statusTitle(node.status)" size="small" />
                    <code>{{ node.uniqueId }}</code>
                </p>
                <button v-if="result?.compiledSql" type="button" class="dbt-sql__copy" aria-label="Copy to clipboard" title="Copy to clipboard" @click="copy">
                    <ContentCopy :size="16" />
                    <span v-if="copied" class="dbt-sql__copied">Copied</span>
                </button>
            </header>
            <div v-if="empty" class="dbt-sql__empty">
                <CodeBraces :size="22" class="dbt-sql__empty-icon" />
                <h4>{{ empty.title }}</h4>
                <p>{{ empty.text }}</p>
                <code v-if="empty.snippet">{{ empty.snippet }}</code>
            </div>
            <template v-else-if="result?.compiledSql !== undefined">
                <pre class="dbt-sql__code"><code><span v-for="(line, i) in lines" :key="i" class="dbt-sql__line"><span class="dbt-sql__text"><span v-for="(t, j) in line" :key="j" :class="`dbt-sql__t-${t.kind}`">{{ t.text }}</span>{{ "\n" }}</span></span></code></pre>
                <p v-if="result.truncated" class="dbt-sql__message">Truncated: this model's compiled SQL is longer than what the drawer shows.</p>
            </template>
        </section>
    </div>
</template>

<style>
/* Unscoped on purpose: scoped attributes do not survive the Module Federation boundary. */
.dbt-sql {
    display: flex;
    flex-direction: column;
    gap: 14px;
}

.dbt-sql__bar {
    display: flex;
    align-items: center;
    gap: 8px;
}

.dbt-sql__switcher {
    position: relative;
}

.dbt-sql__current {
    display: flex;
    align-items: center;
    gap: 10px;
    min-width: 280px;
    max-width: 420px;
    padding: 7px 10px;
    border: 1px solid var(--ks-border-default);
    border-radius: 10px;
    background: var(--ks-bg-surface);
    color: var(--ks-text-primary);
    font: inherit;
    text-align: left;
    cursor: pointer;
}

.dbt-sql__current:hover,
.dbt-sql__current[aria-expanded="true"] {
    border-color: var(--ks-border-primary, #8405ff);
}

.dbt-sql__current .dbt-sql__item-text {
    flex: 1;
}

.dbt-sql__current .dbt-sql__item-name {
    font-weight: 700;
}

.dbt-sql__chevron--open {
    transform: rotate(180deg);
}

.dbt-sql__popover {
    position: absolute;
    top: calc(100% + 6px);
    left: 0;
    z-index: 10;
    width: min(380px, 90vw);
    border: 1px solid var(--ks-border-default);
    border-radius: 10px;
    background: var(--ks-bg-surface);
    box-shadow: 0 16px 40px rgba(0, 0, 0, 0.3);
    overflow: hidden;
}





.dbt-sql__side-head {
    padding: 12px 12px 10px;
    border-bottom: 1px solid var(--ks-border-default);
}

.dbt-sql__side-head h5 {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 0 0 10px;
    font-size: 0.8125rem;
    font-weight: 600;
    color: var(--ks-text-primary);
}

.dbt-sql__count {
    padding: 0 7px;
    border-radius: var(--ks-radius-sm, 6px);
    background: var(--ks-bg-base);
    color: var(--ks-text-secondary);
    font-size: 0.6875rem;
    font-weight: 600;
    line-height: 18px;
}

.dbt-sql__search {
    display: flex;
    align-items: center;
    gap: 8px;
    height: 32px;
    padding: 0 10px;
    border: 1px solid var(--ks-border-default);
    border-radius: 8px;
    background: var(--ks-bg-base);
    color: var(--ks-text-secondary);
}

.dbt-sql__search:focus-within {
    border-color: var(--ks-border-primary, #8405ff);
}

.dbt-sql__search input {
    flex: 1;
    min-width: 0;
    border: 0;
    outline: none;
    background: transparent;
    color: var(--ks-text-primary);
    font: inherit;
    font-size: 0.8125rem;
}

.dbt-sql__list {
    max-height: 360px;
    overflow: auto;
    padding: 6px;
    outline: none;
}

.dbt-sql__group + .dbt-sql__group {
    margin-top: 4px;
}

.dbt-sql__group-head {
    display: flex;
    align-items: center;
    gap: 6px;
    width: 100%;
    padding: 6px 6px;
    border: 0;
    background: transparent;
    color: var(--ks-text-secondary);
    font: inherit;
    font-size: 0.6875rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    cursor: pointer;
}

.dbt-sql__group-head .dbt-sql__count {
    margin-left: auto;
    letter-spacing: 0;
}

.dbt-sql__chevron {
    display: inline-flex;
    transition: transform 0.15s ease;
}

.dbt-sql__chevron--closed {
    transform: rotate(-90deg);
}

.dbt-sql__item {
    position: relative;
    display: flex;
    align-items: center;
    gap: 10px;
    width: 100%;
    margin: 1px 0;
    padding: 7px 10px;
    border: 0;
    border-radius: 8px;
    background: transparent;
    color: var(--ks-text-primary);
    font: inherit;
    text-align: left;
    cursor: pointer;
}

.dbt-sql__item:hover {
    background: var(--ks-bg-hover, rgba(127, 127, 127, 0.1));
}

.dbt-sql__item--active,
.dbt-sql__item--active:hover {
    background: rgba(132, 5, 255, 0.12);
}

.dbt-sql__item--active::before {
    content: "";
    position: absolute;
    left: 0;
    top: 8px;
    bottom: 8px;
    width: 3px;
    border-radius: 2px;
    background: var(--ks-border-primary, #8405ff);
}

.dbt-sql__item-icon {
    flex: none;
    width: 24px;
    height: 24px;
}

.dbt-sql__item-text {
    display: flex;
    flex-direction: column;
    min-width: 0;
}

.dbt-sql__item-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 0.8125rem;
    font-weight: 500;
}

.dbt-sql__item--active .dbt-sql__item-name {
    font-weight: 700;
}

.dbt-sql__match {
    color: var(--ks-text-primary);
    text-decoration: underline;
    text-decoration-thickness: 2px;
    text-underline-offset: 2px;
}

.dbt-sql__item-meta {
    margin-top: 1px;
    color: var(--ks-text-secondary);
    font-size: 0.6875rem;
}

.dbt-sql__none {
    padding: 12px 8px;
}

.dbt-sql__main {
    display: flex;
    flex-direction: column;
    gap: 12px;
    min-width: 0;
}

.dbt-sql__head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
}

.dbt-sql__title {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
    min-width: 0;
    margin: 0;
}

.dbt-sql__title code {
    font-size: 0.75rem;
    color: var(--ks-text-secondary);
    overflow-wrap: anywhere;
}


.dbt-sql__code {
    margin: 0;
    max-height: 60vh;
    overflow-y: auto;
    overflow-x: hidden;
    padding: 12px 0;
    border: 1px solid var(--ks-border-default);
    border-radius: 8px;
    background: var(--ks-bg-base);
    color: var(--ks-text-primary);
    font-size: 0.8125rem;
    line-height: 1.6;
    counter-reset: dbt-line;
}

/* Long lines wrap instead of scrolling sideways; the number column keeps a wrapped line under its own text. */
.dbt-sql__line {
    display: grid;
    grid-template-columns: 3.5em minmax(0, 1fr);
    column-gap: 14px;
    padding-right: 16px;
}

.dbt-sql__line::before {
    counter-increment: dbt-line;
    content: counter(dbt-line);
    padding-right: 10px;
    border-right: 1px solid var(--ks-border-default);
    color: var(--ks-text-secondary);
    text-align: right;
    user-select: none;
    opacity: 0.6;
}

.dbt-sql__text {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
}

.dbt-sql__t-keyword {
    color: var(--ks-text-primary-alt, #a374ff);
    font-weight: 600;
}

.dbt-sql__t-string {
    color: var(--ks-text-success, #0f8a5f);
}

.dbt-sql__t-comment {
    color: var(--ks-text-secondary);
    font-style: italic;
}

.dbt-sql__t-number {
    color: var(--ks-text-warning, #b36b00);
}

.dbt-sql__empty {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 8px;
    min-height: 280px;
    padding: 32px 24px;
    border: 1px dashed var(--ks-border-default);
    border-radius: 8px;
    background: var(--ks-bg-base);
    text-align: center;
}

.dbt-sql__empty-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 48px;
    height: 48px;
    margin-bottom: 4px;
    border: 1px solid var(--ks-border-default);
    border-radius: 50%;
    background: var(--ks-bg-surface);
    color: var(--ks-text-secondary);
}

.dbt-sql__empty h4 {
    margin: 0;
    font-size: 0.9375rem;
    color: var(--ks-text-primary);
}

.dbt-sql__empty p {
    max-width: 420px;
    margin: 0;
    font-size: 0.8125rem;
    line-height: 1.5;
    color: var(--ks-text-secondary);
}

.dbt-sql__empty code {
    margin-top: 6px;
    padding: 4px 10px;
    border: 1px solid var(--ks-border-default);
    border-radius: var(--ks-radius-sm, 6px);
    background: var(--ks-bg-surface);
    font-size: 0.8125rem;
    color: var(--ks-text-primary);
}

.dbt-sql__message {
    margin: 0;
    color: var(--ks-text-secondary);
}

/* Borderless 2rem icon button, like Kestra's CopyToClipboard with `link`. */
.dbt-sql__copy {
    position: relative;
    flex: none;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 2rem;
    height: 2rem;
    padding: 0;
    border: 0;
    border-radius: 6px;
    background: transparent;
    color: var(--ks-text-secondary);
    cursor: pointer;
}

.dbt-sql__copy:hover {
    color: var(--ks-text-primary);
    background: var(--ks-bg-hover, rgba(127, 127, 127, 0.12));
}

.dbt-sql__copied {
    position: absolute;
    bottom: calc(100% + 4px);
    left: 50%;
    transform: translateX(-50%);
    padding: 2px 8px;
    border-radius: 4px;
    background: var(--ks-bg-elevated, #1e202a);
    color: var(--ks-text-primary);
    font-size: 0.75rem;
    white-space: nowrap;
}
</style>
