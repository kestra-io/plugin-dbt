<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { KsExecutionStatus, KsGraph, KsId } from "@kestra-io/design-system";
import ContentCopy from "vue-material-design-icons/ContentCopy.vue";
import type { KsGraphEdge, KsGraphNode } from "@kestra-io/design-system/components/Charts/KsGraph";
import { DBT_ICON as ICON, type GraphNode, type ModelGraph, initialNode, kestraState, statusColor, statusTitle } from "../composables/useModelGraph";

// `sqlAction` shows the Compiled SQL button, only where the host can switch to that view (the drawer tabs).
// `embedded` fills a host panel that already has a frame (the Artifacts tab): no own border, details on the side.
const props = defineProps<{ graph: ModelGraph; selectedId?: string; sqlAction?: boolean; embedded?: boolean }>();
const emit = defineEmits<{ select: [node: GraphNode | null]; "open-sql": [node: GraphNode] }>();

const NODE_WIDTH = 300;
const NODE_HEIGHT = 84;
const X_GAP = 340;
const Y_GAP = 156;
// A level wider than this wraps onto extra rows, so a project with 2,000 staging models stays viewable.
const MAX_ROW_NODES = 10;
const WRAP_GAP = 96;
// Cards are labels scaled with the zoom. Below FULL_CARD_ZOOM a card keeps only the model name, at a readable
// minimum size, and below LABEL_MIN_ZOOM (a large project seen whole) the graph switches to plain shapes.
const FULL_CARD_ZOOM = 0.6;
const LABEL_MIN_ZOOM = 0.25;
// A rounded rectangle drawn at the card's exact size, so corners stay 8px instead of ECharts' pill-shaped roundRect.
const CARD_PATH = `path://M8,0H${NODE_WIDTH - 8}A8,8,0,0,1,${NODE_WIDTH},8V${NODE_HEIGHT - 8}A8,8,0,0,1,${NODE_WIDTH - 8},${NODE_HEIGHT}H8A8,8,0,0,1,0,${NODE_HEIGHT - 8}V8A8,8,0,0,1,8,0Z`;

// Run status -> the topology tokens core uses for task nodes, so the cards match Kestra in light and dark.
const STATUSES: { key: string; border: string; borderFallback: string; bg: string; bgFallback: string }[] = [
    { key: "success", border: "--ks-topology-border-success", borderFallback: "#15b983", bg: "--ks-topology-bg-success", bgFallback: "#ecfff5" },
    { key: "error", border: "--ks-topology-border-errors", borderFallback: "#ed0011", bg: "--ks-topology-bg-errors", bgFallback: "rgba(247, 58, 61, 0.1)" },
    { key: "warn", border: "--ks-topology-border-killing", borderFallback: "#f0b429", bg: "--ks-topology-bg-killing", bgFallback: "#fffceb" },
    { key: "skipped", border: "--ks-topology-border-cancelled", borderFallback: "#9797a6", bg: "--ks-topology-bg-cancelled", bgFallback: "#e9e9ee" },
    { key: "not-run", border: "--ks-topology-border-cancelled", borderFallback: "#d0d0da", bg: "--ks-bg-surface", bgFallback: "#ffffff" },
];

function formatSeconds(seconds?: number): string | undefined {
    if (seconds === undefined) return undefined;
    if (seconds < 60) return `${seconds.toFixed(2)}s`;
    return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function token(name: string, fallback: string): string {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

// Some topology backgrounds are translucent tints (e.g. 10% red). A card must be opaque or the edges behind it
// show through, so the tint is blended onto the surface colour.
function opaque(color: string, surface: string): string {
    const tint = color.match(/rgba?\(\s*(\d+)[,\s]+(\d+)[,\s]+(\d+)[,\s/]+([\d.]+)\s*\)/);
    const base = surface.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
    if (!tint || !base) return color;
    const alpha = Number(tint[4]);
    const mix = (i: number) => Math.round(Number(tint[i]) * alpha + parseInt(base[i], 16) * (1 - alpha));
    return `rgb(${mix(1)}, ${mix(2)}, ${mix(3)})`;
}

function push<K, V>(map: Map<K, V[]>, key: K, value: V) {
    const list = map.get(key);
    if (list) list.push(value);
    else map.set(key, [value]);
}

// `{`, `}`, `|` and line breaks are ECharts rich-text syntax, so they must not reach the label formatter.
function plain(text: string): string {
    return text.replace(/[{}|\n]/g, " ");
}

function typeLabel(node: GraphNode): string {
    const type = node.resourceType ?? "node";
    return node.materialized ? `${type} · ${node.materialized}` : type;
}

// Top-down layout from the server-side `level`. Rows are ordered by the mean position of their neighbours
// (barycenter), sweeping down, up, then down again: this removes most edge crossings without a layout library.
function positions(graph: ModelGraph): Map<string, { x: number; y: number }> {
    const parents = new Map<string, string[]>();
    const children = new Map<string, string[]>();
    graph.edges.forEach((e) => {
        push(parents, e.to, e.from);
        push(children, e.from, e.to);
    });

    const byLevel = new Map<number, GraphNode[]>();
    graph.nodes.forEach((n) => push(byLevel, n.level, n));
    const levels = [...byLevel.keys()].sort((a, b) => a - b);

    const index = new Map<string, number>();
    const place = (row: GraphNode[]) => row.forEach((n, i) => index.set(n.uniqueId, i - (row.length - 1) / 2));
    const sweep = (order: number[], neighbours: Map<string, string[]>) =>
        order.forEach((level) => {
            const row = byLevel.get(level)!;
            const weights = new Map<string, number>();
            row.forEach((n) => {
                let sum = 0;
                let count = 0;
                for (const id of neighbours.get(n.uniqueId) ?? []) {
                    const at = index.get(id);
                    if (at !== undefined) {
                        sum += at;
                        count++;
                    }
                }
                weights.set(n.uniqueId, count ? sum / count : index.get(n.uniqueId) ?? 0);
            });
            row.sort((a, b) => weights.get(a.uniqueId)! - weights.get(b.uniqueId)! || a.uniqueId.localeCompare(b.uniqueId));
            place(row);
        });

    levels.forEach((level) => place(byLevel.get(level)!));
    sweep(levels.slice(1), parents);
    sweep([...levels].reverse().slice(1), children);
    sweep(levels.slice(1), parents);

    const result = new Map<string, { x: number; y: number }>();
    let top = 0;
    levels.forEach((level) => {
        const row = byLevel.get(level)!;
        const width = Math.min(row.length, MAX_ROW_NODES);
        row.forEach((n, i) => {
            result.set(n.uniqueId, {
                x: ((i % MAX_ROW_NODES) - (width - 1) / 2) * X_GAP,
                y: top + Math.floor(i / MAX_ROW_NODES) * WRAP_GAP,
            });
        });
        top += Math.floor((row.length - 1) / MAX_ROW_NODES) * WRAP_GAP + Y_GAP;
    });
    return result;
}

const placed = computed(() => positions(props.graph));
const nodesById = computed(() => new Map(props.graph.nodes.map((n) => [n.uniqueId, n])));
const selected = ref<GraphNode | null>(null);

// Layout extent in data units. ECharts widens a zero span (one column or one row) to 2, so never go below that.
const extent = computed(() => {
    const xs = [...placed.value.values()].map((p) => p.x);
    const ys = [...placed.value.values()].map((p) => p.y);
    const minX = Math.min(...xs);
    const minY = Math.min(...ys);
    return {
        spanX: Math.max(Math.max(...xs) - minX, 2),
        spanY: Math.max(Math.max(...ys) - minY, 2),
        center: [minX + (Math.max(...xs) - minX) / 2, minY + (Math.max(...ys) - minY) / 2],
    };
});

const canvas = ref<HTMLElement | null>(null);
const chart = ref<InstanceType<typeof KsGraph> | null>(null);
const canvasSize = ref({ width: 1000, height: 480 });
// Small projects get a short canvas instead of a mostly empty one, big ones stop at 70% of the viewport.
const canvasHeight = computed(() => `clamp(320px, ${extent.value.spanY + NODE_HEIGHT + 150}px, 70vh)`);

function fitZoom(): number {
    const { width, height } = canvasSize.value;
    return Math.min(1, (width - 64) / (extent.value.spanX + NODE_WIDTH), (height - 64) / (extent.value.spanY + NODE_HEIGHT));
}

function readZoom(): number {
    const series = chart.value?.getEchartsInstance()?.getOption()?.series as { zoom?: number }[] | undefined;
    return series?.[0]?.zoom ?? 1;
}
// Rounded to 5% steps, so a scroll only rebuilds the cards when their size visibly changes.
const zoom = ref(1);
const showLabels = computed(() => zoom.value >= LABEL_MIN_ZOOM);
function syncZoom() {
    requestAnimationFrame(() => {
        const next = Math.round(readZoom() * 20) / 20;
        if (next !== zoom.value) zoom.value = next;
    });
}
function setZoom(zoom: number, center?: number[]) {
    chart.value?.getEchartsInstance()?.setOption({ series: [{ zoom, ...(center ? { center } : {}) }] });
    syncZoom();
}
const zoomIn = () => setZoom(Math.min(3, readZoom() * 1.25));
const zoomOut = () => setZoom(Math.max(0.02, readZoom() / 1.25));
const fit = () => setZoom(fitZoom(), extent.value.center);

// The graph sits in a scrolling drawer, so a plain wheel scrolls the drawer like the rest of the page.
// Ctrl / ⌘ + wheel, and trackpad pinch (sent as a ctrl wheel), zoom the graph, as in embedded maps.
const zoomHint = ref(false);
let hintTimer: ReturnType<typeof setTimeout> | undefined;
function onWheel(event: WheelEvent) {
    if (event.ctrlKey || event.metaKey) {
        event.preventDefault();
        event.stopPropagation();
        markTouched();
        setZoom(Math.min(3, Math.max(0.02, readZoom() * Math.exp(-event.deltaY * 0.01))));
        return;
    }
    zoomHint.value = true;
    clearTimeout(hintTimer);
    hintTimer = setTimeout(() => (zoomHint.value = false), 1200);
}
onBeforeUnmount(() => clearTimeout(hintTimer));

// The drawer slides in, so the first sizes seen are too small: until the user zooms or pans, every resize
// refits the graph (zoomed out only when the project is bigger than the canvas).
let touched = false;
function markTouched() {
    touched = true;
}
function fitUntouched() {
    if (!touched) setZoom(Math.min(1, fitZoom()), extent.value.center);
}

// The drawer slides in from 0x0, and ECharts throws if its first render has no size, so the chart is only
// created once the box is measured, then resized on every later size change.
const sized = ref(false);
let observer: ResizeObserver | undefined;
onMounted(() => {
    observer = new ResizeObserver(() => {
        const width = canvas.value?.clientWidth ?? 0;
        const height = canvas.value?.clientHeight ?? 0;
        if (width === 0 || height === 0) return;
        canvasSize.value = { width, height };
        if (!sized.value) {
            sized.value = true;
            requestAnimationFrame(() => {
                fitUntouched();
                bindBlankClick();
            });
            return;
        }
        chart.value?.getEchartsInstance()?.resize();
        fitUntouched();
        bindBlankClick();
    });
    if (canvas.value) observer.observe(canvas.value);
});
onBeforeUnmount(() => observer?.disconnect());

const chartNodes = computed<KsGraphNode[]>(() => {
    const colors = {
        text: token("--ks-text-primary", "#1c1c1c"),
        muted: token("--ks-text-secondary", "#6b6b7b"),
        border: Object.fromEntries(STATUSES.map((s) => [s.key, token(s.border, s.borderFallback)])) as Record<string, string>,
        bg: {} as Record<string, string>,
    };
    // Cards sit on the canvas, so translucent tints are blended onto the canvas colour, as on the topology.
    const surface = token("--ks-bg-base", "#f7f7f8");
    STATUSES.forEach((s) => (colors.bg[s.key] = opaque(token(s.bg, s.bgFallback), surface)));
    // Labels do not scale with the chart, so the card is sized by hand to follow the zoom.
    const k = zoom.value;
    const selectedBorder = token("--ks-border-primary", "#8405ff");
    return props.graph.nodes.map((n) => {
        const status = colors.border[n.status] ? n.status : "not-run";
        const isSelected = selected.value?.uniqueId === n.uniqueId;
        const border = {
            // Selected takes the same purple as hover, the status stays readable from the card tint.
            borderColor: isSelected ? selectedBorder : colors.border[status],
            borderWidth: isSelected ? 2 : 1,
            borderType: n.resourceType === "source" ? "dashed" : "solid",
            // Selection is a firmer border and a soft neutral shadow, not a coloured glow.
            shadowBlur: isSelected ? 8 : 0,
            shadowColor: "rgba(0, 0, 0, 0.28)",
            shadowOffsetY: isSelected ? 2 : 0,
        } as const;
        if (!showLabels.value) {
            // Zoomed-out overview: plain cards that shrink with the zoom, no text.
            return {
                id: n.uniqueId,
                name: n.name ?? n.uniqueId,
                ...placed.value.get(n.uniqueId),
                itemStyle: { color: colors.bg[status], ...border },
                label: { show: false },
            };
        }
        if (k < FULL_CARD_ZOOM) {
            const fontSize = Math.max(10, 13 * k);
            return {
                id: n.uniqueId,
                name: n.name ?? n.uniqueId,
                ...placed.value.get(n.uniqueId),
                itemStyle: { color: "transparent", borderWidth: 0 },
                label: {
                    show: true,
                    position: "inside",
                    width: (NODE_WIDTH - 12) * k,
                    height: NODE_HEIGHT * k - 8,
                    padding: [4, 6],
                    overflow: "truncate",
                    backgroundColor: colors.bg[status],
                    borderRadius: 6,
                    ...border,
                    formatter: `{name|${plain(n.name ?? n.uniqueId)}}`,
                    rich: { name: { fontSize, fontWeight: 600, color: colors.text, lineHeight: NODE_HEIGHT * k - 8, align: "center" } },
                },
            };
        }
        // A skipped model still reports 0s: only show a duration for models that ran.
        const ran = n.status === "success" || n.status === "error" || n.status === "warn";
        const time = ran ? formatSeconds(n.executionTime) : undefined;
        // No unstyled text between pieces: it would take the label's centre alignment and split the line.
        const top = `{icon|}{name|${plain(n.name ?? n.uniqueId)}}` + (time ? `{time|${n.status === "error" ? "✕" : "✓"} ${time}}` : "");
        const bottom = `{indent|}{type|${plain(typeLabel(n))}}`;
        // The card itself is the label, drawn over the invisible square node set in `options`.
        return {
            id: n.uniqueId,
            name: n.name ?? n.uniqueId,
            ...placed.value.get(n.uniqueId),
            itemStyle: { color: "transparent", borderWidth: 0 },
            label: {
                show: true,
                position: "inside",
                width: (NODE_WIDTH - 28) * k,
                height: (NODE_HEIGHT - 24) * k,
                padding: [12 * k, 14 * k],
                overflow: "truncate",
                backgroundColor: colors.bg[status],
                borderRadius: 8 * k,
                ...border,
                formatter: `${top}\n${bottom}`,
                rich: {
                    icon: { width: 30 * k, height: 30 * k, backgroundColor: { image: ICON }, align: "left" },
                    name: { fontSize: 15 * k, fontWeight: 600, color: colors.text, padding: [0, 0, 0, 10 * k], align: "left" },
                    time: {
                        fontSize: 11 * k,
                        fontWeight: 600,
                        color: colors.text,
                        borderColor: colors.border[status],
                        borderWidth: 1,
                        borderRadius: 6 * k,
                        padding: [4 * k, 6 * k],
                        align: "right",
                    },
                    indent: { width: 40 * k, height: 18 * k, align: "left" },
                    type: { fontSize: 12 * k, color: colors.muted, lineHeight: 22 * k, align: "left" },
                },
            },
        };
    });
});

const chartEdges = computed<KsGraphEdge[]>(() =>
    props.graph.edges.map((e) => ({ source: e.from, target: e.to })),
);

const options = computed(() => {
    const { spanX, spanY } = extent.value;
    const { width, height } = canvasSize.value;
    const edge = token("--ks-topology-dash", "#adadbf");
    return {
        tooltip: { show: false },
        series: [{
            // ECharts trims edges by the mean of the symbol's width and height, so with cards the node is an
            // invisible square of the card's height and edges touch the card's top and bottom.
            symbol: showLabels.value ? "rect" : CARD_PATH,
            symbolSize: showLabels.value ? [NODE_HEIGHT, NODE_HEIGHT] : [NODE_WIDTH, NODE_HEIGHT],
            // The layout box is the graph at 1:1, centred: ECharts would otherwise squeeze every position into
            // the canvas while keeping cards at full size. nodeScaleRatio 1 shrinks cards along with the zoom.
            left: (width - spanX) / 2,
            top: (height - spanY) / 2,
            width: spanX,
            height: spanY,
            nodeScaleRatio: 1,
            cursor: "pointer",
            // ECharts 6 only pans from inside the series box (the area between node centres) unless told otherwise.
            roamTrigger: "global",
            draggable: true,
            lineStyle: { color: edge, width: 1.5, type: "dashed", curveness: 0.08, opacity: 0.9 },
            edgeSymbol: ["none", "arrow"],
            edgeSymbolSize: 10,
            // Hovered card lifts, so it reads as clickable.
            emphasis: { focus: "adjacency", lineStyle: { width: 2.5 }, label: { borderColor: token("--ks-border-primary", "#8405ff"), borderWidth: 2, shadowBlur: 6, shadowColor: "rgba(0, 0, 0, 0.2)", shadowOffsetY: 1 } },
            blur: { itemStyle: { opacity: 0.25 }, lineStyle: { opacity: 0.1 }, label: { opacity: 0.25 } },
            scaleLimit: { min: 0.02, max: 3 },
        }],
    };
});

const detailsCard = ref<HTMLElement | null>(null);

// The card opens below the graph, so the graph never moves: the drawer only scrolls if the card is out of view.
function select(node: GraphNode | null) {
    selected.value = node;
    emit("select", node);
    copied.value = null;
    if (node) nextTick(() => detailsCard.value?.scrollIntoView({ block: "nearest" }));
}

// Open with a model already selected (the first failure, else the first model), so details show at once and the
// highlighted card shows what clicking does.
onMounted(() => {
    const initial = initialNode(props.graph.nodes);
    selected.value = initial;
    emit("select", initial);
});

// A model picked in another tab becomes the selection here too.
watch(() => props.selectedId, (id) => {
    if (id && id !== selected.value?.uniqueId) selected.value = nodesById.value.get(id) ?? null;
});

function onNodeClick(node: KsGraphNode) {
    const next = nodesById.value.get(node.id) ?? null;
    select(selected.value?.uniqueId === next?.uniqueId ? null : next);
}

// A click on the empty canvas closes the panel, like Esc.
let blankClickBound = false;
function bindBlankClick() {
    const instance = chart.value?.getEchartsInstance();
    if (blankClickBound || !instance) return;
    instance.getZr().on("click", (event: { target?: unknown }) => {
        if (!event.target) select(null);
    });
    blankClickBound = true;
}
// The graph stays mounted behind a hidden drawer tab, so Esc only counts while it is on screen.
function onKeydown(event: KeyboardEvent) {
    if (event.key === "Escape" && canvas.value?.offsetParent) select(null);
}
onMounted(() => window.addEventListener("keydown", onKeydown));
onBeforeUnmount(() => window.removeEventListener("keydown", onKeydown));

const lineage = computed(() => {
    const id = selected.value?.uniqueId;
    if (!id) return { upstream: [], downstream: [] };
    const pick = (ids: string[]) => ids.map((i) => nodesById.value.get(i)).filter((n): n is GraphNode => !!n);
    return {
        upstream: pick(props.graph.edges.filter((e) => e.to === id).map((e) => e.from)),
        downstream: pick(props.graph.edges.filter((e) => e.from === id).map((e) => e.to)),
    };
});

// Same feedback as Kestra's CopyToClipboard: the icon stays, a "Copied" hint shows for 2 seconds.
const copied = ref<string | null>(null);
function copy(key: string, value?: string) {
    if (!value) return;
    navigator.clipboard?.writeText(value).then(() => {
        copied.value = key;
        setTimeout(() => {
            if (copied.value === key) copied.value = null;
        }, 2000);
    }).catch(() => undefined);
}
</script>

<template>
    <div :class="['dbt-graph', { 'dbt-graph--embedded': embedded }]">
        <!-- KsGraph sizes itself to 100% of its parent, so the height lives on this wrapper. -->
        <div ref="canvas" class="dbt-graph__canvas" :style="embedded ? undefined : { height: canvasHeight }" @wheel.capture="onWheel" @pointerdown.capture="markTouched">
            <KsGraph
                v-if="sized"
                ref="chart"
                roam="move"
                :nodes="chartNodes"
                :edges="chartEdges"
                :options="options"
                layout="none"
                @node-click="onNodeClick"
            />

            <p v-if="!embedded" class="dbt-graph__count">
                {{ graph.nodes.length }} {{ graph.nodes.length === 1 ? "node" : "nodes" }} · {{ graph.edges.length }}
                {{ graph.edges.length === 1 ? "dependency" : "dependencies" }}
            </p>

            <p v-if="zoomHint" class="dbt-graph__zoom-hint">Hold Ctrl or ⌘ and scroll to zoom</p>

            <div class="dbt-graph__toolbar">
                <button type="button" title="Zoom in" aria-label="Zoom in" @click="zoomIn">+</button>
                <button type="button" title="Zoom out" aria-label="Zoom out" @click="zoomOut">−</button>
                <button type="button" title="Fit to view" aria-label="Fit to view" @click="fit">⤢</button>
            </div>


        </div>
        <p v-if="!selected" class="dbt-graph__card dbt-graph__card--empty">
            Click a model in the graph to see its details.
        </p>
        <article v-else ref="detailsCard" class="dbt-graph__card">
                <header class="dbt-graph__card-head">
                    <img :src="ICON" alt="" class="dbt-graph__card-icon">
                    <div class="dbt-graph__card-title">
                        <span class="dbt-graph__card-caption">Selected model</span>
                        <h4>{{ selected.name ?? selected.uniqueId }}</h4>
                        <p>{{ typeLabel(selected) }}</p>
                    </div>
                    <KsExecutionStatus :status="kestraState(selected.status)" :title="statusTitle(selected.status)" size="small" />
                    <span v-if="selected.executionTime !== undefined && ['success', 'error', 'warn'].includes(selected.status)" class="dbt-graph__muted">{{ formatSeconds(selected.executionTime) }}</span>
                    <button v-if="sqlAction && (selected.resourceType === 'model' || selected.resourceType === 'snapshot')" type="button" class="dbt-graph__text-button" @click="emit('open-sql', selected)">Compiled SQL</button>
                    <button type="button" class="dbt-graph__icon-button dbt-graph__close" title="Close (Esc)" aria-label="Close" @click="select(null)">×</button>
                </header>

                <div class="dbt-graph__card-grid">
                    <section>
                        <h5>Unique ID</h5>
                        <div class="dbt-graph__copy-row">
                            <KsId :value="selected.uniqueId" :shrink="false" />
                            <button type="button" class="dbt-graph__copy" aria-label="Copy to clipboard" title="Copy to clipboard" @click="copy('id', selected.uniqueId)">
                                <ContentCopy :size="16" />
                                <span v-if="copied === 'id'" class="dbt-graph__copied">Copied</span>
                            </button>
                        </div>
                    </section>

                    <section v-if="selected.relation">
                        <h5>Relation</h5>
                        <div class="dbt-graph__copy-row">
                            <KsId :value="selected.relation" :shrink="false" />
                            <button type="button" class="dbt-graph__copy" aria-label="Copy to clipboard" title="Copy to clipboard" @click="copy('relation', selected.relation)">
                                <ContentCopy :size="16" />
                                <span v-if="copied === 'relation'" class="dbt-graph__copied">Copied</span>
                            </button>
                        </div>
                    </section>

                    <section v-for="group in [{ title: 'Upstream', nodes: lineage.upstream }, { title: 'Downstream', nodes: lineage.downstream }]" :key="group.title">
                        <h5>{{ group.title }} <span class="dbt-graph__muted">{{ group.nodes.length }}</span></h5>
                        <div v-if="group.nodes.length" class="dbt-graph__chips">
                            <button
                                v-for="n in group.nodes"
                                :key="n.uniqueId"
                                type="button"
                                class="dbt-graph__chip dbt-graph__chip--link"
                                :title="n.uniqueId"
                                @click="select(n)"
                            >
                                <i :style="{ background: statusColor(n.status) }" />{{ n.name ?? n.uniqueId }}
                            </button>
                        </div>
                        <p v-else class="dbt-graph__muted">None</p>
                    </section>

                    <section v-if="selected.tags?.length">
                        <h5>Tags</h5>
                        <div class="dbt-graph__chips">
                            <span v-for="tag in selected.tags" :key="tag" class="dbt-graph__chip">{{ tag }}</span>
                        </div>
                    </section>
                </div>
            </article>
    </div>
</template>

<style>
/* Unscoped on purpose: scoped attributes do not survive the Module Federation boundary. */
.dbt-graph {
    display: flex;
    flex-direction: column;
    gap: 16px;
}

.dbt-graph__canvas {
    position: relative;
    border: 1px solid var(--ks-border-default);
    border-radius: 8px;
    /* Same canvas as the execution topology: --ks-bg-base with --ks-topology-bg dots every 20px. */
    background-color: var(--ks-bg-base);
    background-image: radial-gradient(var(--ks-topology-bg) 1px, transparent 1px);
    background-size: 20px 20px;
    overflow: hidden;
}

/* Artifacts tab: core's panel is the frame, so the graph fills it edge to edge and details sit in a side column. */
.dbt-graph--embedded {
    flex-direction: row;
    gap: 0;
    height: 100%;
}

.dbt-graph--embedded .dbt-graph__canvas {
    flex: 1;
    min-width: 0;
    /* Guards against a host panel without a definite height: a 0px canvas never renders the graph. */
    min-height: 320px;
    border: 0;
    border-radius: 0;
}

.dbt-graph--embedded .dbt-graph__card {
    flex: 0 0 340px;
    margin: 0;
    overflow-y: auto;
    border: 0;
    border-left: 1px solid var(--ks-border-default);
    border-radius: 0;
}

/* The column is narrow: name and close on the first row, status and duration on a second row under the name. */
.dbt-graph--embedded .dbt-graph__card-head {
    flex-wrap: wrap;
    row-gap: 8px;
}

.dbt-graph--embedded .dbt-graph__card-title {
    flex: 1 1 calc(100% - 100px);
}

.dbt-graph--embedded .dbt-graph__close {
    order: 1;
}

.dbt-graph--embedded .dbt-graph__card-head > :not(.dbt-graph__card-icon):not(.dbt-graph__card-title):not(.dbt-graph__close) {
    order: 2;
}

.dbt-graph--embedded .dbt-graph__card-head > :nth-child(3) {
    margin-left: 48px;
}

.dbt-graph--embedded .dbt-graph__card--empty {
    display: flex;
    align-items: center;
    justify-content: center;
    border-style: solid;
}

.dbt-graph__zoom-hint {
    position: absolute;
    left: 50%;
    bottom: 16px;
    transform: translateX(-50%);
    z-index: 2;
    margin: 0;
    padding: 6px 12px;
    border-radius: var(--ks-radius-sm, 6px);
    background: var(--ks-bg-elevated, rgba(30, 32, 42, 0.92));
    color: var(--ks-text-primary);
    font-size: 0.8125rem;
    pointer-events: none;
}

.dbt-graph__toolbar {
    position: absolute;
    left: 12px;
    bottom: 12px;
    display: flex;
    flex-direction: column;
    border: 1px solid var(--ks-border-default);
    border-radius: 6px;
    overflow: hidden;
    background: var(--ks-bg-surface);
}

.dbt-graph__toolbar button {
    min-width: 30px;
    height: 28px;
    padding: 0 8px;
    border: 0;
    background: transparent;
    color: var(--ks-text-primary);
    font: inherit;
    font-size: 0.8125rem;
    cursor: pointer;
}

.dbt-graph__toolbar button + button {
    border-top: 1px solid var(--ks-border-default);
}

.dbt-graph__toolbar button:hover {
    background: var(--ks-bg-hover, rgba(127, 127, 127, 0.12));
}





.dbt-graph__card {
    padding: 16px 18px;
    border: 1px solid var(--ks-border-default);
    border-radius: 10px;
    background: var(--ks-bg-surface);
    font-size: 0.8125rem;
    color: var(--ks-text-primary);
}


.dbt-graph__card--empty {
    margin: 0;
    border-style: dashed;
    color: var(--ks-text-secondary);
    text-align: center;
}

.dbt-graph__card-head {
    display: flex;
    align-items: center;
    gap: 12px;
}

.dbt-graph__card-icon {
    width: 36px;
    height: 36px;
    flex: none;
}

.dbt-graph__card-title {
    flex: 1;
    min-width: 0;
}

.dbt-graph__card-caption {
    display: block;
    margin-bottom: 2px;
    font-size: 0.6875rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: var(--ks-text-secondary);
}


.dbt-graph__card-title h4 {
    margin: 0;
    font-size: 1rem;
    overflow-wrap: anywhere;
}

.dbt-graph__card-title p,
.dbt-graph__muted {
    margin: 2px 0 0;
    color: var(--ks-text-secondary);
}


.dbt-graph__card-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 20px 32px;
    margin-top: 20px;
}

.dbt-graph__card h5 {
    margin: 0 0 6px;
    font-size: 0.6875rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: var(--ks-text-secondary);
}

.dbt-graph__copy-row {
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
}

.dbt-graph__copy-row .ks-id {
    min-width: 0;
    white-space: normal;
    overflow-wrap: anywhere;
}

/* Borderless 2rem icon button, like Kestra's CopyToClipboard with `link`. */
.dbt-graph__copy {
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

.dbt-graph__copy:hover {
    color: var(--ks-text-primary);
    background: var(--ks-bg-hover, rgba(127, 127, 127, 0.12));
}

.dbt-graph__copied {
    position: absolute;
    bottom: calc(100% + 4px);
    left: 50%;
    transform: translateX(-50%);
    z-index: 1;
    padding: 2px 8px;
    border-radius: 4px;
    background: var(--ks-bg-elevated, #1e202a);
    color: var(--ks-text-primary);
    font-size: 0.75rem;
    white-space: nowrap;
}

.dbt-graph__icon-button {
    flex: none;
    min-width: 28px;
    height: 28px;
    padding: 0 8px;
    border: 1px solid var(--ks-border-default);
    border-radius: 6px;
    background: transparent;
    color: var(--ks-text-primary);
    font: inherit;
    font-size: 0.75rem;
    cursor: pointer;
}

.dbt-graph__text-button {
    flex: none;
    height: 28px;
    padding: 0 10px;
    border: 1px solid var(--ks-border-default);
    border-radius: 6px;
    background: transparent;
    color: var(--ks-text-primary);
    font: inherit;
    font-size: 0.75rem;
    cursor: pointer;
}

.dbt-graph__text-button:hover {
    background: var(--ks-bg-hover, rgba(127, 127, 127, 0.12));
}

.dbt-graph__close {
    border: 0;
    font-size: 1.125rem;
}

.dbt-graph__icon-button:hover,
.dbt-graph__chip--link:hover {
    background: var(--ks-bg-hover, rgba(127, 127, 127, 0.12));
}

.dbt-graph__chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
}

/* Same look as Kestra's default KsTag (plain effect), drawn here because the plugin's Element Plus copy is not styled by the host. */
.dbt-graph__chip {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    height: 24px;
    padding: 0 9px;
    border: 0;
    border-radius: 4px;
    background: var(--ks-bg-tag, rgba(127, 127, 127, 0.14));
    color: var(--ks-text-primary);
    font: inherit;
    font-size: 12px;
    font-weight: var(--ks-font-weight-semibold, 600);
    line-height: 1;
    white-space: nowrap;
}

.dbt-graph__chip--link {
    cursor: pointer;
}

.dbt-graph__chip i {
    width: 7px;
    height: 7px;
    border-radius: 50%;
}

.dbt-graph__count {
    position: absolute;
    top: 12px;
    left: 12px;
    margin: 0;
    padding: 2px 8px;
    border-radius: 6px;
    background: var(--ks-bg-surface);
    font-size: 0.75rem;
    color: var(--ks-text-secondary);
}
</style>
