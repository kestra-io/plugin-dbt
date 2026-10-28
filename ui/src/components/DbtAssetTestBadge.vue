<script lang="ts">
export type TestBadge = { status: "pass" | "warn" | "fail"; label: string; tooltip: string };

const STATES = ["pass", "warn", "fail"] as const;
const count = (value: unknown): number | undefined => (typeof value === "number" ? value : undefined);

/**
 * Test roll-up written by ResultParser (plugin-dbt#343), worded as core showed it before kestra#21101.
 * All three keys or nothing: no tests, cleared values or an unknown status render no badge, never a pass.
 */
export function testBadge(metadata: Record<string, unknown> | undefined): TestBadge | null {
    const total = count(metadata?.dbtTestsTotal);
    const failed = count(metadata?.dbtTestsFailed);
    const status = STATES.find((known) => known === metadata?.dbtTestStatus);
    // A test asset carries its own single result, the badge is for the node the tests cover.
    if (metadata?.dbtResourceType === "test" || !total || failed === undefined || !status) return null;
    const one = total === 1;
    const tests = `${total} ${one ? "test" : "tests"}`;
    switch (status) {
        case "pass":
            return { status, label: tests, tooltip: one ? "The 1 dbt test passed" : `All ${total} dbt tests passed` };
        case "warn":
            return {
                status,
                label: `${tests} · warn`,
                tooltip: one ? "1 dbt test passed with warnings" : `${total} dbt tests passed, some with warnings`,
            };
        case "fail":
            return { status, label: `${failed}/${total} failed`, tooltip: one ? "The dbt test failed" : `${failed} of ${total} dbt tests failed` };
    }
}
</script>

<script setup lang="ts">
import { computed } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import TestTube from "vue-material-design-icons/TestTube.vue";
import TestTubeOff from "vue-material-design-icons/TestTubeOff.vue";

const props = defineProps<KnownSlotProps["asset-card-footer"]>();

const badge = computed(() => testBadge(props.metadata));
// Colour is reserved for bad news: a pass keeps the muted card text colour.
const COLOR = { pass: undefined, warn: "var(--ks-status-warning)", fail: "var(--ks-status-error)" } as const;
</script>

<template>
    <span v-if="badge" class="dbt-test-badge" :title="badge.tooltip" :style="{ color: COLOR[badge.status] }">
        <component :is="badge.status === 'fail' ? TestTubeOff : TestTube" :size="14" />
        {{ badge.label }}
    </span>
</template>

<style>
.dbt-test-badge {
    display: flex;
    align-items: center;
    gap: 0.25rem;
    font-size: 0.75rem;
    line-height: 1.25rem;
    white-space: nowrap;
}
</style>
