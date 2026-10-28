<script setup lang="ts">
import { computed } from "vue";
import type { KnownSlotProps } from "@kestra-io/artifact-sdk";
import TestTube from "vue-material-design-icons/TestTube.vue";
import TestTubeOff from "vue-material-design-icons/TestTubeOff.vue";

const props = defineProps<KnownSlotProps["asset-card-footer"]>();

const STATES = ["pass", "warn", "fail"] as const;
const count = (value: unknown): number | undefined => (typeof value === "number" ? value : undefined);

// All three keys or nothing, worded as core showed it before kestra#21101: missing or unknown values never read as a pass.
const badge = computed(() => {
    const metadata = props.metadata;
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
});
</script>

<template>
    <span v-if="badge" :class="['dbt-test-badge', badge.status]" :title="badge.tooltip">
        <component :is="badge.status === 'fail' ? TestTubeOff : TestTube" :size="12" class="dbt-test-badge__icon" />
        {{ badge.label }}
    </span>
</template>

<style>
.dbt-test-badge {
    display: flex;
    align-items: center;
    gap: var(--ks-spacing-2);
    font-size: var(--ks-font-size-xs);
    line-height: 1rem;
    color: var(--ks-text-secondary);
    white-space: nowrap;
}

.dbt-test-badge.warn {
    color: var(--ks-text-warning);
}

.dbt-test-badge.fail {
    color: var(--ks-text-error);
}

.dbt-test-badge.warn .dbt-test-badge__icon {
    color: var(--ks-status-warning);
}

.dbt-test-badge.fail .dbt-test-badge__icon {
    color: var(--ks-status-error);
}
</style>
