import type { Meta, StoryObj } from "@storybook/vue3";
import DbtAssetTestBadge from "./components/DbtAssetTestBadge.vue";

const model = (metadata: Record<string, unknown>) => ({
  args: {
    assetId: "shop.main.stg_customers",
    assetType: "io.kestra.plugin.dbt.assets.DbtModel",
    metadata: { dbtResourceType: "model", ...metadata },
  },
});

const meta: Meta<typeof DbtAssetTestBadge> = {
  title: "Plugin UI / asset-card-footer / DbtAssetTestBadge",
  component: DbtAssetTestBadge,
};
export default meta;

type Story = StoryObj<typeof DbtAssetTestBadge>;

export const Pass: Story = model({ dbtTestStatus: "pass", dbtTestsTotal: 22, dbtTestsFailed: 0 });
export const Warn: Story = model({ dbtTestStatus: "warn", dbtTestsTotal: 22, dbtTestsFailed: 0 });
export const Fail: Story = model({ dbtTestStatus: "fail", dbtTestsTotal: 22, dbtTestsFailed: 2 });
export const SingleTest: Story = model({ dbtTestStatus: "pass", dbtTestsTotal: 1, dbtTestsFailed: 0 });
// Renders nothing: no tests ran, cleared to null, or an unknown status.
export const NoTests: Story = model({});
export const Cleared: Story = model({ dbtTestStatus: null, dbtTestsTotal: null, dbtTestsFailed: null });
export const UnknownStatus: Story = model({ dbtTestStatus: "error", dbtTestsTotal: 3, dbtTestsFailed: 1 });
export const TestAsset: Story = {
  args: {
    assetId: "shop.main_dbt_test__audit.unique_stg_customers_id",
    assetType: "io.kestra.plugin.dbt.assets.DbtTest",
    metadata: { dbtResourceType: "test", dbtTestStatus: "pass", dbtTestsTotal: 1, dbtTestsFailed: 0 },
  },
};
