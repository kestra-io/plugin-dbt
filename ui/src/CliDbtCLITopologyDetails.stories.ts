import type { Meta, StoryObj } from "@storybook/vue3";
import CliDbtCLITopologyDetails from "./components/CliDbtCLITopologyDetails.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";

const task = { id: "dbt", type: "io.kestra.plugin.dbt.cli.DbtCLI", engine: "CORE" };

const outputs = {
  runSummary: {
    total: 200, success: 197, error: 1, warn: 0, skipped: 2, elapsedTime: 84.2,
    slowest: [
      { uniqueId: "model.shop.orders", executionTime: 42.0 },
      { uniqueId: "model.shop.payments", executionTime: 11.3 },
      { uniqueId: "model.shop.customers", executionTime: 4.1 },
    ],
  },
  testSummary: { total: 200, pass: 198, fail: 2, warn: 0, error: 0, skipped: 0 },
};

const execution = {
  id: "exec-1",
  taskRunList: [{ id: "tr-1", taskId: "dbt", state: { current: "FAILED" } }],
};

const meta: Meta<typeof CliDbtCLITopologyDetails> = {
  title: "Plugin UI / topology-details / CliDbtCLITopologyDetails",
  component: CliDbtCLITopologyDetails,
  tags: ["autodocs"],
};

export default meta;
type Story = StoryObj<typeof CliDbtCLITopologyDetails>;

export const BeforeRun: Story = {
  args: { ...SLOTS["topology-details"].defaultProps, task },
};

export const AfterRun: Story = {
  args: {
    ...SLOTS["topology-details"].defaultProps,
    task,
    execution,
    fetchOutputs: async () => outputs,
  } as any,
};
