import type { Meta, StoryObj } from "@storybook/vue3";
import DbtModelGraph from "./components/DbtModelGraph.vue";
import type { ModelGraph } from "./composables/useModelGraph";

const node = (uniqueId: string, level: number, status: string, extra: Partial<ModelGraph["nodes"][number]> = {}) => ({
  uniqueId,
  name: uniqueId.split(".").pop(),
  resourceType: uniqueId.split(".")[0],
  materialized: uniqueId.startsWith("model.") ? "table" : undefined,
  relation: `analytics.prod.${uniqueId.split(".").pop()}`,
  status,
  level,
  ...extra,
});

const graph: ModelGraph = {
  nodes: [
    node("source.shop.raw.orders", 0, "not-run"),
    node("source.shop.raw.customers", 0, "not-run"),
    node("seed.shop.countries", 0, "success"),
    node("model.shop.stg_orders", 1, "success"),
    node("model.shop.stg_customers", 1, "success"),
    node("model.shop.orders", 2, "success", { tags: ["nightly"], executionTime: 42.1 }),
    node("model.shop.customers", 2, "error", { executionTime: 3.2 }),
    node("model.shop.revenue", 3, "skipped"),
    node("snapshot.shop.orders_snapshot", 3, "not-run"),
  ],
  edges: [
    { from: "source.shop.raw.orders", to: "model.shop.stg_orders" },
    { from: "source.shop.raw.customers", to: "model.shop.stg_customers" },
    { from: "model.shop.stg_orders", to: "model.shop.orders" },
    { from: "seed.shop.countries", to: "model.shop.orders" },
    { from: "model.shop.stg_customers", to: "model.shop.customers" },
    { from: "model.shop.orders", to: "model.shop.revenue" },
    { from: "model.shop.customers", to: "model.shop.revenue" },
    { from: "model.shop.orders", to: "snapshot.shop.orders_snapshot" },
  ],
};

const meta: Meta<typeof DbtModelGraph> = {
  title: "Plugin UI / topology-task-drawer / DbtModelGraph",
  component: DbtModelGraph,
  tags: ["autodocs"],
};

export default meta;
type Story = StoryObj<typeof DbtModelGraph>;

export const AfterRun: Story = {
  args: { graph },
};

// ~2,000 nodes: 300 sources, 1,200 staging models, 500 marts, to check wrapping and render time.
const large: ModelGraph = { nodes: [], edges: [] };
for (let i = 0; i < 300; i++) large.nodes.push(node(`source.shop.raw.t${i}`, 0, "not-run"));
for (let i = 0; i < 1200; i++) {
  large.nodes.push(node(`model.shop.stg_${i}`, 1, i % 97 === 0 ? "error" : "success"));
  large.edges.push({ from: `source.shop.raw.t${i % 300}`, to: `model.shop.stg_${i}` });
}
for (let i = 0; i < 500; i++) {
  large.nodes.push(node(`model.shop.mart_${i}`, 2, "success"));
  large.edges.push({ from: `model.shop.stg_${i}`, to: `model.shop.mart_${i}` }, { from: `model.shop.stg_${i + 600}`, to: `model.shop.mart_${i}` });
}

export const LargeProject: Story = {
  args: { graph: large },
};

// One column (every node at the same x), as in dbt's starter project: the layout box has zero width.
export const SingleColumn: Story = {
  args: {
    graph: {
      nodes: [
        node("model.my_dbt_project.my_first_dbt_model", 0, "success", { executionTime: 0.42 }),
        node("model.my_dbt_project.my_second_dbt_model", 1, "success", { executionTime: 0.18 }),
      ],
      edges: [{ from: "model.my_dbt_project.my_first_dbt_model", to: "model.my_dbt_project.my_second_dbt_model" }],
    },
  },
};

export const SingleNode: Story = {
  args: { graph: { nodes: [node("model.shop.orders", 0, "error")], edges: [] } },
};
