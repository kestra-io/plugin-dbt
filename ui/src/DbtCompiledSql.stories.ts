import type { Meta, StoryObj } from "@storybook/vue3";
import DbtCompiledSql from "./components/DbtCompiledSql.vue";

const node = { uniqueId: "model.shop.orders", name: "orders", resourceType: "model", materialized: "table", status: "success", level: 2 };
const nodes = [
  node,
  { uniqueId: "model.shop.stg_orders", name: "stg_orders", resourceType: "model", materialized: "view", status: "success", level: 1 },
  { uniqueId: "model.shop.customers", name: "customers", resourceType: "model", materialized: "table", status: "error", level: 2 },
  { uniqueId: "snapshot.shop.orders_snapshot", name: "orders_snapshot", resourceType: "snapshot", status: "skipped", level: 3 },
  { uniqueId: "seed.shop.countries", name: "countries", resourceType: "seed", status: "success", level: 0 },
  { uniqueId: "source.shop.raw.orders", name: "orders", resourceType: "source", status: "not-run", level: 0 },
];

const SQL = `with stg_orders as (
    select * from "analytics"."staging"."stg_orders"
),

payments as (
    select order_id, sum(amount) as amount
    from "analytics"."staging"."stg_payments"
    group by 1
)

select stg_orders.*, payments.amount, case when payments.amount > 1000 then 'large' when payments.amount > 100 then 'medium' else 'small' end as order_size_bucket_used_for_reporting
from stg_orders
left join payments using (order_id)`;

const meta: Meta<typeof DbtCompiledSql> = {
  title: "Plugin UI / topology-task-drawer / DbtCompiledSql",
  component: DbtCompiledSql,
};

export default meta;
type Story = StoryObj<typeof DbtCompiledSql>;

export const Loaded: Story = {
  args: { node, nodes, fetchEndpoint: async () => ({ uniqueId: node.uniqueId, compiledSql: SQL, truncated: false }) },
};

export const Disabled: Story = {
  args: { node, nodes, fetchEndpoint: async () => ({ error: "disabled", message: "off" }) },
};

export const OlderKestra: Story = {
  args: { node, nodes },
};

export const SkippedModel: Story = {
  args: {
    node: { uniqueId: "model.shop.report", name: "report", resourceType: "model", materialized: "view", status: "skipped", level: 3 },
    nodes,
    fetchEndpoint: async () => ({ error: "not_found", message: "not compiled" }),
  },
};
