import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";

// Core picks the asset card footer by the task that produced the asset, so every dbt task that emits assets lists it.
const assetCardFooter = {
  slotName: "asset-card-footer" as const,
  path: "./src/components/DbtAssetTestBadge.vue",
  // A 1rem line plus the card body's 0.25rem gap.
  additionalProperties: { footerHeight: 20 },
};

const ASSET_PRODUCERS = [
  "cli.Build",
  "cli.Compile",
  "cli.Run",
  "cli.Seed",
  "cli.Snapshot",
  "cli.Test",
  "cloud.CheckStatus",
  "cloud.TriggerRun",
];

export default defaultViteConfig({
  plugin: "io.kestra.plugin.dbt",

  exposes: {
    "cli.DbtCLI": [
      {
        slotName: "topology-details",
        path: "./src/components/DbtShowDetails.vue",
        additionalProperties: {
          // A non-empty taskProp opens the right drawer; core shows that property above the drawer tabs.
          customAction: { label: "Show details", taskProp: "commands", lang: "yaml" },
        },
      },
      {
        slotName: "topology-task-drawer",
        path: "./src/components/DbtDetailsDrawer.vue",
      },
      // One entry per artifact in the execution Artifacts tab (kestra-io/kestra#21611).
      {
        slotName: "execution-artifact/model-graph",
        path: "./src/components/DbtModelGraphArtifact.vue",
        additionalProperties: { title: "Model graph" },
      },
      {
        slotName: "execution-artifact/compiled-sql",
        path: "./src/components/DbtCompiledSqlArtifact.vue",
        additionalProperties: { title: "Compiled SQL" },
      },
      assetCardFooter,
    ],
    ...Object.fromEntries(ASSET_PRODUCERS.map((task) => [task, [assetCardFooter]])),
  },
});
