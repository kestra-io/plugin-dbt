import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";

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
    ],
  },
});
