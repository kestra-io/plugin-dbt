import defaultViteConfig from "@kestra-io/artifact-sdk/vite.config";

export default defaultViteConfig({
  plugin: "io.kestra.plugin.dbt",
  
  exposes: {
    "cli.DbtCLI": [
      {
        slotName: "topology-details",
        path: "./src/components/CliDbtCLITopologyDetails.vue",
        additionalProperties: {
          // node base (56) + 1 row (Engine) x ~31px
          height: 90,
          // node base (56) + up to 5 rows (Engine, Models, Tests, Duration, Slowest) x ~31px
          heightWithExecution: 210,
          customAction: { label: "Show Details", taskProp: "", lang: "" },
        },
      },
      {
        slotName: "topology-task-modal",
        path: "./src/components/CliDbtCLITaskModal.vue",
      },
    ],
    ...Object.fromEntries(["cloud.TriggerRun", "cloud.CheckStatus"].map((task) => [task, [
      {
        slotName: "topology-details",
        path: "./src/components/CliDbtCLITopologyDetails.vue",
        additionalProperties: {
          // node base (56) + 1 row (Job) x ~31px
          height: 90,
          // node base (56) + up to 6 rows (Job, Status, Models, Tests, Duration, Slowest) x ~31px
          heightWithExecution: 240,
          customAction: { label: "Show Details", taskProp: "", lang: "" },
        },
      },
      {
        slotName: "topology-task-modal",
        path: "./src/components/CliDbtCLITaskModal.vue",
      },
    ]])),
  },
  
});
