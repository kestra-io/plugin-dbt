<script setup lang="ts">
import CliDbtCLITopologyDetails from "./components/CliDbtCLITopologyDetails.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";
</script>

<template>
  <div style="padding: 1rem; font-family: sans-serif">
    <CliDbtCLITopologyDetails v-bind="SLOTS['topology-details'].defaultProps" />
  </div>
</template>
