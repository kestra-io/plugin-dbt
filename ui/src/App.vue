<script setup lang="ts">
import DbtDetailsDrawer from "./components/DbtDetailsDrawer.vue";
import { SLOTS } from "@kestra-io/artifact-sdk";
</script>

<template>
  <div style="padding: 1rem; font-family: sans-serif">
    <DbtDetailsDrawer v-bind="SLOTS['topology-task-drawer'].defaultProps" />
  </div>
</template>
