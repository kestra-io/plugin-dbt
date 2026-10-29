@PluginSubGroup(
    title = "dbt assets",
    description = "Asset types the dbt tasks emit for each model, seed, snapshot, test and exposure when `assets.enableAuto` is on.",
    categories = {
        PluginSubGroup.PluginCategory.DATA
    }
)
package io.kestra.plugin.dbt.assets;

import io.kestra.core.models.annotations.PluginSubGroup;
