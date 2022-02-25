package io.kestra.plugin.dbt.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractRun extends AbstractDbt {
    @Schema(
        title = "Specify number of threads to use while executing models."
    )
    @PluginProperty(dynamic = false)
    Integer thread;

    @Builder.Default
    @Schema(
        title = "If specified, dbt will drop incremental models and fully-recalculate the incremental table " +
            "from the model definition."
    )
    @PluginProperty(dynamic = false)
    Boolean fullRefresh = false;

    @Schema(
        title = "Which target to load for the given profile"
    )
    @PluginProperty(dynamic = true)
    String target;

    @Schema(
        title = "The selector name to use, as defined in selectors.yml"
    )
    @PluginProperty(dynamic = true)
    String selector;

    @Schema(
        title = "Specify the nodes to include"
    )
    @PluginProperty(dynamic = true)
    java.util.List<String> select;

    @Schema(
        title = "Specify the models to exclude"
    )
    @PluginProperty(dynamic = true)
    java.util.List<String> exclude;

    abstract protected String command();

    @Override
    protected java.util.List<String> commands(RunContext runContext) throws IllegalVariableEvaluationException {
        java.util.List<String> commands = new ArrayList<>(java.util.List.of(this.command()));

        if (this.thread != null) {
            commands.add("--thread=" + this.thread);
        }

        if (this.fullRefresh) {
            commands.add("--full-refresh");
        }

        if (this.target != null) {
            commands.add("--target=" + runContext.render(this.target));
        }

        if (this.selector != null) {
            commands.add("--selector=" + runContext.render(this.selector));
        }

        if (this.select != null) {
            commands.add("--select=" + String.join(" ", runContext.render(this.select)));
        }

        if (this.exclude != null) {
            commands.add("--exclude=" + String.join(" ", runContext.render(this.exclude)));
        }

        return commands;
    }
}
