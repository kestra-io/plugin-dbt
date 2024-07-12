package io.kestra.plugin.dbt.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.file.Path;
import java.util.ArrayList;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractRun extends AbstractDbt {
    @Schema(
        title = "Specify the number of threads to use while executing models."
    )
    @PluginProperty
    Integer thread;

    @Builder.Default
    @Schema(
        title = "Whether dbt will drop incremental models and fully-recalculate the incremental table " +
            "from the model definition."
    )
    @PluginProperty
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
        title = "List of nodes to include"
    )
    @PluginProperty(dynamic = true)
    java.util.List<String> select;

    @Schema(
        title = "List of models to exclude"
    )
    @PluginProperty(dynamic = true)
    java.util.List<String> exclude;

    abstract protected String dbtCommand();

    @Override
    protected java.util.List<String> dbtCommands(RunContext runContext) throws IllegalVariableEvaluationException {
        java.util.List<String> commands = new ArrayList<>(java.util.List.of(
            this.dbtCommand(),
            "--profiles-dir {{" + ScriptService.VAR_WORKING_DIR + "}}/.profile"));

        if (this.thread != null) {
            commands.add("--threads " + this.thread);
        }

        if (this.fullRefresh) {
            commands.add("--full-refresh");
        }

        if (this.target != null) {
            commands.add("--target " + runContext.render(this.target));
        }

        if (this.selector != null) {
            commands.add("--selector " + runContext.render(this.selector));
        }

        if (this.select != null) {
            commands.add("--select " + String.join(" ", runContext.render(this.select)));
        }

        if (this.exclude != null) {
            commands.add("--exclude " + String.join(" ", runContext.render(this.exclude)));
        }

        return commands;
    }
}
