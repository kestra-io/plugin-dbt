package io.kestra.plugin.dbt.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractRun extends AbstractDbt {
    @Schema(
        title = "Specify the number of threads to use while executing models."
    )
    Property<Integer> thread;

    @Builder.Default
    @Schema(
        title = "Whether dbt will drop incremental models and fully-recalculate the incremental table " +
            "from the model definition."
    )
    Property<Boolean> fullRefresh = Property.ofValue(Boolean.FALSE);

    @Schema(
        title = "Which target to load for the given profile"
    )
    Property<String> target;

    @Schema(
        title = "The selector name to use, as defined in selectors.yml"
    )
    Property<String> selector;

    @Schema(
        title = "List of nodes to include"
    )
    Property<List<String>> select;

    @Schema(
        title = "List of models to exclude"
    )
    Property<List<String>> exclude;

    abstract protected String dbtCommand();

    @Override
    protected java.util.List<String> dbtCommands(RunContext runContext) throws IllegalVariableEvaluationException {
        java.util.List<String> commands = new ArrayList<>(java.util.List.of(
            this.dbtCommand(),
            "--profiles-dir {{" + ScriptService.VAR_WORKING_DIR + "}}/.profile"));

        if (runContext.render(this.thread).as(Integer.class).isPresent()) {
            commands.add("--threads " + runContext.render(this.thread).as(Integer.class).get());
        }

        if (runContext.render(this.fullRefresh).as(Boolean.class).orElse(false)) {
            commands.add("--full-refresh");
        }

        if (runContext.render(this.target).as(String.class).isPresent()) {
            commands.add("--target " + runContext.render(this.target).as(String.class).get());
        }

        if (runContext.render(this.selector).as(String.class).isPresent()) {
            commands.add("--selector " + runContext.render(this.target).as(String.class).get());
        }

        if (!runContext.render(this.select).asList(String.class).isEmpty()) {
            commands.add("--select " + String.join(" ", runContext.render(this.select).asList(String.class)));
        }

        if (!runContext.render(this.exclude).asList(String.class).isEmpty()) {
            commands.add("--exclude " + String.join(" ", runContext.render(this.exclude).asList(String.class)));
        }

        return commands;
    }
}
