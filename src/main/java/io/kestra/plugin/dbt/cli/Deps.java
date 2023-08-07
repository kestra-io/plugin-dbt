package io.kestra.plugin.dbt.cli;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Invoke dbt `deps` command"
)
public class Deps extends AbstractRun {
    @Override
    protected String command() {
        return "deps";
    }

    @Override
    protected void parseResults(RunContext runContext) {
        // 'dbt deps' didn't return any result files.
    }
}
