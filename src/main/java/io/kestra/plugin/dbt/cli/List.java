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
    title = "Invoke dbt `list` command"
)
public class List extends AbstractRun {
    @Override
    protected String command() {
        return "list";
    }

    @Override
    protected void parseResults(RunContext runContext) {
        // 'dbt list' didn't return any result files.
    }
}
