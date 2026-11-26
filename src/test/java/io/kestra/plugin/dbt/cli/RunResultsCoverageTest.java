package io.kestra.plugin.dbt.cli;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RunResultsCoverageTest {
    @Test
    void shouldReadTheSubcommandAfterTheValueOfAGlobalFlag() {
        assertThat(DbtCLI.runResultsCoverRun(List.of("dbt --profiles-dir . deps", "dbt run"))).isTrue();
        assertThat(DbtCLI.runResultsCoverRun(List.of("dbt --profiles-dir . seed", "dbt run"))).isFalse();
    }

    @Test
    void shouldNotCountTheCommandsThatWriteNoRunResultsOrRunBeforeTheBuild() {
        assertThat(DbtCLI.runResultsCoverRun(List.of("dbt run", "dbt source freshness"))).isTrue();
        assertThat(DbtCLI.runResultsCoverRun(List.of("dbt deps", "dbt run-operation stage", "dbt debug", "dbt ls", "dbt build"))).isTrue();
    }

    @Test
    void shouldCountACommandThatOverwritesRunResultsAfterTheBuild() {
        assertThat(DbtCLI.runResultsCoverRun(List.of("dbt build", "dbt docs generate"))).isFalse();
        assertThat(DbtCLI.runResultsCoverRun(List.of("dbt run", "dbt test"))).isFalse();
    }

    @Test
    void shouldFindEveryDbtCallOfACommand() {
        assertThat(DbtCLI.runResultsCoverRun(List.of("cd carts && dbt seed && dbt run"))).isFalse();
        assertThat(DbtCLI.runResultsCoverRun(List.of("dbt seed; dbt run || true"))).isFalse();
        assertThat(DbtCLI.runResultsCoverRun(List.of("uv run dbt seed", "uv run dbt run"))).isFalse();
    }
}
