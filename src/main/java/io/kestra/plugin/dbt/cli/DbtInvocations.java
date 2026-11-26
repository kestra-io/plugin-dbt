package io.kestra.plugin.dbt.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Finds the dbt calls in shell commands such as {@code cd carts && uv run dbt --profiles-dir . build}.
 */
public final class DbtInvocations {
    private static final Pattern SEPARATOR = Pattern.compile("&&|\\|\\||;|\\n");
    private static final Set<String> SUBCOMMANDS = Set.of(
        "build", "clean", "clone", "compile", "debug", "deps", "docs", "freshness", "init", "list", "ls", "parse", "retry", "run", "run-operation", "seed", "show",
        "snapshot", "source", "test"
    );
    private static final Set<String> RUN_RESULTS_WRITERS = Set.of("build", "clone", "compile", "docs", "retry", "run", "run-operation", "seed", "show", "snapshot", "test");
    private static final Set<String> BUILDING = Set.of("build", "clone", "retry", "run", "seed", "snapshot");

    private DbtInvocations() {
    }

    /**
     * @param text the part of the command from {@code dbt} to the next shell separator
     */
    public record Invocation(String subcommand, String text) {
        public boolean writesRunResults() {
            return RUN_RESULTS_WRITERS.contains(subcommand);
        }

        public boolean builds() {
            return BUILDING.contains(subcommand);
        }
    }

    public static List<Invocation> parse(List<String> commands) {
        List<Invocation> invocations = new ArrayList<>();
        for (String command : commands == null ? List.<String> of() : commands) {
            for (String segment : SEPARATOR.split(command)) {
                List<String> tokens = Arrays.asList(segment.trim().split("\\s+"));
                int dbt = tokens.indexOf("dbt");
                if (dbt < 0) {
                    continue;
                }
                List<String> arguments = tokens.subList(dbt + 1, tokens.size());
                arguments.stream()
                    .filter(SUBCOMMANDS::contains)
                    .findFirst()
                    .ifPresent(subcommand -> invocations.add(new Invocation(subcommand, String.join(" ", tokens.subList(dbt, tokens.size())))));
            }
        }
        return invocations;
    }
}
