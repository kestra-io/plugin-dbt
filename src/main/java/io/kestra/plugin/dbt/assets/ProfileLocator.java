package io.kestra.plugin.dbt.assets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.PebbleUtil;
import io.kestra.plugin.dbt.cli.DbtInvocations;

/**
 * Reads, from {@code profiles.yml}, where dbt connects: the profile picked by {@code --profile}, {@code DBT_PROFILE},
 * {@code dbt_project.yml} or the only one, and its target picked by {@code --target}, {@code DBT_TARGET} or its default.
 * A value that is an {@code env_var(...)} is read from the task's environment; any other Jinja cannot be resolved here,
 * and the target is then unknown, as it is when the commands that build or test pick different profiles or targets, a command picking none
 * counting as picking the default.
 */
public final class ProfileLocator {
    private static final Pattern ENV_VAR = Pattern.compile("^\\{\\{\\s*env_var\\(\\s*['\"]([^'\"]+)['\"]\\s*(?:,\\s*['\"]([^'\"]*)['\"]\\s*)?\\)\\s*}}$");
    private static final Pattern TARGET_FLAG = Pattern.compile("(?:^|\\s)(?:--target|-t)(?:\\s+|=)(\\S+)");
    private static final Pattern PROFILE_FLAG = Pattern.compile("(?:^|\\s)--profile(?:\\s+|=)(\\S+)");

    private ProfileLocator() {
    }

    /**
     * @param host the server host; the account for Snowflake and the project for BigQuery
     * @param database {@code null} for BigQuery
     */
    public record Target(String system, String host, String port, String database) {
    }

    /**
     * @param profiles the content of {@code profiles.yml}
     * @param projectDirectory where {@code dbt_project.yml} is, to read the profile it names
     * @param commands the dbt commands, for {@code --profile} and {@code --target}
     * @param env the environment the commands run with
     */
    @SuppressWarnings("unchecked")
    public static Optional<Target> locate(String profiles, Path projectDirectory, List<String> commands, Map<String, String> env) {
        if (profiles == null || profiles.isBlank()) {
            return Optional.empty();
        }
        try {
            List<String> connecting = DbtInvocations.parse(commands).stream()
                .filter(invocation -> invocation.builds() || "test".equals(invocation.subcommand()))
                .map(DbtInvocations.Invocation::text)
                .toList();
            Set<String> profileNames = flagValues(PROFILE_FLAG, connecting, envValue(env, "DBT_PROFILE"));
            if (profileNames.size() > 1) {
                return Optional.empty();
            }
            Map<String, Object> all = JacksonMapper.ofYaml().readValue(profiles, Map.class);
            Map<String, Object> profile = profile(all, projectDirectory, profileNames.isEmpty() ? envValue(env, "DBT_PROFILE") : profileNames.iterator().next());
            if (profile == null || !(profile.get("outputs") instanceof Map<?, ?> outputs)) {
                return Optional.empty();
            }
            String defaultTarget = Optional.ofNullable(envValue(env, "DBT_TARGET")).orElseGet(() -> resolve(profile.get("target"), env));
            Set<String> targetNames = flagValues(TARGET_FLAG, connecting, defaultTarget);
            if (targetNames.size() > 1) {
                return Optional.empty();
            }
            String targetName = targetNames.isEmpty() ? defaultTarget : targetNames.iterator().next();
            if (targetName == null || !(outputs.get(targetName) instanceof Map<?, ?> output)) {
                return Optional.empty();
            }
            return target((Map<String, Object>) output, env);
        } catch (IOException | RuntimeException e) {
            return Optional.empty();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> profile(Map<String, Object> all, Path projectDirectory, String name) throws IOException {
        if (name != null) {
            return all.get(name) instanceof Map<?, ?> named ? (Map<String, Object>) named : null;
        }
        Path project = projectDirectory == null ? null : projectDirectory.resolve("dbt_project.yml");
        if (project != null && Files.exists(project)) {
            Map<String, Object> dbtProject = JacksonMapper.ofYaml().readValue(Files.readString(project), Map.class);
            if (dbtProject.get("profile") instanceof String projectProfile) {
                return all.get(projectProfile) instanceof Map<?, ?> named ? (Map<String, Object>) named : null;
            }
        }
        List<Object> profiles = all.entrySet().stream().filter(entry -> !"config".equals(entry.getKey())).map(Map.Entry::getValue).toList();
        return profiles.size() == 1 && profiles.getFirst() instanceof Map<?, ?> only ? (Map<String, Object>) only : null;
    }

    private static Set<String> flagValues(Pattern flag, List<String> invocations, String fallback) {
        Set<String> values = new HashSet<>();
        for (String invocation : invocations) {
            Matcher matcher = flag.matcher(invocation);
            values.add(matcher.find() ? matcher.group(1) : fallback);
        }
        return values;
    }

    private static String envValue(Map<String, String> env, String name) {
        String value = env == null ? null : env.get(name);
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static Optional<Target> target(Map<String, Object> output, Map<String, String> env) {
        String type = resolve(output.get("type"), env);
        if (type == null) {
            return Optional.empty();
        }
        String system = type.toLowerCase(Locale.ROOT);
        String host;
        String port = resolve(output.get("port"), env);
        String database;
        switch (system) {
            case "snowflake" -> {
                host = resolve(output.get("account"), env);
                database = resolve(output.get("database"), env);
            }
            case "bigquery" -> {
                host = first(resolve(output.get("project"), env), resolve(output.get("database"), env));
                database = null;
            }
            case "sqlserver", "synapse", "fabric" -> {
                host = first(resolve(output.get("server"), env), resolve(output.get("host"), env));
                database = resolve(output.get("database"), env);
            }
            case "mysql", "mariadb" -> {
                host = first(resolve(output.get("server"), env), resolve(output.get("host"), env));
                database = first(resolve(output.get("database"), env), resolve(output.get("schema"), env));
            }
            default -> {
                host = resolve(output.get("host"), env);
                database = first(resolve(output.get("dbname"), env), resolve(output.get("database"), env));
            }
        }
        return host == null ? Optional.empty() : Optional.of(new Target(system, host, port, database));
    }

    /** @return the value, {@code null} when absent or when it holds Jinja other than one {@code env_var(...)} */
    private static String resolve(Object value, Map<String, String> env) {
        if (value == null) {
            return null;
        }
        String text = value.toString().trim();
        Matcher matcher = ENV_VAR.matcher(text);
        if (matcher.matches()) {
            String resolved = env == null ? null : env.get(matcher.group(1));
            return resolved != null ? resolved : matcher.group(2);
        }
        return PebbleUtil.containsOpeningBlockDelimiter(text) || text.isEmpty() ? null : text;
    }

    private static String first(String value, String fallback) {
        return value != null ? value : fallback;
    }
}
