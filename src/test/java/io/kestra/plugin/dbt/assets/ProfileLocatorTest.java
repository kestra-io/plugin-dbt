package io.kestra.plugin.dbt.assets;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProfileLocatorTest {
    private static final String PROFILES = """
        carts:
          target: dev
          outputs:
            dev:
              type: postgres
              host: "{{ env_var('PG_HOST') }}"
              port: 5432
              user: dbt
              password: "{{ env_var('PG_PASSWORD') }}"
              dbname: analytics
              schema: marts
            prod:
              type: postgres
              host: "{{ env_var('PROD_HOST', 'analytics-prod.internal') }}"
              dbname: analytics
        other:
          target: dev
          outputs:
            dev:
              type: postgres
              host: other.internal
              dbname: other
        """;

    @Test
    void shouldReadTheTargetOfTheProfileTheProjectNames() throws Exception {
        Path project = project("carts");

        assertThat(ProfileLocator.locate(PROFILES, project, List.of("dbt build"), Map.of("PG_HOST", "db.internal")))
            .contains(new ProfileLocator.Target("postgres", "db.internal", "5432", "analytics"));
    }

    @Test
    void shouldReadTheTargetTheCommandsPickAndTheDefaultOfAnEnvVar() throws Exception {
        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt deps", "dbt build --target prod"), Map.of()))
            .contains(new ProfileLocator.Target("postgres", "analytics-prod.internal", null, "analytics"));
    }

    @Test
    void shouldReadTheTargetOfDbtTargetUnlessTheCommandsPickOne() throws Exception {
        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt build"), Map.of("DBT_TARGET", "prod")))
            .contains(new ProfileLocator.Target("postgres", "analytics-prod.internal", null, "analytics"));
        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt build --target dev"), Map.of("DBT_TARGET", "prod", "PG_HOST", "db.internal")))
            .contains(new ProfileLocator.Target("postgres", "db.internal", "5432", "analytics"));
    }

    @Test
    void shouldReadTheProfileTheCommandsOrDbtProfileNameOverTheProject() throws Exception {
        ProfileLocator.Target other = new ProfileLocator.Target("postgres", "other.internal", null, "other");

        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt build --profiles-dir . --profile other"), Map.of())).contains(other);
        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt build"), Map.of("DBT_PROFILE", "other"))).contains(other);
    }

    @Test
    void shouldGiveNothingWhenTheCommandsPickDifferentTargetsOrProfiles() throws Exception {
        Map<String, String> env = Map.of("PG_HOST", "db.internal");

        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt run --target dev", "dbt test --target prod"), env)).isEmpty();
        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt run --profile carts", "dbt test --profile other"), env)).isEmpty();
    }

    @Test
    void shouldGiveNothingWhenTheHostCannotBeResolved() throws Exception {
        assertThat(ProfileLocator.locate(PROFILES, project("carts"), List.of("dbt build"), Map.of())).isEmpty();
        assertThat(ProfileLocator.locate(PROFILES, null, List.of("dbt build"), Map.of("PG_HOST", "db.internal"))).isEmpty();
        assertThat(ProfileLocator.locate("not: [yaml", null, List.of(), Map.of())).isEmpty();
    }

    @Test
    void shouldReadTheAccountOfSnowflakeAndTheProjectOfBigQuery() {
        String snowflake = """
            warehouse:
              target: prod
              outputs:
                prod: {type: snowflake, account: MyOrg-MyAccount, database: ANALYTICS, warehouse: WH}
            """;
        String bigquery = """
            warehouse:
              target: prod
              outputs:
                prod: {type: bigquery, method: oauth, project: my-gcp-project, dataset: marts}
            """;

        assertThat(ProfileLocator.locate(snowflake, null, List.of(), Map.of())).contains(new ProfileLocator.Target("snowflake", "MyOrg-MyAccount", null, "ANALYTICS"));
        assertThat(ProfileLocator.locate(bigquery, null, List.of(), Map.of())).contains(new ProfileLocator.Target("bigquery", "my-gcp-project", null, null));
    }

    private static Path project(String profile) throws Exception {
        Path directory = Files.createTempDirectory("dbt");
        Files.writeString(directory.resolve("dbt_project.yml"), "name: carts\nprofile: " + profile + "\n");
        return directory;
    }
}
