package io.kestra.plugin.dbt;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.assets.Asset;
import io.kestra.core.models.assets.AssetIdentifier;
import io.kestra.core.models.assets.AssetRelationRef;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.DefaultRunContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.dbt.assets.ProfileLocator;
import io.kestra.plugin.dbt.assets.TableScope;
import io.kestra.plugin.dbt.cli.DbtCLI;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@KestraTest
class ResultParserTableScopeTest {
    private static final TableScope PROFILE = new TableScope(null, new ProfileLocator.Target("postgres", "PG", "5432", "analytics"));
    private static final String DATABASE = "postgres:pg:analytics";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private TestAssetManagerFactory assetManagerFactory;

    private RunContext runContext;

    @BeforeEach
    void setUp() {
        assetManagerFactory.clear();
        var task = DbtCLI.builder().id(IdUtils.create()).type(DbtCLI.class.getName()).commands(Property.ofValue(List.of("dbt build"))).build();
        var flow = TestsUtils.mockFlow();
        var execution = TestsUtils.mockExecution(flow, Map.of(), null);
        runContext = runContextFactory.of(flow, task, execution, TestsUtils.mockTaskRun(execution, task), false);
    }

    @Test
    void shouldRecordEachBuiltTableUnderTheDatabaseOfTheProfileAndLinkItToItsProject() throws Exception {
        ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, PROFILE);

        String project = "dbt:" + runContext.flowInfo().namespace() + ":carts";
        List<Asset> outputs = outputs(runContext.assets().emitted());
        assertThat(outputs).extracting(Asset::getId, Asset::getType).containsExactlyInAnyOrder(
            tuple(project, "io.kestra.plugin.ee.assets.DbtProject"),
            tuple(DATABASE + ".marts.orders", "io.kestra.plugin.ee.assets.Table"),
            tuple(DATABASE + ".marts.payments", "io.kestra.plugin.ee.assets.Table")
        );
        assertThat(outputs).allSatisfy(asset -> assertThat(asset.getNamespace()).isEqualTo(runContext.flowInfo().namespace()));
        assertThat(outputs).filteredOn(asset -> asset.getId().endsWith(".marts.orders")).singleElement().satisfies(orders ->
        {
            assertThat(orders.getPartOf()).isEqualTo(new AssetRelationRef(DATABASE, null, null));
            assertThat(orders.getRelated()).containsExactly(new AssetRelationRef(project, null, "managedBy"));
            assertThat(orders.getMetadata()).containsEntry("dbt.uniqueId", "model.carts.orders").containsEntry("dbt.materialized", "table");
        });
    }

    @Test
    void shouldPassTheParentsOfAnEphemeralModelToTheModelThatUsesItAndRecordNoTableForIt() throws Exception {
        ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, PROFILE);

        AssetEmit orders = emitOf(runContext.assets().emitted(), DATABASE + ".marts.orders");
        assertThat(orders.inputs()).extracting(AssetIdentifier::id).containsExactly(DATABASE + ".raw.carts");
        assertThat(outputs(runContext.assets().emitted())).extracting(Asset::getId).noneMatch(id -> id.contains("stg_orders"));
    }

    @Test
    void shouldWriteTheSameTableFromTheModelTaskRunAsFromTheRun() throws Exception {
        ResultParser.ManifestResult manifest = ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, PROFILE);
        Asset fromRun = emitOf(runContext.assets().emitted(), DATABASE + ".marts.orders").outputs().getFirst();

        ResultParser.parseRunResult(runContext, fixture("run_results-carts-build.json"), manifest.manifest(), true, manifest.runResult(), PROFILE);

        ((DefaultRunContext) runContext).emitAssetsOfDynamicTaskRuns();
        Asset fromModel = assetManagerFactory.allEmittedFor().stream()
            .flatMap(emit -> emit.outputs().stream())
            .filter(asset -> asset.getId().equals(fromRun.getId()))
            .findFirst()
            .orElseThrow();
        assertThat(fromModel.getPartOf()).isEqualTo(fromRun.getPartOf());
        assertThat(fromModel.getRelated()).isEqualTo(fromRun.getRelated());
        assertThat(fromModel.getMetadata()).containsEntry("dbt.uniqueId", "model.carts.orders").containsEntry("dbt.materialized", "table");
    }

    @Test
    void shouldUseTheDatabaseTheTaskNamesOverTheProfile() throws Exception {
        TableScope named = new TableScope("postgres:analytics-prod.c9akciq32xyz.eu-west-1.rds.amazonaws.com:analytics", PROFILE.target());

        ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, named);

        assertThat(emitOf(runContext.assets().emitted(), "postgres:analytics-prod.c9akciq32xyz.eu-west-1.rds.amazonaws.com:analytics.marts.orders").outputs().getFirst().getPartOf().id())
            .isEqualTo("postgres:analytics-prod.c9akciq32xyz.eu-west-1.rds.amazonaws.com:analytics");
    }

    @Test
    void shouldKeepTheTablesOfTwoServersHoldingADatabaseOfTheSameNameApart() throws Exception {
        TableScope other = new TableScope(null, new ProfileLocator.Target("postgres", "replica.internal", "5432", "analytics"));

        ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, PROFILE);
        ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, other);

        assertThat(outputs(runContext.assets().emitted())).extracting(Asset::getId)
            .contains(DATABASE + ".marts.orders", "postgres:replica.internal:analytics.marts.orders");
    }

    @Test
    void shouldLinkATableToEveryProjectThatBuildsIt() throws Exception {
        Path renamed = Files.createTempFile("manifest", ".json");
        Files.writeString(renamed, Files.readString(fixture("manifest-carts.json").toPath()).replace("\"project_name\": \"carts\"", "\"project_name\": \"carts_migrated\""));

        ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, PROFILE);
        ResultParser.parseManifestWithAssets(runContext, renamed.toFile(), fixture("run_results-carts-build.json"), true, PROFILE);

        assertThat(runContext.assets().emitted()).flatExtracting(AssetEmit::outputs)
            .filteredOn(asset -> asset.getId().equals(DATABASE + ".marts.orders"))
            .flatExtracting(Asset::getRelated)
            .extracting(AssetRelationRef::id)
            .containsExactlyInAnyOrder("dbt:" + runContext.flowInfo().namespace() + ":carts", "dbt:" + runContext.flowInfo().namespace() + ":carts_migrated");
    }

    @Test
    void shouldKeepTheIdsUnscopedWhenTheDatabaseIsUnknown() throws Exception {
        ResultParser.parseManifestWithAssets(runContext, fixture("manifest-carts.json"), fixture("run_results-carts-build.json"), true, TableScope.NONE);

        Asset orders = emitOf(runContext.assets().emitted(), "analytics.marts.orders").outputs().getFirst();
        assertThat(orders.getPartOf()).isNull();
    }

    private static List<Asset> outputs(List<AssetEmit> emitted) {
        return emitted.stream().flatMap(emit -> emit.outputs().stream()).toList();
    }

    private static AssetEmit emitOf(List<AssetEmit> emitted, String outputId) {
        return emitted.stream().filter(emit -> emit.outputs().stream().anyMatch(asset -> asset.getId().equals(outputId))).findFirst().orElseThrow();
    }

    private static File fixture(String name) throws IOException {
        Path copy = Files.createTempFile(name, ".json");
        try (InputStream file = Objects.requireNonNull(ResultParserTableScopeTest.class.getResourceAsStream("/assets/" + name))) {
            Files.write(copy, file.readAllBytes());
        }
        return copy.toFile();
    }
}
