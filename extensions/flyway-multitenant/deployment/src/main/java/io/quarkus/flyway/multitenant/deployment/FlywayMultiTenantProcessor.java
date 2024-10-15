package io.quarkus.flyway.multitenant.deployment;

import static io.quarkus.datasource.common.runtime.DataSourceUtil.DEFAULT_DATASOURCE_NAME;
import static io.quarkus.deployment.annotations.ExecutionTime.STATIC_INIT;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.quarkus.flyway.multitenant.FlywayPersistenceUnit;
import io.quarkus.flyway.runtime.FlywayContainer;
import io.quarkus.flyway.runtime.FlywayDataSourceBuildTimeConfig;
import io.quarkus.hibernate.orm.deployment.PersistenceUnitDescriptorBuildItem;
import io.quarkus.hibernate.orm.runtime.migration.MultiTenancyStrategy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Singleton;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.Location;
import org.flywaydb.core.api.callback.Callback;
import org.flywaydb.core.api.migration.JavaMigration;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.ClassType;
import org.jboss.jandex.DotName;
import org.jboss.logging.Logger;

import io.quarkus.agroal.runtime.DataSources;
import io.quarkus.agroal.spi.JdbcDataSourceBuildItem;
import io.quarkus.agroal.spi.JdbcDataSourceSchemaReadyBuildItem;
import io.quarkus.agroal.spi.JdbcInitialSQLGeneratorBuildItem;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.BeanContainerBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.SyntheticBeansRuntimeInitBuildItem;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.builder.item.SimpleBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.BuildSteps;
import io.quarkus.deployment.annotations.Consume;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Produce;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.ApplicationInfoBuildItem;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.HotDeploymentWatchedFileBuildItem;
import io.quarkus.deployment.builditem.InitTaskBuildItem;
import io.quarkus.deployment.builditem.InitTaskCompletedBuildItem;
import io.quarkus.deployment.builditem.ServiceStartBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.logging.LoggingSetupBuildItem;
import io.quarkus.deployment.recording.RecorderContext;
import io.quarkus.flyway.FlywayDataSource;
import io.quarkus.flyway.multitenant.runtime.FlywayMultiTenantBuildTimeConfig;
import io.quarkus.flyway.multitenant.runtime.FlywayMultiTenantContainerProducer;
import io.quarkus.flyway.multitenant.runtime.FlywayRecorder;
import io.quarkus.flyway.multitenant.runtime.FlywayMultiTenantRuntimeConfig;
import io.quarkus.runtime.util.ClassPathUtils;

@BuildSteps(onlyIf = FlywayEnabled.class)
class FlywayMultiTenantProcessor {

    private static final String CLASSPATH_APPLICATION_MIGRATIONS_PROTOCOL = "classpath";

    private static final String FLYWAY_CONTAINER_BEAN_NAME_PREFIX = "flyway_container_";
    private static final String FLYWAY_BEAN_NAME_PREFIX = "flyway_";

    private static final DotName JAVA_MIGRATION = DotName.createSimple(JavaMigration.class.getName());

    private static final Logger LOGGER = Logger.getLogger(FlywayMultiTenantProcessor.class);

    @Record(STATIC_INIT)
    @BuildStep
    MigrationStateBuildItem build(BuildProducer<NativeImageResourceBuildItem> resourceProducer,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClassProducer,
            BuildProducer<HotDeploymentWatchedFileBuildItem> hotDeploymentProducer,
            FlywayRecorder recorder,
            RecorderContext context,
            CombinedIndexBuildItem combinedIndexBuildItem,
            List<JdbcDataSourceBuildItem> jdbcDataSourceBuildItems,
            FlywayMultiTenantBuildTimeConfig flywayMultiTenantBuildTimeConfig) throws Exception {

        Collection<String> dataSourceNames = getDataSourceNames(jdbcDataSourceBuildItems);
        Map<String, Collection<String>> applicationMigrationsToDs = new HashMap<>();
        for (var dataSourceName : dataSourceNames) {
            FlywayDataSourceBuildTimeConfig flywayPersistenceUnitBuildTimeConfig = flywayMultiTenantBuildTimeConfig
                    .getConfigForPersistenceUnitName(dataSourceName);

            Collection<String> migrationLocations = discoverApplicationMigrations(
                    flywayPersistenceUnitBuildTimeConfig.locations);
            applicationMigrationsToDs.put(dataSourceName, migrationLocations);
        }
        Set<String> datasourcesWithMigrations = new HashSet<>();
        Set<String> datasourcesWithoutMigrations = new HashSet<>();
        for (var e : applicationMigrationsToDs.entrySet()) {
            if (e.getValue().isEmpty()) {
                datasourcesWithoutMigrations.add(e.getKey());
            } else {
                datasourcesWithMigrations.add(e.getKey());
            }
        }

        Collection<String> applicationMigrations = applicationMigrationsToDs.values().stream().collect(HashSet::new,
                AbstractCollection::addAll, HashSet::addAll);
        for (String applicationMigration : applicationMigrations) {
            Location applicationMigrationLocation = new Location(applicationMigration);
            String applicationMigrationPath = applicationMigrationLocation.getPath();

            if ((applicationMigrationPath != null) &&
            // we don't include .class files in the watched files because that messes up live reload
                    !applicationMigrationPath.endsWith(".class")) {
                hotDeploymentProducer.produce(new HotDeploymentWatchedFileBuildItem(applicationMigrationPath));
            }
        }
        recorder.setApplicationMigrationFiles(applicationMigrations);

        Set<Class<? extends JavaMigration>> javaMigrationClasses = new HashSet<>();
        addJavaMigrations(combinedIndexBuildItem.getIndex().getAllKnownImplementors(JAVA_MIGRATION), context,
                reflectiveClassProducer, javaMigrationClasses);
        recorder.setApplicationMigrationClasses(javaMigrationClasses);

        final Map<String, Collection<Callback>> callbacks = FlywayMultiTenantCallbacksLocator.with(
                dataSourceNames,
                flywayMultiTenantBuildTimeConfig,
                combinedIndexBuildItem,
                reflectiveClassProducer).getCallbacks();
        recorder.setApplicationCallbackClasses(callbacks);

        resourceProducer.produce(new NativeImageResourceBuildItem(applicationMigrations.toArray(new String[0])));
        return new MigrationStateBuildItem(datasourcesWithMigrations, datasourcesWithoutMigrations);
    }

    @SuppressWarnings("unchecked")
    private void addJavaMigrations(Collection<ClassInfo> candidates, RecorderContext context,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClassProducer,
            Set<Class<? extends JavaMigration>> javaMigrationClasses) {
        for (ClassInfo javaMigration : candidates) {
            if (Modifier.isAbstract(javaMigration.flags())) {
                continue;
            }
            javaMigrationClasses.add((Class<JavaMigration>) context.classProxy(javaMigration.name().toString()));
            reflectiveClassProducer.produce(
                    ReflectiveClassBuildItem.builder(javaMigration.name().toString()).build());
        }
    }

    @BuildStep
    @Produce(SyntheticBeansRuntimeInitBuildItem.class)
    @Consume(LoggingSetupBuildItem.class)
    @Record(ExecutionTime.RUNTIME_INIT)
    void createBeans(FlywayRecorder recorder,
            List<PersistenceUnitDescriptorBuildItem> persistenceUnits,
            List<JdbcInitialSQLGeneratorBuildItem> sqlGeneratorBuildItems,
            BuildProducer<AdditionalBeanBuildItem> additionalBeans,
            BuildProducer<SyntheticBeanBuildItem> syntheticBeanBuildItemBuildProducer,
            MigrationStateBuildItem migrationsBuildItem,
            FlywayMultiTenantBuildTimeConfig flywayMultiTenantBuildTimeConfig) {
        // make a FlywayContainerProducer bean
        additionalBeans.produce(
                AdditionalBeanBuildItem.builder().addBeanClasses(FlywayMultiTenantContainerProducer.class).setUnremovable()
                        .setDefaultScope(DotNames.SINGLETON).build());
        // add the @FlywayDataSource class otherwise it won't be registered as a qualifier
        additionalBeans.produce(AdditionalBeanBuildItem.builder().addBeanClass(FlywayDataSource.class).build());

        for (var persistenceUnit : persistenceUnits) {
            String persistenceUnitName = persistenceUnit.getPersistenceUnitName();
            String dataSourceName = persistenceUnit.getConfig().getDataSource().orElse(DEFAULT_DATASOURCE_NAME);
            boolean multiTenant = persistenceUnit.getConfig().getMultiTenancyStrategy() == MultiTenancyStrategy.SCHEMA;

            boolean hasMigrations = migrationsBuildItem.hasMigrations.contains(persistenceUnitName);
            boolean createPossible = false;
            if (!hasMigrations) {
                createPossible = sqlGeneratorBuildItems.stream().anyMatch(s -> s.getDatabaseName().equals(persistenceUnitName));
            }

            SyntheticBeanBuildItem.ExtendedBeanConfigurator flywayContainerConfigurator = SyntheticBeanBuildItem
                    .configure(FlywayContainer.class)
                    .scope(multiTenant ? Dependent.class : Singleton.class)
                    .setRuntimeInit()
                    .unremovable()
                    .addInjectionPoint(ClassType.create(DotName.createSimple(FlywayMultiTenantContainerProducer.class)))
                    .addInjectionPoint(ClassType.create(DotName.createSimple(DataSources.class)))
                    .createWith(recorder.flywayContainerFunction(dataSourceName, persistenceUnitName, multiTenant,
                            hasMigrations, createPossible));

            AnnotationInstance flywayContainerQualifier;

            String containerBeanName = FLYWAY_CONTAINER_BEAN_NAME_PREFIX + persistenceUnitName;
            flywayContainerConfigurator.name(containerBeanName);

            flywayContainerConfigurator.addQualifier().annotation(DotNames.NAMED).addValue("value", containerBeanName).done();
            flywayContainerConfigurator.addQualifier().annotation(FlywayPersistenceUnit.class)
                    .addValue("value", persistenceUnitName)
                    .done();
            flywayContainerConfigurator.priority(5);

            flywayContainerQualifier = AnnotationInstance.builder(FlywayPersistenceUnit.class).add("value", persistenceUnitName)
                    .build();

            syntheticBeanBuildItemBuildProducer.produce(flywayContainerConfigurator.done());

            SyntheticBeanBuildItem.ExtendedBeanConfigurator flywayConfigurator = SyntheticBeanBuildItem
                    .configure(Flyway.class)
                    .scope(multiTenant ? Dependent.class : Singleton.class)
                    .setRuntimeInit()
                    .unremovable()
                    .addInjectionPoint(ClassType.create(DotName.createSimple(FlywayContainer.class)), flywayContainerQualifier)
                    .createWith(recorder.flywayFunction(persistenceUnitName, multiTenant));

            if (multiTenant) {
                flywayConfigurator.addInjectionPoint(
                        ClassType.create(DotName.createSimple(InjectionPoint.class)));
            }

            String flywayBeanName = FLYWAY_BEAN_NAME_PREFIX + persistenceUnitName;
            flywayConfigurator.name(flywayBeanName);
            flywayConfigurator.priority(5);

            flywayConfigurator.addQualifier().annotation(DotNames.NAMED).addValue("value", flywayBeanName).done();
            flywayConfigurator.addQualifier().annotation(FlywayPersistenceUnit.class).addValue("value", persistenceUnitName)
                    .done();

            syntheticBeanBuildItemBuildProducer.produce(flywayConfigurator.done());
        }
    }

    @BuildStep
    @Consume(BeanContainerBuildItem.class)
    @Record(ExecutionTime.RUNTIME_INIT)
    public ServiceStartBuildItem startActions(FlywayRecorder recorder,
            FlywayMultiTenantRuntimeConfig config,
            BuildProducer<JdbcDataSourceSchemaReadyBuildItem> schemaReadyBuildItem,
            BuildProducer<InitTaskCompletedBuildItem> initializationCompleteBuildItem,
            List<JdbcDataSourceBuildItem> jdbcDataSourceBuildItems,
            MigrationStateBuildItem migrationsBuildItem) {

        Collection<String> dataSourceNames = getDataSourceNames(jdbcDataSourceBuildItems);

        for (String dataSourceName : dataSourceNames) {
            recorder.doStartActions(dataSourceName);
        }

        // once we are done running the migrations, we produce a build item indicating that the
        // schema is "ready"
        schemaReadyBuildItem.produce(new JdbcDataSourceSchemaReadyBuildItem(migrationsBuildItem.hasMigrations));
        initializationCompleteBuildItem.produce(new InitTaskCompletedBuildItem("flyway"));
        return new ServiceStartBuildItem("flyway");
    }

    @BuildStep
    public InitTaskBuildItem configureInitTask(ApplicationInfoBuildItem app) {
        return InitTaskBuildItem.create()
                .withName(app.getName() + "-flyway-init")
                .withTaskEnvVars(Map.of("QUARKUS_INIT_AND_EXIT", "true", "QUARKUS_FLYWAY_ENABLED", "true"))
                .withAppEnvVars(Map.of("QUARKUS_FLYWAY_ENABLED", "false"))
                .withSharedEnvironment(true)
                .withSharedFilesystem(true);
    }

    private Set<String> getDataSourceNames(List<JdbcDataSourceBuildItem> jdbcDataSourceBuildItems) {
        Set<String> result = new HashSet<>(jdbcDataSourceBuildItems.size());
        for (JdbcDataSourceBuildItem item : jdbcDataSourceBuildItems) {
            result.add(item.getName());
        }
        return result;
    }

    private Collection<String> discoverApplicationMigrations(Collection<String> locations)
            throws IOException {
        LinkedHashSet<String> applicationMigrationResources = new LinkedHashSet<>();
        // Locations can be a comma separated list
        for (String location : locations) {
            location = normalizeLocation(location);
            if (location.startsWith(Location.FILESYSTEM_PREFIX)) {
                applicationMigrationResources.add(location);
                continue;
            }

            String finalLocation = location;
            ClassPathUtils.consumeAsPaths(Thread.currentThread().getContextClassLoader(), location, path -> {
                Set<String> applicationMigrations = null;
                try {
                    applicationMigrations = FlywayMultiTenantProcessor.this.getApplicationMigrationsFromPath(finalLocation,
                            path);
                } catch (IOException e) {
                    LOGGER.warnv(e,
                            "Can't process files in path %s", path);
                }
                if (applicationMigrations != null) {
                    applicationMigrationResources.addAll(applicationMigrations);
                }
            });
        }
        return applicationMigrationResources;
    }

    private String normalizeLocation(String location) {
        if (location == null) {
            throw new IllegalStateException("Flyway migration location may not be null.");
        }

        // Strip any 'classpath:' protocol prefixes because they are assumed
        // but not recognized by ClassLoader.getResources()
        if (location.startsWith(CLASSPATH_APPLICATION_MIGRATIONS_PROTOCOL + ':')) {
            location = location.substring(CLASSPATH_APPLICATION_MIGRATIONS_PROTOCOL.length() + 1);
            if (location.startsWith("/")) {
                location = location.substring(1);
            }
        }
        if (!location.endsWith("/")) {
            location += "/";
        }

        return location;
    }

    private Set<String> getApplicationMigrationsFromPath(final String location, final Path rootPath)
            throws IOException {

        try (final Stream<Path> pathStream = Files.walk(rootPath)) {
            return pathStream.filter(Files::isRegularFile)
                    .map(it -> Paths.get(location, rootPath.relativize(it).toString()).normalize().toString())
                    // we don't want windows paths here since the paths are going to be used as classpath paths anyway
                    .map(it -> it.replace('\\', '/'))
                    .peek(it -> LOGGER.debugf("Discovered path: %s", it))
                    .collect(Collectors.toSet());
        }
    }

    public static final class MigrationStateBuildItem extends SimpleBuildItem {

        final Set<String> hasMigrations;
        final Set<String> missingMigrations;

        MigrationStateBuildItem(Set<String> hasMigrations, Set<String> missingMigrations) {
            this.hasMigrations = hasMigrations;
            this.missingMigrations = missingMigrations;
        }
    }
}
