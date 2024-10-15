package io.quarkus.flyway.multitenant.runtime;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import io.quarkus.flyway.runtime.FlywayContainer;
import io.quarkus.flyway.runtime.FlywayCreator;
import io.quarkus.flyway.runtime.FlywayDataSourceBuildTimeConfig;
import io.quarkus.flyway.runtime.FlywayDataSourceRuntimeConfig;
import io.quarkus.hibernate.orm.runtime.PersistenceUnitUtil;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.callback.Callback;

import io.quarkus.arc.All;
import io.quarkus.arc.InstanceHandle;

import io.quarkus.flyway.FlywayConfigurationCustomizer;
import io.quarkus.flyway.multitenant.FlywayPersistenceUnit;

/**
 * This class is sort of a producer for {@link Flyway}.
 *
 * It isn't a CDI producer in the literal sense, but it is marked as a bean
 * and it's {@code createFlyway} method is called at runtime in order to produce
 * the actual {@code Flyway} objects.
 *
 * CDI scopes and qualifiers are set up at build-time, which is why this class is devoid of
 * any CDI annotations
 *
 */
public class FlywayMultiTenantContainerProducer {

    private final FlywayMultiTenantRuntimeConfig flywayMultiTenantRuntimeConfig;
    private final FlywayMultiTenantBuildTimeConfig flywayBuildConfig;

    private final List<InstanceHandle<FlywayConfigurationCustomizer>> configCustomizerInstances;

    public FlywayMultiTenantContainerProducer(FlywayMultiTenantRuntimeConfig flywayMultiTenantRuntimeConfig,
            FlywayMultiTenantBuildTimeConfig flywayBuildConfig,
            @All List<InstanceHandle<FlywayConfigurationCustomizer>> configCustomizerInstances) {
        this.flywayMultiTenantRuntimeConfig = flywayMultiTenantRuntimeConfig;
        this.flywayBuildConfig = flywayBuildConfig;
        this.configCustomizerInstances = configCustomizerInstances;
    }

    public FlywayContainer createFlyway(DataSource dataSource, String persistenceUnitName, String tenantId,
            boolean hasMigrations,
            boolean createPossible) {
        FlywayDataSourceRuntimeConfig matchingRuntimeConfig = flywayMultiTenantRuntimeConfig
                .getConfigForPersistenceUnitName(persistenceUnitName);
        FlywayDataSourceBuildTimeConfig matchingBuildTimeConfig = flywayBuildConfig
                .getConfigForPersistenceUnitName(persistenceUnitName);
        final Collection<Callback> callbacks = QuarkusPathLocationScanner.callbacksForPersistenceUnit(persistenceUnitName);
        final Flyway flyway = new FlywayCreator(matchingRuntimeConfig, matchingBuildTimeConfig, matchingConfigCustomizers(
                configCustomizerInstances, persistenceUnitName)).withCallbacks(callbacks)
                .withTenantId(tenantId)
                .createFlyway(dataSource);
        return new FlywayContainer(flyway, matchingRuntimeConfig.baselineAtStart, matchingRuntimeConfig.cleanAtStart,
                matchingRuntimeConfig.migrateAtStart,
                matchingRuntimeConfig.repairAtStart, matchingRuntimeConfig.validateAtStart,
                persistenceUnitName, hasMigrations,
                createPossible);
    }

    private List<FlywayConfigurationCustomizer> matchingConfigCustomizers(
            List<InstanceHandle<FlywayConfigurationCustomizer>> configCustomizerInstances, String persistenceUnitName) {
        if ((configCustomizerInstances == null) || configCustomizerInstances.isEmpty()) {
            return Collections.emptyList();
        }
        List<FlywayConfigurationCustomizer> result = new ArrayList<>();
        for (InstanceHandle<FlywayConfigurationCustomizer> instance : configCustomizerInstances) {
            Set<Annotation> qualifiers = instance.getBean().getQualifiers();
            boolean qualifierMatchesPS = false;
            boolean hasFlywayPersistenceUnitQualifier = false;
            for (Annotation qualifier : qualifiers) {
                if (qualifier.annotationType().equals(FlywayPersistenceUnit.class)) {
                    hasFlywayPersistenceUnitQualifier = true;
                    if (persistenceUnitName.equals(((FlywayPersistenceUnit) qualifier).value())) {
                        qualifierMatchesPS = true;
                        break;
                    }
                }
            }
            if (qualifierMatchesPS) {
                result.add(instance.get());
            } else if (PersistenceUnitUtil.isDefaultPersistenceUnit(persistenceUnitName)
                    && !hasFlywayPersistenceUnitQualifier) {
                // this is the case where a FlywayConfigurationCustomizer does not have a qualifier at all, therefore is applies to the default datasource
                result.add(instance.get());
            }
        }
        return result;
    }
}
