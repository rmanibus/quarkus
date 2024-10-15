package io.quarkus.flyway.multitenant.runtime;

import io.quarkus.flyway.multitenant.FlywayPersistenceUnit;

import java.lang.annotation.Annotation;

import static io.quarkus.flyway.runtime.FlywayCreator.TENANT_ID_DEFAULT;

public final class FlywayMultiTenantContainerUtil {
    private FlywayMultiTenantContainerUtil() {
    }

    public static Annotation getFlywayContainerQualifier(String persistenceUnitName) {
        return getFlywayContainerQualifier(persistenceUnitName, TENANT_ID_DEFAULT);
    }

    public static Annotation getFlywayContainerQualifier(String persistenceUnitName, String tenantId) {
        return FlywayPersistenceUnit.FlywayPersistenceUnitLiteral.of(persistenceUnitName, tenantId);
    }
}
