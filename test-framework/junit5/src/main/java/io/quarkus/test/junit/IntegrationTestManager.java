package io.quarkus.test.junit;

import io.quarkus.bootstrap.app.CuratedApplication;
import io.quarkus.bootstrap.logging.InitialConfigurator;
import io.quarkus.runtime.logging.JBossVersion;
import io.quarkus.runtime.test.TestHttpEndpointProvider;
import io.quarkus.test.common.ArtifactLauncher;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.LauncherUtil;
import io.quarkus.test.common.PropertyTestUtil;
import io.quarkus.test.common.RestAssuredURLManager;
import io.quarkus.test.common.RunCommandLauncher;
import io.quarkus.test.common.TestConfigUtil;
import io.quarkus.test.common.TestHostLauncher;
import io.quarkus.test.common.TestResourceManager;
import io.quarkus.test.junit.callback.QuarkusTestMethodContext;
import io.quarkus.test.junit.launcher.ArtifactLauncherProvider;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.TestAbortedException;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.quarkus.test.junit.ArtifactTypeUtil.isContainer;
import static io.quarkus.test.junit.ArtifactTypeUtil.isJar;
import static io.quarkus.test.junit.IntegrationTestUtil.activateLogging;
import static io.quarkus.test.junit.IntegrationTestUtil.determineBuildOutputDirectory;
import static io.quarkus.test.junit.IntegrationTestUtil.determineTestProfileAndProperties;
import static io.quarkus.test.junit.IntegrationTestUtil.ensureNoInjectAnnotationIsUsed;
import static io.quarkus.test.junit.IntegrationTestUtil.findProfile;
import static io.quarkus.test.junit.IntegrationTestUtil.getArtifactType;
import static io.quarkus.test.junit.IntegrationTestUtil.getSysPropsToRestore;
import static io.quarkus.test.junit.IntegrationTestUtil.handleDevServices;
import static io.quarkus.test.junit.IntegrationTestUtil.readQuarkusArtifactProperties;
import static io.quarkus.test.junit.IntegrationTestUtil.startLauncher;
import static io.quarkus.test.junit.TestResourceUtil.TestResourceManagerReflections.copyEntriesFromProfile;

public class IntegrationTestManager {

    private static final String ENABLED_CALLBACKS_PROPERTY = "quarkus.test.enable-callbacks-for-integration-tests";

    private static boolean failedBoot;

    private static List<Function<Class<?>, String>> testHttpEndpointProviders;
    private static boolean ssl;

    private static Class<? extends QuarkusTestProfile> quarkusTestProfile;
    private static Throwable firstException; //if this is set then it will be thrown from the very first test that is run, the rest are aborted

    private static Class<?> currentJUnitTestClass;

    private static Map<String, String> devServicesProps;
    private static String containerNetworkId;

    static boolean isFailedBoot() {
        return failedBoot;
    }

    static void setURL(Method method, Class<?> testClass){
        RestAssuredURLManager.setURL(ssl, QuarkusTestExtension.getEndpointPath(method, testClass, testHttpEndpointProviders));
    }
    static QuarkusTestExtensionState ensureStarted(Class<?> requiredTestClass, QuarkusTestExtensionState state, Consumer<QuarkusTestExtensionState> setState, Runnable populateCallbacks, Consumer<Runnable> closeAction) {
        ensureNoInjectAnnotationIsUsed(requiredTestClass);
        Properties quarkusArtifactProperties = readQuarkusArtifactProperties(requiredTestClass);

        Class<? extends QuarkusTestProfile> selectedProfile = findProfile(requiredTestClass);
        boolean wrongProfile = !Objects.equals(selectedProfile, quarkusTestProfile);
        // we reset the failed state if we changed test class
        boolean isNewTestClass = !Objects.equals(requiredTestClass, currentJUnitTestClass);
        if (isNewTestClass && state != null) {
            state.setTestFailed(null);
            currentJUnitTestClass = requiredTestClass;
        }
        // we reload the test resources if we changed test class and if we had or will have per-test test resources
        boolean reloadTestResources = false;
        if ((state == null && !failedBoot) || wrongProfile || (reloadTestResources = isNewTestClass
                && TestResourceUtil.testResourcesRequireReload(state, requiredTestClass))) {
            if (wrongProfile || reloadTestResources) {
                if (state != null) {
                    try {
                        state.close();
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
            }
            try {
                state = doProcessStart(quarkusArtifactProperties, selectedProfile, requiredTestClass, populateCallbacks, closeAction);
                setState.accept(state);
            } catch (Throwable e) {
                try {
                    Path appLogPath = PropertyTestUtil.getLogFilePath();
                    File appLogFile = appLogPath.toFile();
                    if (appLogFile.exists() && (appLogFile.length() > 0)) {
                        System.err.println("Failed to launch the application. The application logs can be found at: "
                                + appLogPath.toAbsolutePath());
                    }
                } catch (IllegalStateException ignored) {

                }

                failedBoot = true;
                firstException = e;
            }
        }
        return state;
    }

    private static QuarkusTestExtensionState doProcessStart(Properties quarkusArtifactProperties,
                                                     Class<? extends QuarkusTestProfile> profile, Class<?> requiredTestClass, Runnable populateCallbacks, Consumer<Runnable> closeAction)
            throws Throwable {
        JBossVersion.disableVersionLogging();

        String artifactType = getArtifactType(quarkusArtifactProperties);

        Config config = LauncherUtil.installAndGetSomeConfig();
        String testProfile = TestConfigUtil.integrationTestProfile(config);
        boolean isDockerLaunch = isContainer(artifactType)
                || (isJar(artifactType) && "test-with-native-agent".equals(testProfile));

        ArtifactLauncher.InitContext.DevServicesLaunchResult devServicesLaunchResult = handleDevServices(requiredTestClass,
                isDockerLaunch);
        devServicesProps = devServicesLaunchResult.properties();
        containerNetworkId = devServicesLaunchResult.networkId();
        quarkusTestProfile = profile;
        currentJUnitTestClass = requiredTestClass;
        TestResourceManager testResourceManager = null;
        try {

            Map<String, String> sysPropRestore = getSysPropsToRestore();
            for (String devServicesProp : devServicesProps.keySet()) {
                sysPropRestore.put(devServicesProp, null); // used to signal that the property needs to be cleared
            }
            TestProfileAndProperties testProfileAndProperties = determineTestProfileAndProperties(profile, sysPropRestore);

            testResourceManager = new TestResourceManager(requiredTestClass, quarkusTestProfile,
                    copyEntriesFromProfile(testProfileAndProperties.testProfile,
                            requiredTestClass.getClassLoader()),
                    testProfileAndProperties.testProfile != null
                            && testProfileAndProperties.testProfile.disableGlobalTestResources(),
                    devServicesProps, containerNetworkId == null ? Optional.empty() : Optional.of(containerNetworkId));
            testResourceManager.init(
                    testProfileAndProperties.testProfile != null ? testProfileAndProperties.testProfile.getClass().getName()
                            : null);

            if (isCallbacksEnabledForIntegrationTests()) {
                populateCallbacks.run();
            }

            Map<String, String> additionalProperties = new HashMap<>();

            // propagate Quarkus properties set from the build tool
            Properties existingSysProps = System.getProperties();
            for (String name : existingSysProps.stringPropertyNames()) {
                if (name.startsWith("quarkus.")) {
                    additionalProperties.put(name, existingSysProps.getProperty(name));
                }
            }

            additionalProperties.putAll(testProfileAndProperties.properties);
            //we also make the dev services config accessible from the test itself
            Map<String, String> resourceManagerProps = new HashMap<>(devServicesProps);
            // Allow override of dev services props by integration test extensions
            resourceManagerProps.putAll(testResourceManager.start());
            Map<String, String> old = new HashMap<>();
            for (Map.Entry<String, String> i : resourceManagerProps.entrySet()) {
                old.put(i.getKey(), System.getProperty(i.getKey()));
                if (i.getValue() == null) {
                    System.clearProperty(i.getKey());
                } else {
                    System.setProperty(i.getKey(), i.getValue());
                }
            }

            closeAction.accept(() -> {
                for (Map.Entry<String, String> i : old.entrySet()) {
                    old.put(i.getKey(), System.getProperty(i.getKey()));
                    if (i.getValue() == null) {
                        System.clearProperty(i.getKey());
                    } else {
                        System.setProperty(i.getKey(), i.getValue());
                    }
                }
            });

            additionalProperties.putAll(resourceManagerProps);

            ArtifactLauncher<?> launcher = null;
            String testHost = System.getProperty("quarkus.http.test-host");
            if ((testHost != null) && !testHost.isEmpty()) {
                launcher = new TestHostLauncher();
            } else {
                Duration waitDuration = TestConfigUtil.waitTimeValue(config);
                String target = TestConfigUtil.runTarget(config);
                // try to execute a run command published by an extension if it exists.  We do this so that extensions that have a custom run don't have to create any special artifact type
                launcher = RunCommandLauncher.tryLauncher(devServicesLaunchResult.getCuratedApplication().getQuarkusBootstrap(),
                        target, waitDuration);
                if (launcher == null) {
                    ServiceLoader<ArtifactLauncherProvider> loader = ServiceLoader.load(ArtifactLauncherProvider.class);
                    for (ArtifactLauncherProvider launcherProvider : loader) {
                        if (launcherProvider.supportsArtifactType(artifactType, testProfile)) {
                            launcher = launcherProvider.create(
                                    new DefaultArtifactLauncherCreateContext(quarkusArtifactProperties,
                                            requiredTestClass,
                                            devServicesLaunchResult));
                            break;
                        }
                    }
                }
            }
            if (launcher == null) {
                throw new IllegalStateException(
                        "Artifact type + '" + artifactType + "' is not supported by @QuarkusIntegrationTest");
            }

            activateLogging();
            startLauncher(launcher, additionalProperties, () -> ssl = true);

            Closeable resource = new IntegrationTestExtensionStateResource(launcher,
                    devServicesLaunchResult.getCuratedApplication());
            IntegrationTestExtensionState state = new IntegrationTestExtensionState(testResourceManager, resource,
                    AbstractTestWithCallbacksExtension::clearCallbacks, sysPropRestore);
            testHttpEndpointProviders = TestHttpEndpointProvider.load();

            return state;
        } catch (Throwable e) {
            if (!InitialConfigurator.DELAYED_HANDLER.isActivated()) {
                activateLogging();
            }

            try {
                if (testResourceManager != null) {
                    testResourceManager.close();
                }
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }

    static void injectTestContext(Object testInstance) {
        Class<?> c = testInstance.getClass();
        while (c != Object.class) {
            for (Field f : c.getDeclaredFields()) {
                if (f.getType().equals(DevServicesContext.class)) {
                    try {
                        f.setAccessible(true);
                        f.set(testInstance, createTestContext());
                        return;
                    } catch (Exception e) {
                        throw new RuntimeException("Unable to set field '" + f.getName()
                                + "' with the proper test context", e);
                    }
                } else if (DevServicesContext.ContextAware.class.isAssignableFrom(f.getType())) {
                    f.setAccessible(true);
                    try {
                        DevServicesContext.ContextAware val = (DevServicesContext.ContextAware) f.get(testInstance);
                        val.setIntegrationTestContext(createTestContext());
                    } catch (Exception e) {
                        throw new RuntimeException("Unable to inject context into field " + f.getName(), e);
                    }
                }
            }
            c = c.getSuperclass();
        }
    }

    static void throwBootFailureException() {
        if (firstException != null) {
            Throwable throwable = firstException;
            firstException = null;
            throw new RuntimeException(throwable);
        } else {
            throw new TestAbortedException("Boot failed");
        }
    }

    private static DevServicesContext createTestContext() {
        Map<String, String> devServicesPropsCopy = devServicesProps.isEmpty() ? Collections.emptyMap()
                : Collections.unmodifiableMap(devServicesProps);
        return new DefaultQuarkusIntegrationTestContext(devServicesPropsCopy,
                containerNetworkId == null ? Optional.empty() : Optional.of(containerNetworkId));
    }

    private static boolean isCallbacksEnabledForIntegrationTests() {
        return Optional.ofNullable(System.getProperty(ENABLED_CALLBACKS_PROPERTY)).map(Boolean::parseBoolean)
                .or(() -> LauncherUtil.installAndGetSomeConfig().getOptionalValue(ENABLED_CALLBACKS_PROPERTY, Boolean.class))
                .orElse(false);
    }

    private static final class IntegrationTestExtensionStateResource implements Closeable {

        private final ArtifactLauncher<?> launcher;
        private final CuratedApplication curatedApplication;

        public IntegrationTestExtensionStateResource(ArtifactLauncher<?> launcher,
                                                     CuratedApplication curatedApplication) {
            this.launcher = launcher;
            this.curatedApplication = curatedApplication;
        }

        @Override
        public void close() {
            if (launcher != null) {
                try {
                    launcher.close();
                } catch (Exception e) {
                    System.err.println("Unable to close ArtifactLauncher: " + e.getMessage());
                }
            }
            try {
                curatedApplication.close();
            } catch (Exception e) {
                System.err.println("Unable to close CuratedApplication: " + e.getMessage());
            }
        }
    }

    private static class DefaultArtifactLauncherCreateContext implements ArtifactLauncherProvider.CreateContext {
        private final Properties quarkusArtifactProperties;
        private final Class<?> requiredTestClass;
        private final ArtifactLauncher.InitContext.DevServicesLaunchResult devServicesLaunchResult;

        DefaultArtifactLauncherCreateContext(Properties quarkusArtifactProperties,
                                             Class<?> requiredTestClass, ArtifactLauncher.InitContext.DevServicesLaunchResult devServicesLaunchResult) {
            this.quarkusArtifactProperties = quarkusArtifactProperties;
            this.requiredTestClass = requiredTestClass;
            this.devServicesLaunchResult = devServicesLaunchResult;
        }

        @Override
        public Properties quarkusArtifactProperties() {
            return quarkusArtifactProperties;
        }

        @Override
        public Path buildOutputDirectory() {
            return determineBuildOutputDirectory(requiredTestClass);
        }

        @Override
        public Class<?> testClass() {
            return requiredTestClass;
        }

        @Override
        public ArtifactLauncher.InitContext.DevServicesLaunchResult devServicesLaunchResult() {
            return devServicesLaunchResult;
        }
    }

    private static class DefaultQuarkusIntegrationTestContext implements DevServicesContext {

        private final Map<String, String> devServicesProperties;
        private final Optional<String> containerNetworkId;

        private DefaultQuarkusIntegrationTestContext(Map<String, String> devServicesProperties,
                                                     Optional<String> containerNetworkId) {
            this.devServicesProperties = devServicesProperties;
            this.containerNetworkId = containerNetworkId;
        }

        @Override
        public Map<String, String> devServicesProperties() {
            return devServicesProperties;
        }

        @Override
        public Optional<String> containerNetworkId() {
            return containerNetworkId;
        }
    }
}
