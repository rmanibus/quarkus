package io.quarkus.test.junit;

import io.quarkus.test.common.RestAssuredURLManager;
import io.quarkus.test.common.TestScopeManager;
import io.quarkus.test.junit.callback.QuarkusTestMethodContext;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.opentest4j.TestAbortedException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.quarkus.test.junit.IntegrationTestManager.injectTestContext;
import static io.quarkus.test.junit.IntegrationTestManager.isFailedBoot;
import static io.quarkus.test.junit.IntegrationTestManager.throwBootFailureException;
import static io.quarkus.test.junit.IntegrationTestUtil.doProcessTestInstance;

public class QuarkusIntegrationTestExtension extends AbstractQuarkusTestWithContextExtension
        implements BeforeTestExecutionCallback, AfterTestExecutionCallback, BeforeEachCallback, AfterEachCallback,
        BeforeAllCallback, AfterAllCallback, TestInstancePostProcessor {


    @Override
    public void afterTestExecution(ExtensionContext context) throws Exception {
        if (!isFailedBoot() && !isAfterTestCallbacksEmpty()) {
            invokeAfterTestExecutionCallbacks(createQuarkusTestMethodContext(context));
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        if (!isFailedBoot()) {
            if (!isAfterEachCallbacksEmpty()) {
                invokeAfterEachCallbacks(createQuarkusTestMethodContext(context));
            }

            RestAssuredURLManager.clearURL();
            TestScopeManager.tearDown(true);
        }
    }

    @Override
    public void beforeTestExecution(ExtensionContext context) throws Exception {
        if (!isFailedBoot()) {
            if (!isBeforeTestCallbacksEmpty()) {
                invokeBeforeTestExecutionCallbacks(createQuarkusTestMethodContext(context));
            }

        } else {
            throwBootFailureException();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (isFailedBoot()) {
            throwBootFailureException();
        } else {
            if (!isBeforeEachCallbacksEmpty()) {
                invokeBeforeEachCallbacks(createQuarkusTestMethodContext(context));
            }

            IntegrationTestManager.setURL(context.getRequiredTestMethod(), context.getRequiredTestClass());
            TestScopeManager.setup(true);
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        ensureStarted(context);
        invokeBeforeClassCallbacks(context.getRequiredTestClass());
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (!isFailedBoot() && !isAfterAllCallbacksEmpty()) {
            invokeAfterAllCallbacks(createQuarkusTestMethodContext(context));
        }
    }


    @Override
    public void postProcessTestInstance(Object testInstance, ExtensionContext context) {
        ensureStarted(context);
        if (!isFailedBoot()) {
            doProcessTestInstance(testInstance, context);
            injectTestContext(testInstance);
        }
    }

    private QuarkusTestExtensionState ensureStarted(ExtensionContext extensionContext) {

        return IntegrationTestManager.ensureStarted(
                extensionContext.getRequiredTestClass(),
                getState(extensionContext),
                state -> setState(extensionContext, state),
                () -> {
                    try {
                        populateCallbacks(extensionContext.getRequiredTestClass().getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                },
                closeAction -> extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(
                QuarkusIntegrationTestExtension.class.getName() + ".systemProps", closeAction));
    }

    private QuarkusTestMethodContext createQuarkusTestMethodContext(ExtensionContext context) {
        Object testInstance = context.getTestInstance().orElse(null);
        List<Object> outerInstances = context.getTestInstances()
                .map(testInstances -> testInstances.getAllInstances().stream()
                        .filter(instance -> instance != testInstance)
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        return new QuarkusTestMethodContext(
                testInstance,
                outerInstances,
                context.getTestMethod().orElse(null),
                getState(context).getTestErrorCause());
    }
}
