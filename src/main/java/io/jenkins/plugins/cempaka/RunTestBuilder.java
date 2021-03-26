package io.jenkins.plugins.cempaka;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import hudson.Extension;
import hudson.Launcher;
import hudson.model.AbstractBuild;
import hudson.model.AbstractProject;
import hudson.model.BuildListener;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.Builder;
import hudson.util.FormValidation;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.cempaka.cyclone.client.ApacheCycloneClient;
import org.cempaka.cyclone.client.CycloneClient;
import org.cempaka.cyclone.client.ImmutableTestExecutionProperties;
import org.cempaka.cyclone.client.MetricDataPoint;
import org.cempaka.cyclone.client.TestExecution;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

public class RunTestBuilder extends Builder
{
    public final String cycloneUrl;
    public final String parcelId;
    public final String testName;
    public final String loopCount;
    public final String threadsNumber;
    public final String nodes;
    public final String parameters;
    public final String jvmOptions;

    @DataBoundConstructor
    public RunTestBuilder(final String cycloneUrl,
                          final String parcelId,
                          final String testName,
                          final String loopCount,
                          final String threadsNumber,
                          final String nodes,
                          final String parameters,
                          final String jvmOptions)
    {
        this.cycloneUrl = requireNonNull(cycloneUrl);
        this.parcelId = requireNonNull(parcelId);
        this.testName = requireNonNull(testName);
        this.loopCount = requireNonNull(loopCount);
        this.threadsNumber = requireNonNull(threadsNumber);
        this.nodes = requireNonNull(nodes);
        this.parameters = requireNonNull(parameters);
        this.jvmOptions = requireNonNull(jvmOptions);
    }

    private List<String> parseNodes(final String nodes) {
        return Arrays.stream(nodes.split(",")).collect(Collectors.toList());
    }

    private Map<String, String> parseParameters(final String parameters) {
        if (parameters.isEmpty()) {
            return ImmutableMap.of();
        }
        return Arrays.stream(parameters.split(";"))
            .map(keyValue -> keyValue.split("=", 2))
            .peek(keyValue -> {
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException();
                }
            })
            .collect(Collectors.toMap(keyValue -> keyValue[0], keyValue -> keyValue[1]));
    }

    @Override
    public boolean perform(final AbstractBuild<?, ?> build, final Launcher launcher, final BuildListener listener)
    {
        final CycloneClient cycloneClient = ApacheCycloneClient.builder()
            .withApiUrl(cycloneUrl)
            .build();

        final ImmutableTestExecutionProperties testExecutionProperties = ImmutableTestExecutionProperties.builder()
            .parcelId(UUID.fromString(parcelId))
            .testName(testName)
            .loopCount(Integer.parseInt(loopCount))
            .threadsNumber(Integer.parseInt(threadsNumber))
            .nodes(parseNodes(nodes))
            .parameters(parseParameters(parameters))
            .jvmOptions(jvmOptions)
            .build();

        final PrintStream logger = listener.getLogger();
        logger.printf("About to start test with properties '%s'...\n", testExecutionProperties);
        final UUID testExecutionId = cycloneClient.startTest(testExecutionProperties);
        logger.printf("Test started with id '%s'.\n", testExecutionId);

        boolean testsCompleted = false;
        boolean testFailed = false;
        Instant lastRead = Instant.now();
        while (!testsCompleted) {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            final Set<TestExecution> testExecutions = cycloneClient.getTestExecutions(testExecutionId);
            final Map<String, String> nodeStates = testExecutions.stream()
                .collect(Collectors.toMap(TestExecution::getNode, TestExecution::getState));

            nodeStates.forEach((node, state) -> logger.printf("Test state on %s=%s.\n", node, state));
            testsCompleted = testExecutions.stream()
                .map(TestExecution::getState)
                .allMatch(state -> ImmutableSet.of("ENDED", "ABORTED", "ERROR").contains(state));
            testFailed = testExecutions.stream()
                .map(TestExecution::getState)
                .anyMatch(state -> ImmutableSet.of("ABORTED", "ERROR").contains(state));
            if (testFailed) {
                listener.error("Test failure with executions '%s'", testExecutions);
            }
            cycloneClient.getTestExecutionLogMessages(testExecutionId, lastRead).stream()
                .map(logLine -> logLine + "\n")
                .forEach(logger::printf);
            lastRead = Instant.now();
        }

        logger.print("Test execution completed.\n");
        logger.print("Getting metrics...\n");
        final Set<MetricDataPoint> metrics = cycloneClient.getTestExecutionMetrics(testExecutionId);
        logger.printf("Test execution metrics '%s'.\n", metrics);
        return !testFailed;
    }

    @Extension
    public static final class BuildDescriptor extends BuildStepDescriptor<Builder>
    {
        public FormValidation doCheckCycloneUrl(final @QueryParameter String value) {
            return checkEmptyString(value, "Cyclone url");
        }

        public FormValidation doCheckTestName(final @QueryParameter String value) {
            return checkEmptyString(value, "Test name");
        }

        public FormValidation doCheckParcelId(@QueryParameter String value) {
            try {
                //noinspection ResultOfMethodCallIgnored
                UUID.fromString(value);
                return FormValidation.ok();
            } catch (IllegalArgumentException e) {
                return FormValidation.error(e.getMessage());
            }
        }

        public FormValidation doCheckLoopCount(final @QueryParameter String value) {
            return checkInteger(value);
        }

        public FormValidation doCheckThreadsNumber(final @QueryParameter String value) {
            return checkInteger(value);
        }

        private FormValidation checkInteger(final String value)
        {
            try {
                Integer.parseInt(value);
                return FormValidation.ok();
            } catch (NumberFormatException e) {
                return FormValidation.error("'" + value + "' is not an integer.");
            }
        }

        public FormValidation doCheckNodes(final @QueryParameter String value) {
            return checkEmptyString(value, "Node");
        }

        private FormValidation checkEmptyString(final String value, final String parameterName)
        {
            if (value.isEmpty()) {
                return FormValidation.error(parameterName + " can't be empty.");
            } else {
                return FormValidation.ok();
            }
        }

        public FormValidation doCheckParameters(final @QueryParameter String value) {
            if (value.isEmpty() || value.matches("(\\w+)=([^\\s]+)")) {
                return FormValidation.ok();
            } else {
                return FormValidation.error("'" + value + "' is not a comma separated key value.");
            }
        }

        @Override
        public boolean isApplicable(final Class<? extends AbstractProject> aClass)
        {
            return true;
        }

        @Override
        public String getDisplayName()
        {
            return "Run Cyclone Test";
        }
    }
}
