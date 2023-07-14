package org.ohdsi.whiterabbit.scan;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.TimeoutException;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Intent: "deploy" the distributed application in a docker container containing a Java 8 runtime,
 * and run a set of tests that aim to verify that the distribution is complete, i.e. no dependencies
 * are missing. Typical tests are those that connect to a database of brand X and connect to it, so that the
 * distribution is known to have all dependencies present required to connect to a database of brand X.
 */
@Testcontainers(disabledWithoutDocker = true)
public class VerifyDistributionIT {

    @Container
    static GenericContainer<?> java8Container = new GenericContainer<>(
            DockerImageName.parse("eclipse-temurin:8"))
            .withCommand("sh", "-c", "tail -f /dev/null")
            .withFileSystemBind(Paths.get("../dist").toAbsolutePath().toString(), "/app");

    @BeforeAll
    public static void beforeAll() {
        try {
            java8Container.start();
        } catch (ContainerLaunchException | TimeoutException e) {
            String logs = java8Container.getLogs();
            System.out.println(logs);
        }
    }

    @AfterAll
    public static void afterAll() {
        java8Container.stop();
    }

    @Test
    public void testContainerSetup() throws IOException, InterruptedException {
        ExecResult execResult = java8Container.execInContainer("sh", "-c", "java -version");
        assertNotNull(execResult);
        assertTrue(execResult.getStderr().startsWith("openjdk version \"1.8.0_"));
        execResult = java8Container.execInContainer("sh", "-c", "ls /app");
        // please note: the test below verifies that the distribution had been generated
        // this works
        assertTrue(execResult.getStdout().contains("repo"));
    }
}
