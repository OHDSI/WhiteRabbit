/*******************************************************************************
 * Copyright 2023 Observational Health Data Sciences and Informatics & The Hyve
 *
 * This file is part of WhiteRabbit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.databases;

import org.apache.commons.lang.StringUtils;
import org.ohdsi.databases.configuration.DbSettings;
import org.ohdsi.databases.configuration.DbType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.BooleanSupplier;

public class SnowflakeTestUtils {

    public static String getEnvOrFail(String name) {
        String value = System.getenv(name);
        if (StringUtils.isEmpty(value)) {
            throw new RuntimeException(String.format("Environment variable '%s' is not set.", name));
        }

        return value;
    }

    public static String getPropertyOrFail(String name) {
        String value = System.getProperty(name);
        if (StringUtils.isEmpty(value)) {
            throw new RuntimeException(String.format("System property '%s' is not set.", name));
        }

        return value;
    }

    @FunctionalInterface
    public interface ReaderInterface {
        String getOrFail(String name);
    }

    public static class EnvironmentReader implements ReaderInterface {
        public String getOrFail(String name) {
            return getEnvOrFail(name);
        }
    }
    public static class PropertyReader implements ReaderInterface {
        public String getOrFail(String name) {
            return getPropertyOrFail(name);
        }
    }

    public static class SnowflakeSystemPropertiesFileChecker implements BooleanSupplier {
        @Override
        public boolean getAsBoolean() {
            String buildDirectory = System.getProperty("projectBuildDirectory");
            Path snowflakeEnvVarPath = Paths.get(buildDirectory,"../..", "snowflake.env");
            if (StringUtils.isNotEmpty(buildDirectory) && Files.exists(snowflakeEnvVarPath)) {
                try {
                    loadSystemProperties(snowflakeEnvVarPath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            // check the endpoint here and return either true or false
            return false;
        }

        private void loadSystemProperties(Path envVarFile) throws IOException {
            Files.lines(envVarFile)
                    .map(line -> line.replaceAll("^export ", ""))
                    .map(line2 -> line2.split("=", 2))
                    .forEach(v -> System.setProperty(v[0], v[1]));
        }
    }
}
