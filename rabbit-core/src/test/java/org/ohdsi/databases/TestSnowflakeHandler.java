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
import org.junit.jupiter.api.Test;
import org.ohdsi.databases.configuration.*;
import org.ohdsi.utilities.files.IniFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.ohdsi.databases.SnowflakeHandler.*;

class TestSnowflakeHandler {

    Logger logger = LoggerFactory.getLogger(TestSnowflakeHandler.class);

    @Test
    void testPrintIniFileTemplate() throws IOException {
        String output;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); PrintStream printStream = new PrintStream(outputStream)) {
            DBConfiguration configuration = new SnowflakeConfiguration();
            configuration.printIniFileTemplate(printStream);
            output = outputStream.toString();
            for (ConfigurationField field: configuration.getFields()) {
                assertTrue(output.contains(field.name), String.format("ini file template should contain field name (%s)", field.name));
                assertTrue(output.contains(field.toolTip), String.format("ini file template should contain tool tip (%s)", field.toolTip));
                if (!StringUtils.isEmpty(field.getDefaultValue())) {
                    assertTrue(output.contains(field.getDefaultValue()), String.format("ini file template should contain default value (%s)", field.getDefaultValue()));
                }
            }
        }
    }

    @Test
    void testLoadAndValidateConfiguration() {
        DBConfiguration snowflakeConfiguration = new SnowflakeConfiguration();
        IniFile iniFile = new IniFile();

        iniFile.set(DBConfiguration.DATA_TYPE_FIELD, DbType.SNOWFLAKE.name());

        // start with no values set, should generate an error for each required field
        ValidationFeedback feedback = snowflakeConfiguration.loadAndValidateConfiguration(iniFile);
        assertFalse(feedback.hasWarnings());
        assertTrue(feedback.hasErrors());
        assertEquals(6,feedback.getErrors().size());

        // fill in all required fields, verify no errors
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_ACCOUNT, "some-account");
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_USER, "some-user");
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_PASSWORD, "some-password");
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_WAREHOUSE, "some-warehouse");
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_DATABASE, "some-database");
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_SCHEMA, "some-schema");

        feedback = snowflakeConfiguration.loadAndValidateConfiguration(iniFile);
        assertFalse(feedback.hasWarnings());
        assertFalse(feedback.hasErrors());

        // add (invalid) value for authenticator field, should generate two errors
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_AUTHENTICATOR, "some-value");

        feedback = snowflakeConfiguration.loadAndValidateConfiguration(iniFile);
        assertFalse(feedback.hasWarnings());
        assertTrue(feedback.hasErrors());
        assertEquals(2,feedback.getErrors().size());
        assertTrue(feedback.getErrors().containsKey(SnowflakeConfiguration.ERROR_MUST_NOT_SET_PASSWORD_AND_AUTHENTICATOR),
                "there should be an error indicating that both password and authenticator were set");
        assertEquals(1,
                (int) new ArrayList<>(feedback.getErrors().keySet()).stream().filter(k -> k.startsWith(SnowflakeConfiguration.ERROR_VALUE_CAN_ONLY_BE_ONE_OF)).count(),
                "there should be an error indicating that a wrong value was set for the authenticator field");

        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_PASSWORD, null);
        iniFile.set(SnowflakeConfiguration.SNOWFLAKE_AUTHENTICATOR, "externalbrowser");
        feedback = snowflakeConfiguration.loadAndValidateConfiguration(iniFile);
        assertFalse(feedback.hasWarnings());
        assertFalse(feedback.hasErrors());
    }
}