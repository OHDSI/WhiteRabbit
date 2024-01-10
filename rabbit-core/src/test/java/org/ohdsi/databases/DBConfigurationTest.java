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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ohdsi.databases.configuration.ConfigurationField;
import org.ohdsi.databases.configuration.DBConfiguration;
import org.ohdsi.databases.configuration.DBConfigurationException;

import static org.junit.jupiter.api.Assertions.*;
import static org.ohdsi.databases.configuration.DBConfiguration.ERROR_DUPLICATE_DEFINITIONS_FOR_FIELD;

class DBConfigurationTest {

    private final String NAME_FIELD1 = "FIELD_1";
    private final String LABEL_FIELD1 = "Field one";
    private final String TOOLTIP_FIELD1 = "Tooltip for field one";
    private final String NAME_FIELD2 = "FIELD_2";
    private final String LABEL_FIELD2 = "Field two";
    private final String TOOLTIP_FIELD2 = "Tooltip for field two";

    @BeforeEach
    void setUp() {
    }

    @Test
    void doNotAcceptDuplicateDefinitionsForField() {
    Exception exception = assertThrows(DBConfigurationException.class, () -> {
        DBConfiguration testConfiguration = new DBConfiguration(
                ConfigurationField.create(NAME_FIELD1, LABEL_FIELD1, TOOLTIP_FIELD1).required(),
                ConfigurationField.create(NAME_FIELD1, LABEL_FIELD2, TOOLTIP_FIELD2));
        });
        assertTrue(exception.getMessage().startsWith(ERROR_DUPLICATE_DEFINITIONS_FOR_FIELD));
    }

    @Test
    void getFields() {
    }

    @Test
    void printIniFileTemplate() {
    }
}