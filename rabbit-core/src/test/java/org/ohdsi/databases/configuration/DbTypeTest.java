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
package org.ohdsi.databases.configuration;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class DbTypeTest {

    @Test
    void testPickList() {
        List<String> labelsFromAllDbTypeValues = Stream.of(DbType.values()).map(DbType::label).sorted().collect(Collectors.toList());
        List<String> labelsFromPickList = Stream.of(DbType.pickList()).sorted().collect(Collectors.toList());

        assertEquals(labelsFromAllDbTypeValues, labelsFromPickList, "The picklist should contain all the labels defined in the DbType enum");
    }
}