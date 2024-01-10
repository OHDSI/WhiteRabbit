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
package org.ohdsi.utilities;

public interface ScanFieldName {
    String TABLE = "Table";
    String FIELD = "Field";
    String DESCRIPTION = "Description";
    String TYPE = "Type";
    String MAX_LENGTH = "Max length";
    String N_ROWS = "N rows";
    String N_ROWS_CHECKED = "N rows checked";
    String FRACTION_EMPTY = "Fraction empty";
    String UNIQUE_COUNT = "N unique values";
    String FRACTION_UNIQUE = "Fraction unique";
    String AVERAGE = "Average";
    String STDEV = "Standard Deviation";
    String MIN = "Min";
    String Q1 = "25%";
    String Q2 = "Median";
    String Q3 = "75%";
    String MAX = "Max";
    String N_FIELDS = "N Fields";
    String N_FIELDS_EMPTY = "N Fields Empty";
}
