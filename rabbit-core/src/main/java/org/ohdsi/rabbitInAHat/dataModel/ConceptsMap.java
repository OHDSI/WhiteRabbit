/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
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
package org.ohdsi.rabbitInAHat.dataModel;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConceptMap {

    private Map<String, Map<String, List<Concept>>> conceptMap;

    public ConceptMap() {
        this.conceptMap = new HashMap<>();
    }

    public void put(String targetTable, String targetField) {
        if (!this.conceptMap.containsKey(targetTable)) {
            this.conceptMap.put(targetTable, new HashMap<>());
        }
        this.conceptMap.put(targetTable)
    }

    public Concept get(String targetTable, String targetField) {
        return this.conceptMap.get(targetTable).get(targetField);
    }

    public void load(String filename) {
        InputStream conceptStream = Database.class.getResourceAsStream("concept_id_hints.csv");
        Map<String, List<String>> conceptsMap = new HashMap<>();
        try {
            for (CSVRecord conceptRow : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(conceptStream))) {
                String tableName = conceptRow.get("omop_cdm_table");
                String fieldName = conceptRow.get("omop_cdm_field");
                String key = tableName + fieldName;

                String conceptId = conceptRow.get("concept_id");
                String conceptName = conceptRow.get("concept_name");
                String value = conceptId + " | " + conceptName;

                if (!conceptsMap.containsKey(key)) {
                    conceptsMap.put(key, new ArrayList<>());
                }
                conceptsMap.get(key).add(value);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private class Concept {
        private int conceptId;
        private String conceptName;
        private String targetTable;
        private String targetField;

        public Concept(int conceptId, String conceptName) {
            this.conceptId = conceptId;
            this.conceptName = conceptName;
        }

        public int getConceptId() {
            return conceptId;
        }

        public void setConceptId(int conceptId) {
            this.conceptId = conceptId;
        }

        public String getConceptName() {
            return conceptName;
        }

        public void setConceptName(String conceptName) {
            this.conceptName = conceptName;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }

        public String getTargetField() {
            return targetField;
        }

        public void setTargetField(String targetField) {
            this.targetField = targetField;
        }

        public String toString() {
            return this.conceptId + ' --' + this.conceptName;
        }
    }
}
