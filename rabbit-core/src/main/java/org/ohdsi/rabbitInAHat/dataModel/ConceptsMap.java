/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics & The Hyve
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

public class ConceptsMap {

    private Map<String, Map<String, List<Concept>>> conceptMap;

    public ConceptsMap() {
        this.conceptMap = new HashMap<>();
    }

    public void put(String targetTable, String targetField, Concept concept) {
        if (!this.conceptMap.containsKey(targetTable)) {
            this.conceptMap.put(targetTable, new HashMap<>());
        }
        if (!this.conceptMap.get(targetTable).containsKey(targetField)) {
            this.conceptMap.get(targetTable).put(targetField, new ArrayList<>());
        }
        this.conceptMap.get(targetTable).get(targetField).add(concept);
    }

    public List<Concept> get(String targetTable, String targetField) {
        if (!this.containsKey(targetTable, targetField)) {
            return null;
        }
        return this.conceptMap.get(targetTable).get(targetField);
    }

    public boolean containsKey(String targetTable, String targetField) {
        if (!this.conceptMap.containsKey(targetTable)) {
            return false;
        }
        if (!this.conceptMap.get(targetTable).containsKey(targetField)) {
            return false;
        }
        return true;
    }

    public void load(String filename) {
        InputStream conceptStream = Database.class.getResourceAsStream(filename);

        try {
            for (CSVRecord conceptRow : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(conceptStream))) {
                String tableName = conceptRow.get("omop_cdm_table");
                String fieldName = conceptRow.get("omop_cdm_field");

                Concept concept = new Concept();
                concept.setConceptId(conceptRow.get("concept_id"));
                concept.setConceptName(conceptRow.get("concept_name"));
                concept.setStandardConcept(conceptRow.get("standard_concept"));

                this.put(tableName, fieldName, concept);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public class Concept {
        private String conceptId;
        private String conceptName;
        private String standardConcept;

        public Concept() {
        }

        public Concept(String conceptId, String conceptName) {
            this.conceptId = conceptId;
            this.conceptName = conceptName;
        }

        public String getConceptId() {
            return conceptId;
        }

        public void setConceptId(String conceptId) {
            this.conceptId = conceptId;
        }

        public String getConceptName() {
            return conceptName;
        }

        public void setConceptName(String conceptName) {
            this.conceptName = conceptName;
        }

        public String getStandardConcept() {
            return standardConcept;
        }

        public void setStandardConcept(String standardConcept) {
            this.standardConcept = standardConcept;
        }

        public String toString() {
            return this.conceptId + " -- " + this.conceptName;
        }
    }
}
