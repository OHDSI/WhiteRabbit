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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class ConceptsMap {

    private Map<String, Map<String, List<Concept>>> conceptMap;
    public String vocabularyVersion;

    private ConceptsMap() {
        this.conceptMap = new HashMap<>();
    }

    ConceptsMap(String filename) throws IOException{
        this();
        this.load(filename);
    }

    private void load(String filename) throws IOException{
        try (InputStream conceptStream = Database.class.getResourceAsStream(filename)) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(conceptStream));
            vocabularyVersion = bufferedReader.readLine();

            for (CSVRecord conceptRow : CSVFormat.RFC4180.withHeader().parse(bufferedReader)) {
                String omopTableName = conceptRow.get("omop_cdm_table");
                String omopFieldName = conceptRow.get("omop_cdm_field");

                Concept concept = new Concept();
                concept.setConceptId(conceptRow.get("concept_id"));
                concept.setConceptName(conceptRow.get("concept_name"));
                concept.setStandardConcept(conceptRow.get("standard_concept"));

                // Optional fields
                if (conceptRow.isSet("domain_id")) {
                    concept.setDomainId(conceptRow.get("domain_id"));
                }

                if (conceptRow.isSet("vocabulary_id")) {
                    concept.setVocabularyId(conceptRow.get("vocabulary_id"));
                }

                if (conceptRow.isSet("concept_class_id")) {
                    concept.setConceptClassId(conceptRow.get("concept_class_id"));
                }

                this.put(omopTableName, omopFieldName, concept);
            }
        } catch (IOException e) {
            throw new IOException("Could not load concept_id hints: " + e.getMessage());
        }
    }

    public void put(String targetTableName, String targetFieldName, Concept concept) {
        this.conceptMap
                .computeIfAbsent(targetTableName, t -> new HashMap<>())
                .computeIfAbsent(targetFieldName, t -> new ArrayList<>())
                .add(concept);
    }

    public List<Concept> get(String targetTable, String targetField) {
        return conceptMap.getOrDefault(targetTable, Collections.emptyMap()).get(targetField);
    }

    public static class Concept {
        private String conceptId;
        private String conceptName;
        private String standardConcept;
        private String domainId;
        private String vocabularyId;
        private String conceptClassId;

        public String getConceptId() {
            return conceptId;
        }

        void setConceptId(String conceptId) {
            this.conceptId = conceptId;
        }

        public String getConceptName() {
            return conceptName;
        }

        void setConceptName(String conceptName) {
            this.conceptName = conceptName;
        }

        public String getStandardConcept() {
            return standardConcept;
        }

        void setStandardConcept(String standardConcept) {
            this.standardConcept = standardConcept;
        }

        public String getDomainId() {
            return domainId;
        }

        public void setDomainId(String domainId) {
            this.domainId = domainId;
        }

        public String getVocabularyId() {
            return vocabularyId;
        }

        public void setVocabularyId(String vocabularyId) {
            this.vocabularyId = vocabularyId;
        }

        public String getConceptClassId() {
            return conceptClassId;
        }

        public void setConceptClassId(String conceptClassId) {
            this.conceptClassId = conceptClassId;
        }

        public String toString() {
            return this.conceptId + " -- " + this.conceptName;
        }
    }
}
