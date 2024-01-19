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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class ConfigurationFields {
    List<ConfigurationField> fields;
    List<ConfigurationValidator> validators = new ArrayList<>();

    public ConfigurationFields(ConfigurationField... fields) {
        this.fields = new ArrayList<>(Arrays.asList(fields));
    }

    public void addValidator(ConfigurationValidator validator) {
        this.validators.add(validator);
    }

    public List<ConfigurationField> getFields() {
        return this.fields;
    }

    public ConfigurationField get(String fieldName) {
        Optional<ConfigurationField> field = fields.stream().filter(f -> fieldName.equalsIgnoreCase(f.name)).findFirst();
        if (field.isPresent()) {
            return field.get();
        }

        throw new DBConfigurationException(String.format("No ConfigurationField object found for field name '%s'", fieldName));
    }

    public String getValue(String fieldName) {
        Optional<String> value = this.fields.stream().filter(f -> fieldName.equalsIgnoreCase(f.name)).map(ConfigurationField::getValue).findFirst();
        return (value.orElse(""));
    }

    public ValidationFeedback validate() {
        ValidationFeedback allFeedback = new ValidationFeedback();
        for (ConfigurationValidator validator : this.validators) {
            allFeedback.add(validator.validate(this));
        }

        return allFeedback;
    }
}
