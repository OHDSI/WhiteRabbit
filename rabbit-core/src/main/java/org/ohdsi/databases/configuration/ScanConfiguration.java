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

import org.apache.commons.lang.StringUtils;
import org.ohdsi.utilities.files.IniFile;

import java.io.PrintStream;
import java.util.*;

/**
 * Configuration for a database scan.
 *
 * This class defines the common configuration for a database scan. It defines the parameters that are used to scan a
 * database. It serves as the base class for the configuration of specific database types.
 */
public abstract class ScanConfiguration {
    public static final String DATA_TYPE_FIELD = "DATA_TYPE";
    public static final String DELIMITER_FIELD = "DELIMITER";
    public static final String TABLES_TO_SCAN_FIELD = "TABLES_TO_SCAN";
    public static final String SCAN_FIELD_VALUES_FIELD = "SCAN_FIELD_VALUES";
    public static final String MIN_CELL_COUNT_FIELD = "MIN_CELL_COUNT";
    public static final String MAX_DISTINCT_VALUES_FIELD = "MAX_DISTINCT_VALUES";
    public static final String ROWS_PER_TABLE_FIELD = "ROWS_PER_TABLE";
    public static final String CALCULATE_NUMERIC_STATS_FIELD = "CALCULATE_NUMERIC_STATS";
    public static final String NUMERIC_STATS_SAMPLER_SIZE_FIELD = "NUMERIC_STATS_SAMPLER_SIZE";
    public static final String ERROR_DUPLICATE_DEFINITIONS_FOR_FIELD = "Multiple definitions for field ";
    protected ConfigurationFields configurationFields;

    public ScanConfiguration(ConfigurationField... fields) {
        this.checkForDuplicates(fields);
        this.configurationFields = new ConfigurationFields(fields);
    }

    public static ConfigurationField[] createScanConfigurationFields() {
        return new ConfigurationField[]{
                ConfigurationField.create(DELIMITER_FIELD,
                                "",
                                "")
                        .defaultValue(",")
                        .required(),
                ConfigurationField.create(TABLES_TO_SCAN_FIELD,
                                "",
                                "")
                        .defaultValue("*")
                        .required(),
                ConfigurationField.create(SCAN_FIELD_VALUES_FIELD,
                                "",
                                "")
                        .defaultValue("yes")
                        .required(),
                ConfigurationField.create(MIN_CELL_COUNT_FIELD,
                                "",
                                "")
                        .defaultValue("5")
                        .integerValue()
                        .required(),
                ConfigurationField.create(MAX_DISTINCT_VALUES_FIELD,
                                "",
                                "")
                        .defaultValue("1000")
                        .integerValue()
                        .required(),
                ConfigurationField.create(ROWS_PER_TABLE_FIELD,
                                "",
                                "")
                        .defaultValue("100000")
                        .integerValue()
                        .required(),
                ConfigurationField.create(CALCULATE_NUMERIC_STATS_FIELD,
                                "",
                                "")
                        .defaultValue("no")
                        .yesNoValue()
                        .required(),
                ConfigurationField.create(NUMERIC_STATS_SAMPLER_SIZE_FIELD,
                        "",
                        "")
                        .defaultValue("500")
                        .integerValue()
                        .required()
        };
    }

    public IniFile toIniFile() {
        IniFile iniFile = new IniFile();
        this.configurationFields.getFields().forEach(f -> {
            iniFile.set(f.name, f.getValue());
        });

        return iniFile;
    }

    public DbSettings toDbSettings(ValidationFeedback feedback) {
        throw new ScanConfigurationException("Should be implemented by inheriting classes");
    }

    private void checkForDuplicates(ConfigurationField... fields) {
        Set<String> names = new HashSet<>();
        for (ConfigurationField field : fields) {
            if (names.contains(field.name)) {
                throw new ScanConfigurationException(ERROR_DUPLICATE_DEFINITIONS_FOR_FIELD + field.name);
            }
            names.add(field.name);
        }
    }

    public ValidationFeedback loadAndValidateConfiguration(IniFile iniFile) throws ScanConfigurationException {
        for (ConfigurationField field : this.getFields()) {
            field.setValue(iniFile.get(field.name));
        }

        return this.validateAll();
    }

    public ValidationFeedback validateAll() {
        ValidationFeedback configurationFeedback = new ValidationFeedback();
        for (ConfigurationField field : this.getFields()) {
            for (FieldValidator validator : field.validators) {
                ValidationFeedback feedback = validator.validate(field);
                configurationFeedback.add(feedback);
            }
        }

        configurationFeedback.add(configurationFields.validate());

        return configurationFeedback;
    }

    public List<ConfigurationField> getFields() {
        return configurationFields.getFields();
    }

    public ConfigurationField getField(String fieldName) {
        return this.getFields().stream().filter(f -> f.name.equalsIgnoreCase(fieldName)).findFirst().orElse(null);
    }

    public String getValue(String fieldName) {
        Optional<ConfigurationField> field = getFields().stream().filter(f -> fieldName.equalsIgnoreCase(f.name)).findFirst();
        if (field.isPresent()) {
            return field.get().getValue();
        } else {
            return "";
        }
    }

    public void printIniFileTemplate(PrintStream stream) {
        for (ConfigurationField field : this.configurationFields.getFields()) {
            stream.printf("%s: %s\t%s%n",
                    field.name,
                    StringUtils.isEmpty(field.getDefaultValue()) ? "_" : field.getDefaultValue(),
                    field.toolTip);
        }
    }
}
