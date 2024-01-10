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

import one.util.streamex.EntryStream;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ValidationFeedback {
    private Map<String, List<ConfigurationField>> warnings = new HashMap<>();
    private Map<String, List<ConfigurationField>> errors = new HashMap<>();

    public boolean isFullyValid() {
        return warnings.isEmpty() && errors.isEmpty();
    }

    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public Map<String, List<ConfigurationField>> getWarnings() {
        return this.warnings;
    }

    public Map<String, List<ConfigurationField>> getErrors() {
        return this.errors;
    }

    public void addWarning(String warning, ConfigurationField field) {
        if (this.warnings.containsKey(warning)) {
            this.warnings.get(warning).add(field);
        } else {
            this.warnings.put(warning, Collections.singletonList(field));
        }
    }

    public void addError(String error, ConfigurationField field) {
        if (this.errors.containsKey(error)) {
            this.errors.get(error).add(field);
        } else {
            this.errors.put(error, Stream.of(field).collect(Collectors.toList()));
        }
    }

    public void add(ValidationFeedback feedback) {
        this.warnings = EntryStream.of(this.warnings)
                .append(EntryStream.of(feedback.getWarnings()))
                .toMap((e1, e2) -> e1);
        this.errors = EntryStream.of(this.errors)
                .append(EntryStream.of(feedback.getErrors()))
                .toMap((e1, e2) -> e1);
    }
}
