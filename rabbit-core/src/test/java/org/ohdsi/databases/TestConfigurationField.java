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

import org.junit.jupiter.api.Test;
import org.ohdsi.databases.configuration.ConfigurationField;
import org.ohdsi.databases.configuration.ScanConfiguration;
import org.ohdsi.databases.configuration.FieldValidator;
import org.ohdsi.databases.configuration.ValidationFeedback;

import static org.junit.jupiter.api.Assertions.*;
import static org.ohdsi.databases.configuration.ConfigurationField.*;

class TestConfigurationField {

    @Test
    void testStandardValidators() {
        final String REQUIRED_FIELD = "REQUIRED_FIELD";
        final String OPTIONAL_INTEGER_FIELD = "OPTIONAL_INTEGER_FIELD";
        final String REQUIRED_INTEGER_FIELD = "REQUIRED_INTEGER_FIELD";
        final String OPTIONAL_YESNO_FIELD = "OPTIONAL_YESNO_FIELD";
        final String REQUIRED_YESNO_FIELD = "REQUIRED_YESNO_FIELD";
        ScanConfiguration configuration = new ScanConfigurationForTest(
                ConfigurationField
                        .create(REQUIRED_FIELD, REQUIRED_FIELD, "")
                        .required(),
                ConfigurationField
                        .create(OPTIONAL_INTEGER_FIELD, OPTIONAL_INTEGER_FIELD, "")
                        .integerValue(),
                ConfigurationField
                        .create(REQUIRED_INTEGER_FIELD, REQUIRED_INTEGER_FIELD, "")
                        .integerValue()
                        .required(),
                ConfigurationField
                        .create(OPTIONAL_YESNO_FIELD, OPTIONAL_YESNO_FIELD, "")
                        .yesNoValue(),
                ConfigurationField
                        .create(REQUIRED_YESNO_FIELD, REQUIRED_YESNO_FIELD, "")
                        .yesNoValue()
                        .required()
        );

        // test for values in required fields
        ValidationFeedback feedback = configuration.validateAll();
        assertEquals(0, feedback.getWarnings().size());
        assertEquals(3, feedback.getErrors().size());
        String expectedErrorKey = String.format(VALUE_REQUIRED_FORMAT_STRING, REQUIRED_FIELD, REQUIRED_FIELD);
        assertTrue(feedback.getErrors().containsKey(expectedErrorKey));
        assertTrue(feedback.getErrors().get(expectedErrorKey).get(0).name.equalsIgnoreCase(REQUIRED_FIELD));
        expectedErrorKey = String.format(VALUE_REQUIRED_FORMAT_STRING, REQUIRED_INTEGER_FIELD, REQUIRED_INTEGER_FIELD);
        assertTrue(feedback.getErrors().containsKey(expectedErrorKey));
        assertTrue(feedback.getErrors().get(expectedErrorKey).get(0).name.equalsIgnoreCase(REQUIRED_INTEGER_FIELD));
        expectedErrorKey = String.format(VALUE_REQUIRED_FORMAT_STRING, REQUIRED_YESNO_FIELD, REQUIRED_YESNO_FIELD);
        assertTrue(feedback.getErrors().containsKey(expectedErrorKey));
        assertTrue(feedback.getErrors().get(expectedErrorKey).get(0).name.equalsIgnoreCase(REQUIRED_YESNO_FIELD));

        // set (valid) values where required
        configuration.getField(REQUIRED_FIELD).setValue("some value");
        configuration.getField(REQUIRED_INTEGER_FIELD).setValue("123");
        configuration.getField(REQUIRED_YESNO_FIELD).setValue("yes");
        feedback = configuration.validateAll();
        assertEquals(0, feedback.getWarnings().size());
        assertEquals(0, feedback.getErrors().size());

        // set some bogus values
        configuration.getField(REQUIRED_INTEGER_FIELD).setValue("abc");
        configuration.getField(REQUIRED_YESNO_FIELD).setValue("of course!");
        configuration.getField(OPTIONAL_YESNO_FIELD).setValue("maybe not?");
        feedback = configuration.validateAll();
        assertEquals(0, feedback.getWarnings().size());
        assertEquals(3, feedback.getErrors().size());
        expectedErrorKey = String.format(INTEGER_VALUE_REQUIRED_FORMAT_STRING, REQUIRED_INTEGER_FIELD, REQUIRED_INTEGER_FIELD);
        assertTrue(feedback.getErrors().containsKey(expectedErrorKey));
        assertTrue(feedback.getErrors().get(expectedErrorKey).get(0).name.equalsIgnoreCase(REQUIRED_INTEGER_FIELD));
        expectedErrorKey = String.format(ONLY_YESNO_ALLOWED_FORMAT_STRING, REQUIRED_YESNO_FIELD, REQUIRED_YESNO_FIELD);
        assertTrue(feedback.getErrors().containsKey(expectedErrorKey));
        assertTrue(feedback.getErrors().get(expectedErrorKey).get(0).name.equalsIgnoreCase(REQUIRED_YESNO_FIELD));
        expectedErrorKey = String.format(ONLY_YESNO_ALLOWED_FORMAT_STRING, OPTIONAL_YESNO_FIELD, OPTIONAL_YESNO_FIELD);
        assertTrue(feedback.getErrors().containsKey(expectedErrorKey));
        assertTrue(feedback.getErrors().get(expectedErrorKey).get(0).name.equalsIgnoreCase(OPTIONAL_YESNO_FIELD));

        // and test the normalization of a yes/no field
        configuration.getField(REQUIRED_INTEGER_FIELD).setValue("0"); // no error wanted here either
        configuration.getField(REQUIRED_YESNO_FIELD).setValue("YeS");
        configuration.getField(OPTIONAL_YESNO_FIELD).setValue("NO");
        feedback = configuration.validateAll();
        assertEquals(0, feedback.getWarnings().size());
        assertEquals(0, feedback.getErrors().size());
        assertEquals("yes", configuration.getField(REQUIRED_YESNO_FIELD).getValue());
        assertEquals("no", configuration.getField(OPTIONAL_YESNO_FIELD).getValue());
    }

    static class WarningValidator implements FieldValidator {
        final static String expectedValue = "Expected value";
        final static String warning = "Field does not contain the expected value!";
        @Override
        public ValidationFeedback validate(ConfigurationField field) {
            ValidationFeedback feedback = new ValidationFeedback();

            if (!field.getValue().equalsIgnoreCase(expectedValue)) {
                feedback.addWarning(warning, field);
            }

            return feedback;
        }
    }

    @Test
    void testBespokeWarningValidator() {
        final String FIELD_NAME = "FieldName";
        ScanConfiguration configuration = new ScanConfigurationForTest(
                ConfigurationField
                        .create(FIELD_NAME, FIELD_NAME, "")
                        .addValidator(new WarningValidator())
                        .setValue(""));

        ValidationFeedback feedback = configuration.validateAll();
        assertEquals(1, feedback.getWarnings().size());
        assertEquals(0, feedback.getErrors().size());
        assertTrue(feedback.getWarnings().get(WarningValidator.warning).get(0).name.equalsIgnoreCase(FIELD_NAME));

        configuration.getFields().get(0).setValue(WarningValidator.expectedValue);
        feedback = configuration.validateAll();
        assertEquals(0, feedback.getWarnings().size());
        assertEquals(0, feedback.getErrors().size());
    }
}