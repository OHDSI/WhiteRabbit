/*******************************************************************************
 * Copyright 202 Observational Health Data Sciences and Informatics
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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateUtilities {
    /**
     * Parses a date to millis. Copies behaviour of StringUtilities.isDate()
     * Two recognised date formats:
     *  - yyyy*dd*MM
     *  - MM*dd*yy
     * If the time included, the value is NOT recognised as a date.
     * @param dateString value to be parsed
     * @return long date as time in millis
     */
    public static long parseDate(String dateString) {
        String pattern = "";
        if (dateString.length() == 10) {
            char separator = dateString.charAt(4);
            pattern = "yyyy" + separator + "MM" + separator + "dd";
        } else if (dateString.length() == 8) {
            char separator = dateString.charAt(2);
            pattern = "MM" + separator + "dd" + separator + "yy";
        }

        LocalDate localDate = LocalDate.from(DateTimeFormatter.ofPattern(pattern).parse(dateString));
        return localDate.toEpochDay();
    }
}
