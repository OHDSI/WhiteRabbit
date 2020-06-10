/*******************************************************************************
 * Copyright 2020 Observational Health Data Sciences and Informatics
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

import java.util.ArrayList;

public class ValueCounts {
    private ArrayList<ValueCounts.ValueCount> valueCounts = new ArrayList<>();
    private int totalFrequency = 0;
    private String mostFrequentValue;
    private int mostFrequentValueCount = -1;

    public class ValueCount {
        private String value;
        private int frequency;

        public ValueCount(String value, int frequency) {
            this.value = value;
            this.frequency = frequency;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getFrequency() {
            return frequency;
        }
    }

    public boolean add(String value, int count) {
        totalFrequency += count;
        if (count > mostFrequentValueCount) {
            mostFrequentValue = value;
            mostFrequentValueCount = count;
        }
        return valueCounts.add(new ValueCount(value, count));
    }

    public ArrayList<ValueCounts.ValueCount> getAll() {
        return valueCounts;
    }

    public ValueCounts.ValueCount get(int i) {
        return valueCounts.get(i);
    }

    public String getMostFrequentValue() {
        return mostFrequentValue;
    }

    public int getTotalFrequency() {
        return totalFrequency;
    }

    public int size() {
        return valueCounts.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

}
