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

import org.ohdsi.utilities.DateUtilities;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.CountingSet;
import org.ohdsi.utilities.collections.Pair;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FieldInfo {
    private final ScanParameters scanParameters;
    public String type;
    public String name;
    public String label;
    public CountingSet<String> valueCounts = new CountingSet<>();
    public long sumLength = 0;
    public int maxLength = 0;
    public long nProcessed = 0;
    public long emptyCount = 0;
    public long uniqueCount = 0;
    public long rowCount = -1;
    public boolean isInteger = true;
    public boolean isReal = true;
    public boolean isDate = true;
    public boolean isFreeText = false;
    public boolean tooManyValues = false;
    public UniformSamplingReservoir samplingReservoir;
    public Object average;
    public Object stdev;
    public Object minimum;
    public Object maximum;
    public Object q1;
    public Object q2;
    public Object q3;

    public FieldInfo(ScanParameters scanParameters, String name) {
        this.scanParameters = scanParameters;
        this.name = name;
        if (scanParameters.doCalculateNumericStats()) {
            this.samplingReservoir = new UniformSamplingReservoir(scanParameters.getNumStatsSamplerSize());
        }
    }

    public void trim() {
        // Only keep values that are used in scan report
        if (valueCounts.size() > scanParameters.getMaxValues()) {
            valueCounts.keepTopN(scanParameters.getMaxValues());
        }

        // Calculate numeric stats and dereference sampling reservoir to save memory.
        if (scanParameters.doCalculateNumericStats()) {
            average = getAverage();
            stdev = getStandardDeviation();
            minimum = getMinimum();
            maximum = getMaximum();
            q1 = getQ1();
            q2 = getQ2();
            q3 = getQ3();
        }
        samplingReservoir = null;
    }

    public boolean hasValuesTrimmed() {
        return tooManyValues;
    }

    public Double getFractionEmpty() {
        if (nProcessed == 0)
            return 1d;
        else
            return emptyCount / (double) nProcessed;
    }

    public String getTypeDescription() {
        if (type != null)
            return type;
        else if (!scanParameters.doScanValues()) // If not type assigned and not values scanned, do not derive
            return "";
        else if (nProcessed == emptyCount)
            return DataType.EMPTY.name();
        else if (isFreeText)
            return DataType.TEXT.name();
        else if (isDate)
            return DataType.DATE.name();
        else if (isInteger)
            return DataType.INT.name();
        else if (isReal)
            return DataType.REAL.name();
        else
            return DataType.VARCHAR.name();
    }

    public Double getFractionUnique() {
        if (nProcessed == 0 || uniqueCount == 1) {
            return 0d;
        } else {
            return uniqueCount / (double) nProcessed;
        }

    }

    public void processValue(String value) {
        nProcessed++;
        sumLength += value.length();
        if (value.length() > maxLength)
            maxLength = value.length();

        String trimValue = value.trim();
        if (trimValue.length() == 0)
            emptyCount++;

        if (!isFreeText) {
            boolean newlyAdded = valueCounts.add(value);
            if (newlyAdded) uniqueCount++;

            if (trimValue.length() != 0) {
                evaluateDataType(trimValue);
            }

            if (nProcessed == ScanParameters.N_FOR_FREE_TEXT_CHECK && !isInteger && !isReal && !isDate) {
                doFreeTextCheck();
            }
        } else {
            valueCounts.addAll(StringUtilities.mapToWords(trimValue.toLowerCase()));
        }

        // if over this large constant number, then trimmed back to size used in report (maxValues).
        if (!tooManyValues && valueCounts.size() > ScanParameters.MAX_VALUES_IN_MEMORY) {
            tooManyValues = true;
            this.trim();
        }

        if (scanParameters.doCalculateNumericStats() && !trimValue.isEmpty()) {
            if (isInteger || isReal) {
                samplingReservoir.add(Double.parseDouble(trimValue));
            } else if (isDate) {
                samplingReservoir.add(DateUtilities.parseDate(trimValue));
            }
        }

    }

    public List<Pair<String, Integer>> getSortedValuesWithoutSmallValues() {
        List<Pair<String, Integer>> result = valueCounts.key2count.entrySet().stream()
                .filter(e -> e.getValue().count >= scanParameters.getMinCellCount())
                .sorted(Comparator.<Map.Entry<String, CountingSet.Count>>comparingInt(e -> e.getValue().count).reversed())
                .limit(scanParameters.getMaxValues())
                .map(e -> new Pair<>(e.getKey(), e.getValue().count))
                .collect(Collectors.toCollection(ArrayList::new));

        if (result.size() < valueCounts.key2count.size()) {
            result.add(new Pair<>("List truncated...", -1));
        }
        return result;
    }

    private void evaluateDataType(String value) {
        if (isReal && !StringUtilities.isNumber(value))
            isReal = false;
        if (isInteger && !StringUtilities.isLong(value))
            isInteger = false;
        if (isDate && !StringUtilities.isDate(value))
            isDate = false;
    }

    private void doFreeTextCheck() {
        double averageLength = sumLength / (double) (nProcessed - emptyCount);
        if (averageLength >= ScanParameters.MIN_AVERAGE_LENGTH_FOR_FREE_TEXT) {
            isFreeText = true;
            // Reset value count to word count
            CountingSet<String> wordCounts = new CountingSet<>();
            for (Map.Entry<String, CountingSet.Count> entry : valueCounts.key2count.entrySet())
                for (String word : StringUtilities.mapToWords(entry.getKey().toLowerCase()))
                    wordCounts.add(word, entry.getValue().count);
            valueCounts = wordCounts;
        }
    }

    private Object formatNumericValue(double value) {
        return formatNumericValue(value, false);
    }

    private Object formatNumericValue(double value, boolean dateAsDays) {
        if (nProcessed == 0) {
            return Double.NaN;
        } else if (getTypeDescription().equals(DataType.EMPTY.name())) {
            return Double.NaN;
        } else if (isInteger || isReal) {
            return value;
        } else if (isDate && dateAsDays) {
            return value;
        } else if (isDate) {
            return LocalDate.ofEpochDay((long) value).toString();
        } else {
            return Double.NaN;
        }
    }

    private Object getMinimum() {
        double min = samplingReservoir.getPopulationMinimum();
        return formatNumericValue(min);
    }

    private Object getMaximum() {
        double max = samplingReservoir.getPopulationMaximum();
        return formatNumericValue(max);
    }

    private Object getAverage() {
        double average = samplingReservoir.getPopulationMean();
        return formatNumericValue(average);
    }

    private Object getStandardDeviation() {
        double stddev = samplingReservoir.getSampleStandardDeviation();
        return formatNumericValue(stddev, true);
    }

    private Object getQ1() {
        double q1 = samplingReservoir.getSampleQuartiles().get(0);
        return formatNumericValue(q1);
    }

    private Object getQ2() {
        double q2 = samplingReservoir.getSampleQuartiles().get(1);
        return formatNumericValue(q2);
    }

    private Object getQ3() {
        double q3 = samplingReservoir.getSampleQuartiles().get(2);
        return formatNumericValue(q3);
    }
}
