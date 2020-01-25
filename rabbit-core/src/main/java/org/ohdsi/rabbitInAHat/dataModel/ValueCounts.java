package org.ohdsi.rabbitInAHat.dataModel;

import java.util.ArrayList;

/**
 * Created by Maxim Moinat.
 */
public class ValueCounts extends ArrayList<ValueCounts.ValueCount> {
    private int summedFrequency = 0;

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

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }
    }

    public boolean add(String value, int frequency) {
        summedFrequency += frequency;
        return super.add(new ValueCount(value, frequency));
    }

    public String getMostFrequentValue() {
        // Assumption: first added value is the most frequent one (that is how the scan report is structured)
        if (this.size() > 0) {
            return this.get(0).getValue();
        }
        return null;
    }

    public int getSummedFrequency() {
        return summedFrequency;
    }

}
