package org.ohdsi.whiteRabbit.scan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Uniform sampling reservoir for streaming. This should capture the input distribution in order
 * to compute quartiles, using so-called Algorithm R.
 *
 * <p>The maximum size of the reservoir can be increased to get more accurate quartile estimations.
 * As long as the number of samples is lower than the maximum size of the reservoir, the quartiles
 * are computed exactly. Otherwise they are an estimate. The minimum, maximum and average are always
 * calculated exactly.
 */
public class UniformSamplingReservoir {
    private double[] samples;
    private int maxSize;
    private long count;
    private double sum;
    private double minimum;
    private double maximum;
    private transient int currentLength;

    public static void main(String[] args) {
        UniformSamplingReservoir us = new UniformSamplingReservoir(10);
        for (int i = 20; i > 0; i--) {
            us.add(i);
        }

        System.out.println(us.getSamples().toString());
        System.out.println(us.getCount());
        System.out.println(us.getQuartiles().toString());
        System.out.println(us.sum);
        System.out.println(us.getAverage());
        System.out.println(us.getMinimum());
        System.out.println(us.getMaximum());
    }

    /**
     * Create an empty reservoir.
     * @param maxSize maximum reservoir size.
     * @throws NullPointerException if given allValues are {@code null}
     */
    public UniformSamplingReservoir(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Reservoir maximum size must be strictly positive");
        }
        this.maxSize = maxSize;
        this.samples = new double[maxSize];
        this.count = 0;
        this.currentLength = 0;
    }

    /** Add a sample to the reservoir. */
    public void add(double value) {
        if (currentLength == maxSize) {
            long removeIndex = ThreadLocalRandom.current().nextLong(count);
            if (removeIndex < maxSize) {
                removeAndAdd((int)removeIndex, value);
            }
        } else {
            removeAndAdd(currentLength, value);
            currentLength++;
        }

        sum += value;
        minimum = Math.min(value, minimum);
        maximum = Math.max(value, maximum);
        count++;
    }

    private void removeAndAdd(int removeIndex, double value) {
        int addIndex = Arrays.binarySearch(samples, 0, currentLength, value);
        if (addIndex < 0) {
            addIndex = -addIndex - 1;
        }

        // start: [a, b, c, d, e]
        if (removeIndex < addIndex) {
            // removeIndex = 2, value = d2 -> addIndex = 4
            addIndex--;
            // new addIndex -> 3
            // copy(3 -> 2, len 1)
            // end: [a, b, d, d2, e]
            if (removeIndex < addIndex) {
                System.arraycopy(samples, removeIndex + 1, samples, removeIndex,
                        addIndex - removeIndex);
            }
        } else if (removeIndex > addIndex) {
            // removeIndex = 2, value = a2 -> addIndex = 1
            // copy(1 -> 2, len 1)
            // end: [a, a2, b, d, e]
            System.arraycopy(samples, addIndex, samples, addIndex + 1,
                    removeIndex - addIndex);
        }

        samples[addIndex] = value;
    }

    /**
     * Get the quartiles of the underlying distribution. If the number of samples is larger than
     * the maximum size of the reservoir, this will be an estimate.
     * @return list with size three, of the 25, 50 and 75 percentiles.
     */
    public List<Double> getQuartiles() {
        List<Double> quartiles = new ArrayList<>(3);

        switch (currentLength) {
            case 0:
                quartiles.add(Double.NaN);
                quartiles.add(Double.NaN);
                quartiles.add(Double.NaN);
                break;
            case 1:
                quartiles.add(samples[0]);
                quartiles.add(samples[0]);
                quartiles.add(samples[0]);
                break;
            default:
                for (int i = 1; i <= 3; i++) {
                    double pos = i * (currentLength + 1) * 0.25d; // 25 percentile steps
                    int intPos = (int) pos;
                    if (intPos == 0) {
                        quartiles.add(samples[0]);
                    } else if (intPos == currentLength) {
                        quartiles.add(samples[currentLength - 1]);
                    } else {
                        double diff = pos - intPos;
                        double base = samples[intPos - 1];
                        quartiles.add(base + diff * (samples[intPos] - base));
                    }
                }
                break;
        }

        return quartiles;
    }

    public double getAverage() {
        return sum/count;
    }

    public double getMinimum() {
        return minimum;
    }

    public double getMaximum() {
        return maximum;
    }

    /** Get the currently stored samples. */
    public List<Double> getSamples() {
        List<Double> doubleList = new ArrayList<>(currentLength);
        for (int i = 0; i < currentLength; i++) {
            doubleList.add(samples[i]);
        }
        return doubleList;
    }

    /** Get the maximum size of this reservoir. */
    public int getMaxSize() {
        return maxSize;
    }

    /** Get the number of samples that are being represented by the reservoir. */
    public long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UniformSamplingReservoir that = (UniformSamplingReservoir) o;
        return count == that.count
                && maxSize == that.maxSize
                && Arrays.equals(samples, that.samples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(samples, maxSize, count);
    }

    @Override
    public String toString() {
        return "UniformSamplingReservoir{"
                + "samples=" + Arrays.toString(samples)
                + ", maxSize=" + maxSize
                + ", count=" + count
                + '}';
    }
}