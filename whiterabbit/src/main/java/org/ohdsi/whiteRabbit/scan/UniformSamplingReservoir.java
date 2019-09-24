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
 * are computed exactly.
 * TODO: frequencies
 * TODO: sum and mean
 * TODO: all data types
 */
public class UniformSamplingReservoir<T> {
    private Object[] samples;
    private int maxSize;
    private long count;
    private transient int currentLength;
    private static final int MAX_SIZE_DEFAULT = 999;

    public static void main(String[] args) {
        UniformSamplingReservoir us = new UniformSamplingReservoir(10);
        for (int i = 0; i < 15; i++) {
            us.add(i);
        }

        System.out.println(us.getSamples().toString());
        System.out.println(us.getCount());
        System.out.println(us.getQuartiles().toString());
    }

    /** Empty reservoir with default maximum size. */
    public UniformSamplingReservoir() {
        this(new Object[] {}, 0, MAX_SIZE_DEFAULT);
    }

    /** Empty reservoir with given maximum size. */
    public UniformSamplingReservoir(int maxSize) {
        this(new Object[] {}, 0, maxSize);
    }

    /**
     * Create a reservoir that samples from given values.
     * @param allValues list of values to sample from.
     * @throws NullPointerException if given allValues are {@code null}.
     */
    public UniformSamplingReservoir(T... allValues) {
        this(allValues, allValues.length, MAX_SIZE_DEFAULT);
    }

    /**
     * Create a reservoir that samples from given values.
     * @param samples list of values to sample from.
     * @param count current size of the number of samples that the reservoir represents.
     * @param maxSize maximum reservoir size.
     * @throws NullPointerException if given allValues are {@code null}
     */
    public UniformSamplingReservoir(Object[] samples, long count, int maxSize) {
        initializeReservoir(samples, count, maxSize);
    }

    /** Sample from given list of samples to initialize the reservoir. */
    private void initializeReservoir(Object[] initSamples, long initCount, int initMaxSize) {
        if (initSamples == null) {
            throw new IllegalArgumentException("Samples may not be null");
        }
        if (initMaxSize <= 0) {
            throw new IllegalArgumentException("Reservoir maximum size must be strictly positive");
        }
        if (initCount < initSamples.length) {
            throw new IllegalArgumentException(
                    "Reservoir count must be larger or equal than number of samples.");
        }
        this.maxSize = initMaxSize;
        this.samples = new Object[initMaxSize];
        this.count = initCount;

        int length = (int)Math.min(initSamples.length, count);

        if (length == 0) {
            currentLength = 0;
            return;
        }

        subsample(initSamples, length);
        Arrays.sort(this.samples, 0, currentLength);
    }

    private void subsample(Object[] initSamples, int length) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // There are much more samples than the size permits. Random sample from the
        // given list. Duplicate addresses are retried, and for each hit there is at least a
        // 50% probability of getting a number that has not yet been picked.
        if (length > maxSize * 2) {
            Set<Integer> indexSet = new HashSet<>();
            int sampleIndex = 0;
            while (sampleIndex < maxSize) {
                int initIndex = random.nextInt(length);
                // only add to samples if index set does not yet contain value
                if (indexSet.add(initIndex)) {
                    this.samples[sampleIndex] = initSamples[initIndex];
                    sampleIndex++;
                }
            }
            currentLength = maxSize;

            // There are not much more samples than the size permits. Make a list from all indexes
            // and at random pick and remove index from that. Put all the samples at given
            // indexes in the reservoir. Do not do retry sampling as above, as the final entry may
            // have a probability of 1/maxSize of actually being picked.
        } else if (length > maxSize) {
            LinkedList<Integer> allInitIndexes = new LinkedList<>();
            for (int i = 0; i < length; i++) {
                allInitIndexes.add(i);
            }
            for (int sampleIndex = 0; sampleIndex < maxSize; sampleIndex++) {
                int initIndex = allInitIndexes.remove(random.nextInt(allInitIndexes.size()));
                this.samples[sampleIndex] = initSamples[initIndex];
            }
            currentLength = maxSize;
        } else {
            System.arraycopy(initSamples, 0, this.samples, 0, length);
            currentLength = length;
        }
    }

    /** Add a sample to the reservoir. */
    public void add(T value) {
        if (currentLength == maxSize) {
            long removeIndex = ThreadLocalRandom.current().nextLong(count);
            if (removeIndex < maxSize) {
                removeAndAdd((int)removeIndex, value);
            }
        } else {
            removeAndAdd(currentLength, value);
            currentLength++;
        }

        count++;
    }

    private void removeAndAdd(int removeIndex, T value) {
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
     * TODO: only return if samples of numeric type
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
                quartiles.add((Double) samples[0]);
                quartiles.add((Double) samples[0]);
                quartiles.add((Double) samples[0]);
                break;
            default:
                for (int i = 1; i <= 3; i++) {
                    double pos = i * (currentLength + 1) * 0.25d; // 25 percentile steps
                    int intPos = (int) pos;
                    if (intPos == 0) {
                        quartiles.add((Double) samples[0]);
                    } else if (intPos == currentLength) {
                        quartiles.add((Double) samples[currentLength - 1]);
                    } else {
                        double diff = pos - intPos;
                        double base = (Double) samples[intPos - 1];
                        quartiles.add(base + diff * ((Double) samples[intPos] - base));
                    }
                }
                break;
        }

        return quartiles;
    }

    /** Get the currently stored samples. */
    public List<T> getSamples() {
        List<T> sampleList = new ArrayList<>(currentLength);
        for (int i = 0; i < currentLength; i++) {
            sampleList.add((T) samples[i]);
        }
        return sampleList;
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
