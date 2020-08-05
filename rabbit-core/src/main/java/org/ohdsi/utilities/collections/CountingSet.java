/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
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
package org.ohdsi.utilities.collections;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class for counting recurring objects.
 * 
 * @author schuemie
 * @param <T>
 */
public class CountingSet<T> extends AbstractSet<T> {
	
	public Map<T, Count>	key2count;
	
	public CountingSet() {
		key2count = new HashMap<>();
	}
	
	public CountingSet(int capacity) {
		key2count = new HashMap<>(capacity);
	}
	
	public CountingSet(CountingSet<T> set) {
		key2count = set.key2count.entrySet().stream()
				.collect(Collectors.toMap(
						Map.Entry::getKey,
						e -> new Count(e.getValue().count),
						(v1, v2) -> v1,
						HashMap::new));
	}

	public int getCount(T key) {
		Count count = key2count.get(key);
		if (count == null)
			return 0;
		else
			return count.count;
	}
	
	/**
	 * Computes the sum of the counts
	 * 
	 * @return
	 */
	public int getSum() {
		return key2count.values().parallelStream()
				.mapToInt(c -> c.count)
				.sum();
	}

	/**
	 * Returns the maximum count
	 * 
	 * @return
	 */
	public int getMax() {
		return key2count.values().parallelStream()
				.mapToInt(c -> c.count)
				.max()
				.orElse(0);
	}
	
	/**
	 * Computes the mean of the counts
	 * 
	 * @return
	 */
	public double getMean() {
		return (getSum() / (double) key2count.size());
	}
	
	/**
	 * Computes the standard deviations of the counts
	 * 
	 * @return
	 */
	public double getSD() {
		final double mean = getMean();
		double sqSum = key2count.values().parallelStream()
				.mapToDouble(c -> sqr(c.count - mean))
				.sum();
		return Math.sqrt(sqSum / key2count.size());
	}
	
	private static double sqr(double d) {
		return d * d;
	}
	
	public int size() {
		return key2count.size();
	}

	public boolean contains(Object arg0) {
		return key2count.containsKey(arg0);
	}
	
	public Iterator<T> iterator() {
		return key2count.keySet().iterator();
	}

	public boolean add(T arg0) {
		Count count = key2count.get(arg0);
		if (count == null) {
			key2count.put(arg0, new Count(1));
			return true;
		} else {
			count.increment();
			return false;
		}
	}
	
	public boolean add(T arg0, int inc) {
		Count count = key2count.get(arg0);
		if (count == null) {
			key2count.put(arg0, new Count(inc));
			return true;
		} else {
			count.add(inc);
			return false;
		}
	}
	
	public boolean remove(Object arg0) {
		return (key2count.remove(arg0) != null);
	}
	
	public void clear() {
		key2count.clear();
	}
	
	/**
	 * Keep the n most frequent values, remove the rest
	 * 
	 * @param n
	 */
	public void keepTopN(int n) {
		if (size() < n)
			return;

		key2count = decliningCountStream()
				.limit(n)
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, HashMap::new));
	}

	public static class Count implements Comparable<Count> {
		public int count;
		
		public Count(int count) {
			this.count = count;
		}

		public void increment() {
			count++;
		}

		public void add(int count) {
			this.count += count;
		}

		@Override
		public int compareTo(Count o) {
			return count - o.count;
		}
	}
	
	public void printCounts() {
		decliningCountStream()
			.forEach(entry -> System.out.println(entry.getKey() + "\t" + entry.getValue().count));
	}

	private Stream<Map.Entry<T, Count>> decliningCountStream() {
		return key2count.entrySet().stream()
				.sorted(Comparator.<Map.Entry<T, Count>, Count>comparing(Map.Entry::getValue).reversed());
	}
}
