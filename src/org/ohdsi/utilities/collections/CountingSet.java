/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
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
 * 
 * @author Observational Health Data Sciences and Informatics
 * @author Martijn Schuemie
 ******************************************************************************/
package org.ohdsi.utilities.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Class for counting recurring objects.
 * 
 * @author schuemie
 * @param <T>
 */
public class CountingSet<T> implements Set<T> {
	
	public Map<T, Count>	key2count;
	
	public CountingSet() {
		key2count = new HashMap<T, Count>();
	}
	
	public CountingSet(int capacity) {
		key2count = new HashMap<T, Count>(capacity);
	}
	
	public CountingSet(CountingSet<T> set) {
		key2count = new HashMap<T, Count>(set.key2count.size());
		for (Map.Entry<T, Count> entry : set.key2count.entrySet())
			key2count.put(entry.getKey(), new Count(entry.getValue().count));
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
		int sum = 0;
		for (Count count : key2count.values())
			sum += count.count;
		return sum;
	}
	
	/**
	 * Returns the maximum count
	 * 
	 * @return
	 */
	public int getMax() {
		int max = 0;
		for (Count count : key2count.values())
			if (count.count > max)
				max = count.count;
		return max;
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
		double mean = getMean();
		double sum = 0;
		for (Count count : key2count.values())
			sum += sqr(count.count - mean);
		return Math.sqrt(sum / (double) key2count.size());
	}
	
	private double sqr(double d) {
		return d * d;
	}
	
	public int size() {
		return key2count.size();
	}
	
	public boolean isEmpty() {
		return key2count.isEmpty();
	}
	
	public boolean contains(Object arg0) {
		return key2count.containsKey(arg0);
	}
	
	public Iterator<T> iterator() {
		return key2count.keySet().iterator();
	}
	
	public Object[] toArray() {
		return key2count.keySet().toArray();
	}
	
	@SuppressWarnings("unchecked")
	public Object[] toArray(Object[] arg0) {
		return key2count.keySet().toArray(arg0);
	}
	
	public boolean add(T arg0) {
		Count count = key2count.get(arg0);
		if (count == null) {
			count = new Count();
			key2count.put(arg0, count);
			return true;
		} else {
			count.count++;
			return false;
		}
	}
	
	public boolean add(T arg0, int inc) {
		Count count = key2count.get(arg0);
		if (count == null) {
			count = new Count();
			count.count = inc;
			key2count.put(arg0, count);
			return true;
		} else {
			count.count += inc;
			return false;
		}
	}
	
	public boolean remove(Object arg0) {
		
		return (key2count.remove(arg0) != null);
	}
	
	public boolean containsAll(Collection<?> arg0) {
		return key2count.keySet().containsAll(arg0);
	}
	
	public boolean addAll(Collection<? extends T> arg0) {
		boolean changed = false;
		for (T object : arg0) {
			if (add(object))
				changed = true;
		}
		return changed;
	}
	
	public boolean retainAll(Collection<?> arg0) {
		return key2count.keySet().retainAll(arg0);
	}
	
	public boolean removeAll(Collection<?> arg0) {
		return key2count.keySet().removeAll(arg0);
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
		List<Map.Entry<T, Count>> list = new ArrayList<Map.Entry<T, Count>>(key2count.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<T, Count>>() {
			
			@Override
			public int compare(Entry<T, Count> arg0, Entry<T, Count> arg1) {
				return IntegerComparator.compare(arg1.getValue().count, arg0.getValue().count);
			}
		});
		
		Map<T, Count> newMap = new HashMap<T, CountingSet.Count>(n);
		for (int i = 0; i < n; i++)
			newMap.put(list.get(i).getKey(), list.get(i).getValue());
		
		key2count = newMap;
	}
	
	public static class Count {
		public int	count	= 1;
		
		public Count() {
		}
		
		public Count(int count) {
			this.count = count;
		}
		
	}
	
	public void printCounts() {
		List<Map.Entry<T, Count>> result = new ArrayList<Map.Entry<T, Count>>(key2count.entrySet());
		Collections.sort(result, new Comparator<Map.Entry<T, Count>>() {
			public int compare(Entry<T, Count> o1, Entry<T, Count> o2) {
				return IntegerComparator.compare(o2.getValue().count, o1.getValue().count);
			}
		});
		for (Map.Entry<T, Count> entry : result)
			System.out.println(entry.getKey() + "\t" + entry.getValue().count);
	}
}
