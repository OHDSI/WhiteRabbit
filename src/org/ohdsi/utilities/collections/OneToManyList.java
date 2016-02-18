/*******************************************************************************
 * Copyright 2016 Observational Health Data Sciences and Informatics
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OneToManyList<K, V> {
	private Map<K, List<V>>	map	= new HashMap<K, List<V>>();
	
	public void put(K key, V value) {
		List<V> list = map.get(key);
		if (list == null) {
			list = new ArrayList<V>();
			map.put(key, list);
		}
		list.add(value);
	}
	
	public void putAll(OneToManyList<K, V> other) {
		for (Map.Entry<K, List<V>> entry : other.map.entrySet())
			for (V value : entry.getValue())
				put(entry.getKey(), value);
	}
	
	public void putAll(K key, Collection<V> values) {
		for (V value : values)
			put(key, value);
	}
	
	public List<V> get(K key) {
		List<V> list = map.get(key);
		if (list == null)
			return Collections.emptyList();
		else
			return list;
	}
	
	public Set<K> keySet() {
		return map.keySet();
	}
	
	public Collection<List<V>> values() {
		return map.values();
	}
	
	public List<V> remove(K key) {
		return map.remove(key);
	}
	
	public void clear() {
		map.clear();
	}
	
	public int size() {
		int size = 0;
		for (List<V> value : map.values())
			size += value.size();
		return size;
	}
	
}
