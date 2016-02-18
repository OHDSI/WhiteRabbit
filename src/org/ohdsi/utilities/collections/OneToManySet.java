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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OneToManySet<K,V> {
  private Map<K,Set<V>> map = new HashMap<K, Set<V>>();
  
  public boolean put(K key, V value){
    Set<V> set = map.get(key);
    if (set == null){
      set = new HashSet<V>();
      map.put(key, set);
    }
    return set.add(value);
  }
  
  public void set(K key, Set<V> set){
  	map.put(key, set);
  }
  
  public Set<V> get(K key){
    Set<V> set = map.get(key);
    if (set == null)
      return Collections.emptySet();
    else
      return set;   
  }
  
  public Set<K> keySet(){
    return map.keySet();
  }
  
  public Collection<Set<V>> values(){
    return map.values();
  }
  
  public Set<Map.Entry<K, Set<V>>> entrySet(){
  	return map.entrySet();
  }
  
  public int size(){
  	return map.size();
  }
  

}

