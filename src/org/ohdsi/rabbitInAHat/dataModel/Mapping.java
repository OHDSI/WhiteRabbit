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
 ******************************************************************************/
package org.ohdsi.rabbitInAHat.dataModel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Mapping <T extends MappableItem>{
	private List<T>	sourceItems;
	private List<T>					targetItems;
	private List<ItemToItemMap>		sourceToTargetMaps;
	
	public Mapping(List<T> sourceItems, List<T> targetItems, List<ItemToItemMap> sourceToTargetMaps) {
		this.sourceItems = sourceItems;
		this.targetItems = targetItems;
		this.sourceToTargetMaps = sourceToTargetMaps;
	}
	
	public void addSourceToTargetMap(MappableItem sourceItem, MappableItem targetItem) {
		sourceToTargetMaps.add(new ItemToItemMap(sourceItem, targetItem));
	}
	
	public List<MappableItem> getSourceItems() {
		List<MappableItem> list = new ArrayList<MappableItem>();
		for (MappableItem item : sourceItems)
			list.add(item);
		return list;
	}
	
	@SuppressWarnings("unchecked")
	public void setSourceItems(List<MappableItem> sourceItems) {
		this.sourceItems.clear();
		for (MappableItem item : sourceItems)
			this.sourceItems.add((T)item);
	}
	
	@SuppressWarnings("unchecked")
	public void setTargetItems(List<? extends MappableItem> targetItems) {
		this.targetItems.clear();
		for (MappableItem item : targetItems)
			this.targetItems.add((T) item);
	}
	
	public List<MappableItem> getTargetItems() {
		List<MappableItem> list = new ArrayList<MappableItem>();
		for (MappableItem item : targetItems)
			list.add(item);
		return list;

	}
	
	public List<ItemToItemMap> getSourceToTargetMaps() {
		return sourceToTargetMaps;
	}
	
	public void removeSourceToTargetMap(MappableItem sourceItem, MappableItem targetItem) {
		Iterator<ItemToItemMap> iterator = sourceToTargetMaps.iterator();
		while (iterator.hasNext()) {
			ItemToItemMap sourceToTargetMap = iterator.next();
			if (sourceToTargetMap.getSourceItem().equals(sourceItem) && sourceToTargetMap.getTargetItem().equals(targetItem))
				iterator.remove();
		}
	}
	
	public ItemToItemMap getSourceToTargetMap(MappableItem sourceItem, MappableItem targetItem) {
		Iterator<ItemToItemMap> iterator = sourceToTargetMaps.iterator();
		while (iterator.hasNext()) {
			ItemToItemMap sourceToTargetMap = iterator.next();
			if (sourceToTargetMap.getSourceItem().equals(sourceItem) && sourceToTargetMap.getTargetItem().equals(targetItem))
				return sourceToTargetMap;
		}
		return null;
	}
}
