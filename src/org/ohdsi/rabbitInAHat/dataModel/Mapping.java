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
	private List<T>						sourceItems;
	private List<T>						cdmItems;
	private List<ItemToItemMap>			sourceToCdmMaps;
	private List<List<ItemToItemMap>> 	sourceToCdmMapsOfMaps;
	
	public Mapping(List<T> sourceItems, List<T> targetItems, List<ItemToItemMap> sourceToTargetMaps) {
		this.sourceItems = sourceItems;
		this.cdmItems = targetItems;
		this.sourceToCdmMaps = sourceToTargetMaps;
		//this.sourceToCdmMapsOfMaps = sourceToCdmMapsOfMaps;
	}
	
	public void addSourceToTargetMap(MappableItem sourceItem, MappableItem targetItem) {
		sourceToCdmMaps.add(new ItemToItemMap(sourceItem, targetItem));
	}
	
	public void addSourceToTargetMap(ItemToItemMap itemToItemMap) {
		sourceToCdmMaps.add(itemToItemMap);
	}
	
	public void addSourceToTargetMapList(List<ItemToItemMap> maps){
		sourceToCdmMapsOfMaps.add(maps);
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
		this.cdmItems.clear();
		for (MappableItem item : targetItems)
			this.cdmItems.add((T) item);
	}
	
	public List<MappableItem> getTargetItems() {
		List<MappableItem> list = new ArrayList<MappableItem>();
		for (MappableItem item : cdmItems)
			list.add(item);
		return list;

	}
	
	
	public List<ItemToItemMap> getSourceToTargetMaps() {
		return sourceToCdmMaps;
	}
	
	public List<List<ItemToItemMap>> getSourceToTargetMapsOfMaps(){
		return sourceToCdmMapsOfMaps;
	}
	
	public MappableItem getTargetItemFromMapsOfMaps(){
		return null;
	}
	
	public void removeSourceToTargetMap(MappableItem sourceItem, MappableItem targetItem) {
		Iterator<ItemToItemMap> iterator = sourceToCdmMaps.iterator();
		while (iterator.hasNext()) {
			ItemToItemMap sourceToTargetMap = iterator.next();
			if (sourceToTargetMap.getSourceItem().equals(sourceItem) && sourceToTargetMap.getTargetItem().equals(targetItem))
				iterator.remove();
		}
	}
	
	public ItemToItemMap getSourceToTargetMap(MappableItem sourceItem, MappableItem targetItem) {
		Iterator<ItemToItemMap> iterator = sourceToCdmMaps.iterator();
		while (iterator.hasNext()) {
			ItemToItemMap sourceToTargetMap = iterator.next();
			if (sourceToTargetMap.getSourceItem().equals(sourceItem) && sourceToTargetMap.getTargetItem().equals(targetItem))
				return sourceToTargetMap;
		}
		return null;
	}
	
	public ItemToItemMap getSourceToTargetMapByName(MappableItem sourceItem, MappableItem targetItem) {
		Iterator<ItemToItemMap> iterator = sourceToCdmMaps.iterator();

		while (iterator.hasNext()) {
			ItemToItemMap sourceToTargetMap = iterator.next();
			if (sourceToTargetMap.getSourceItem().getName().equals(sourceItem.getName()) && sourceToTargetMap.getTargetItem().getName().equals(targetItem.getName()))
				return sourceToTargetMap;
		}
		return null;
	}
}
