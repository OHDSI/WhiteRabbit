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
package org.ohdsi.rabbitInAHat.dataModel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Mapping <T extends MappableItem>{
	private List<T>	sourceItems;
	private List<T>	cdmItems;
	private List<ItemToItemMap>				sourceToCdmMaps;
	
	public Mapping(List<T> sourceItems, List<T> cdmItems, List<ItemToItemMap> sourceToCdmMaps) {
		this.sourceItems = sourceItems;
		this.cdmItems = cdmItems;
		this.sourceToCdmMaps = sourceToCdmMaps;
	}
	
	public void addSourceToCdmMap(MappableItem sourceItem, MappableItem cdmItem) {
		sourceToCdmMaps.add(new ItemToItemMap(sourceItem, cdmItem));
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
	public void setCdmItems(List<? extends MappableItem> cdmItems) {
		this.cdmItems.clear();
		for (MappableItem item : cdmItems)
			this.cdmItems.add((T) item);
	}
	
	public List<MappableItem> getCdmItems() {
		List<MappableItem> list = new ArrayList<MappableItem>();
		for (MappableItem item : cdmItems)
			list.add(item);
		return list;

	}
	
	public List<ItemToItemMap> getSourceToCdmMaps() {
		return sourceToCdmMaps;
	}
	
	public void removeSourceToCdmMap(MappableItem sourceItem, MappableItem cdmItem) {
		Iterator<ItemToItemMap> iterator = sourceToCdmMaps.iterator();
		while (iterator.hasNext()) {
			ItemToItemMap sourceToCdmMap = iterator.next();
			if (sourceToCdmMap.getSourceItem().equals(sourceItem) && sourceToCdmMap.getCdmItem().equals(cdmItem))
				iterator.remove();
		}
	}
	
	public ItemToItemMap getSourceToCdmMap(MappableItem sourceItem, MappableItem cdmItem) {
		Iterator<ItemToItemMap> iterator = sourceToCdmMaps.iterator();
		while (iterator.hasNext()) {
			ItemToItemMap sourceToCdmMap = iterator.next();
			if (sourceToCdmMap.getSourceItem().equals(sourceItem) && sourceToCdmMap.getCdmItem().equals(cdmItem))
				return sourceToCdmMap;
		}
		return null;
	}
}
