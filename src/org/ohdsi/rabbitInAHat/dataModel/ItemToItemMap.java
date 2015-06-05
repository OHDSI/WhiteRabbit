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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ItemToItemMap implements Serializable {
	
	private MappableItem		sourceItem;
	private MappableItem		targetItem;
	private Map<String, String>	extraFieldToValue	= new HashMap<String, String>();
	private String				comment				= "";
	private String				logic				= "";
	private static final long	serialVersionUID	= -7803242002700513410L;
	
	public ItemToItemMap(MappableItem sourceItem, MappableItem targetItem) {
		this.sourceItem = sourceItem;
		this.targetItem = targetItem;
	}
	
	public MappableItem getSourceItem() {
		return sourceItem;
	}
	
	public void setSourceItem(MappableItem sourceItem) {
		this.sourceItem = sourceItem;
	}
	
	public MappableItem getTargetItem() {
		return targetItem;
	}
	
	public void setTargetItem(MappableItem targetItem) {
		this.targetItem = targetItem;
	}
	
	public Map<String, String> getExtraFieldToValue() {
		return extraFieldToValue;
	}
	
	public boolean equals(Object other) {
		if (other instanceof ItemToItemMap) {
			return (((ItemToItemMap) other).sourceItem.equals(sourceItem) && ((ItemToItemMap) other).targetItem.equals(targetItem));
		} else
			return false;
	}
	
	public int hashCode() {
		return (sourceItem.toString() + "\t" + targetItem.toString()).hashCode();
	}
	
	public String getComment() {
		return comment;
	}
	
	public void setComment(String comment) {
		this.comment = comment;
	}
	
	public String getLogic() {
		return logic;
	}
	
	public void setLogic(String logic) {
		this.logic = logic;
	}
}
