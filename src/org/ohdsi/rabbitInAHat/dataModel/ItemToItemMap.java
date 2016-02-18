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
package org.ohdsi.rabbitInAHat.dataModel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ItemToItemMap implements Serializable {
	
	private MappableItem		sourceItem;
	private MappableItem		cdmItem;
	private Map<String, String>	extraFieldToValue	= new HashMap<String, String>();
	private String				comment				= "";
	private String				logic				= "";
	private static final long	serialVersionUID	= -7803242002700513410L;
	private boolean				isCompleted			= false;
	
	public ItemToItemMap(MappableItem sourceItem, MappableItem cdmItem) {
		this.sourceItem = sourceItem;
		this.cdmItem = cdmItem;
	}
	
	public MappableItem getSourceItem() {
		return sourceItem;
	}
	
	public void setSourceItem(MappableItem sourceItem) {
		this.sourceItem = sourceItem;
	}
	
	public MappableItem getTargetItem() {
		return cdmItem;
	}
	
	public void setTargetItem(MappableItem cdmItem) {
		this.cdmItem = cdmItem;
	}
	
	public Map<String, String> getExtraFieldToValue() {
		return extraFieldToValue;
	}
	
	public boolean equals(Object other) {
		if (other instanceof ItemToItemMap) {
			return (((ItemToItemMap) other).sourceItem.getName().equals(sourceItem.getName()) && ((ItemToItemMap) other).cdmItem.getName().equals(cdmItem.getName()));
		} else
			return false;
	}
	
	public int hashCode() {
		return (sourceItem.toString() + "\t" + cdmItem.toString()).hashCode();
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
	
	public boolean isCompleted() {
		return isCompleted;
	}
	
	public void setCompleted(boolean isCompleted) {
		this.isCompleted = isCompleted;
	}
}
