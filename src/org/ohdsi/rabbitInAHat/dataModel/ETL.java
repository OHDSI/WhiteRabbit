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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ETL implements Serializable {
	private Database								sourceDb					= new Database();
	private Database								cdmDb						= new Database();
	private List<ItemToItemMap>						tableToTableMaps			= new ArrayList<ItemToItemMap>();
	private Map<ItemToItemMap, List<ItemToItemMap>>	tableMapToFieldToFieldMaps	= new HashMap<ItemToItemMap, List<ItemToItemMap>>();
	private static final long						serialVersionUID			= 8987388381751618498L;
	
	public void saveCurrentState() {
		
	}
	
	public void revertToPreviousState() {
		
	}
	
	public void revertToNextState() {
		
	}
	
	public Mapping<Table> getTableToTableMapping() {
		return new Mapping<Table>(sourceDb.getTables(), cdmDb.getTables(), tableToTableMaps);
	}
	
	public Mapping<Field> getFieldToFieldMapping(Table sourceTable, Table cdmTable) {
		List<ItemToItemMap> fieldToFieldMaps = tableMapToFieldToFieldMaps.get(new ItemToItemMap(sourceTable, cdmTable));
		if (fieldToFieldMaps == null) {
			fieldToFieldMaps = new ArrayList<ItemToItemMap>();
			tableMapToFieldToFieldMaps.put(new ItemToItemMap(sourceTable, cdmTable), fieldToFieldMaps);
		}
		return new Mapping<Field>(sourceTable.getFields(), cdmTable.getFields(), fieldToFieldMaps);
	}
	
	public void setCDMDatabase(Database cdmDb) {
		this.cdmDb = cdmDb;
	}
	
	public void setSourceDatabase(Database sourceDb) {
		this.sourceDb = sourceDb;
	}
	
	public Database getCDMDatabase() {
		return cdmDb;
	}
	
	public Database getSourceDatabase() {
		return sourceDb;
	}
	
	public ItemToItemMap createItemToItemMap(MappableItem source, MappableItem target) {
		ItemToItemMap itemToItemMap = new ItemToItemMap(source, target);
		if (source instanceof Table) {
			tableToTableMaps.add(itemToItemMap);
		} else {
			tableMapToFieldToFieldMaps.get(((Field) source).getTable()).add(itemToItemMap);
		}
		return itemToItemMap;
	}
}
