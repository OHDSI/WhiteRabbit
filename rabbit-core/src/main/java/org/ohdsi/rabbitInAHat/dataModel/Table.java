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
package org.ohdsi.rabbitInAHat.dataModel;

import java.util.ArrayList;
import java.util.List;

public class Table implements MappableItem {

	private Database			db;
	private String				name;
	private int					rowCount;
	private int					rowsCheckedCount;
	private String				comment				= "";
	private List<Field>			fields				= new ArrayList<Field>();
	private boolean				isStem				= false;
	private boolean				isFilter			= false;
	private static final long	serialVersionUID	= 8866500385429215492L;

	public Table() {
		super();
	}
	
	public Table(Table table) {
		super();
		db = table.db;
		name = table.name;
		rowCount = table.rowCount;
		comment = table.comment;
		fields = new ArrayList<Field>(table.fields);
		isStem = table.isStem;
	}
	
	public Database getDb() {
		return db;
	}

	public void setDb(Database db) {
		this.db = db;
	}

	public String getName() {
		return name;
	}

	public String outputName() {
		return getName();
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	public int getRowsCheckedCount() {
		return rowsCheckedCount;
	}

	public void setRowsCheckedCount(int rowsCheckedCount) {
		this.rowsCheckedCount = rowsCheckedCount;
	}

	public List<Field> getFields() {
		return fields;
	}
	
	public Field getFieldByName(String name) {
		for (Field field : fields)
			if (field.getName().toLowerCase().equals(name.toLowerCase()))
				return field;
		return null;		
	}

	public String toString() {
		return name;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getComment() {
		return comment;
	}

	public boolean isStem() {
		return isStem;
	}

	public void setStem(boolean isStem) {
		this.isStem = isStem;
	}

	public boolean isFilter() { return isFilter; }

	public static String indexTableNameForSheet(String tableName, int index) {
		String name = tableName;
		// Prepend index for long table names (to make sheet name unique later)
		if (name.length() > 31) {
			name = Integer.toString(index) + '_' + name;
		}
		return name;
	}

	public static String createSheetNameFromTableName(String tableName) {
		String name = tableName;

		// Excel sheet names have a maximum of 31 characters
		if (name.length() > 31) {
			name = name.substring(0, 31);
		}

		// Backslash causes issues in excel
		name = name.replace('/','_');
		return name;
	}
}
