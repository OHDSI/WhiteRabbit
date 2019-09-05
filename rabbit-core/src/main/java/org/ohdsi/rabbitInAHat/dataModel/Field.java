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

public class Field implements MappableItem {

	private static final long	serialVersionUID	= 3687778470032619497L;
	private Table				table;
	private String				name;
	private String				comment				= "";
	private String[][]			valueCounts;
	private boolean				isNullable;
	private String				type;
	private String				description			= "";
	private int					maxLength;
	private boolean				isStem;

	public Field(String name, Table table) {
		this.table = table;
		this.name = name;
	}

	public Database getDb() {
		return this.table.getDb();
	}

	public Table getTable() {
		return table;
	}

	public void setTable(Table table) {
		this.table = table;
	}

	public String getName() {
		return name;
	}

	public String outputName() {
		if (!isNullable) {
			return "*" + name;
		} else {
			return name;
		}
	}

	public void setName(String name) {
		this.name = name;
	}

	public String[][] getValueCounts() {
		return valueCounts;
	}

	public void setValueCounts(String[][] valueCounts) {
		this.valueCounts = valueCounts;
	}

	public int getRowsCheckedCount() {
		return this.table.getRowsCheckedCount();
	}

	public boolean isNullable() {
		return isNullable;
	}

	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String toString() {
		return table.getName() + "." + name;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public String getComment() {
		return comment;
	}

	public int getMaxLength() {
		return maxLength;
	}

	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}

	public boolean isStem() {
		return isStem;
	}

	public void setStem(boolean isStem) {
		this.isStem = isStem;
	}
}
