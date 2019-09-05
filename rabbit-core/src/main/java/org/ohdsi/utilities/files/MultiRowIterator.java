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
package org.ohdsi.utilities.files;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.ohdsi.utilities.files.MultiRowIterator.MultiRowSet;

/**
 * Allows iteration over multiple tables (as Iterator<Row>) simultaneously, synchronized by the value of the [linkingColumn]. Assumes all tables are sorted by
 * the [linkingColumn].
 * 
 * @author MSCHUEMI
 */
public class MultiRowIterator implements Iterator<MultiRowSet> {
	
	private Iterator<Row>[]	iterators;
	private String[]		tableNames;
	private Row[]			nextRows;
	private MultiRowSet		buffer;
	private String			linkingColumn;
	private boolean			sortedNumerically;
	
	@SafeVarargs
	public MultiRowIterator(String linkingColumn, String[] tableNames, Iterator<Row>... tableIterators) {
		this(linkingColumn, false, tableNames, tableIterators);
	}
	
	public MultiRowIterator(String linkingColumn, boolean sortedNumerically, String[] tableNames, Iterator<Row>[] tableIterators) {
		this.iterators = tableIterators;
		this.linkingColumn = linkingColumn;
		this.tableNames = tableNames;
		this.sortedNumerically = sortedNumerically;
		startRead();
	}
	
	private void startRead() {
		nextRows = new Row[iterators.length];
		for (int i = 0; i < iterators.length; i++)
			if (iterators[i].hasNext())
				nextRows[i] = iterators[i].next();
			else
				nextRows[i] = null;
		readNext();
	}
	
	@Override
	public boolean hasNext() {
		return (buffer != null);
	}
	
	@Override
	public MultiRowSet next() {
		MultiRowSet result = buffer;
		readNext();
		return result;
	}
	
	private void readNext() {
		String lowestLinkingColumn = findLowestLinkingColumn(nextRows);
		if (lowestLinkingColumn == null) {
			buffer = null;
			return;
		}
		buffer = new MultiRowSet(tableNames);
		buffer.linkingId = lowestLinkingColumn;
		for (int i = 0; i < iterators.length; i++) {
			Iterator<Row> iterator = iterators[i];
			while (nextRows[i] != null && nextRows[i].get(linkingColumn).equals(lowestLinkingColumn)) {
				buffer.get(tableNames[i]).add(nextRows[i]);
				if (iterator.hasNext())
					nextRows[i] = iterator.next();
				else
					nextRows[i] = null;
			}
		}
	}
	
	private String findLowestLinkingColumn(Row[] rows) {
		String linkingId = null;
		for (Row row : rows)
			if (row != null && (linkingId == null || compare(row.get(linkingColumn), linkingId) < 0))
				linkingId = row.get(linkingColumn);
		return linkingId;
	}
	
	private int compare(String value1, String value2) {
		if (sortedNumerically)
			return efficientLongCompare(value1, value2);
		else
			return value1.compareTo(value2);
	}
	
	private int efficientLongCompare(String value1, String value2) {
		if (value1.length() > value2.length())
			return 1;
		else if (value1.length() < value2.length())
			return -1;
		else
			return value1.compareTo(value2);
	}
	
	@Override
	public void remove() {
		System.err.println("Calling unimplemented remove method in class " + this.getClass().getName());
	}
	
	public static class MultiRowSet extends HashMap<String, List<Row>> {
		private static final long	serialVersionUID	= 1164317535150664720L;
		
		public String				linkingId;
		
		public MultiRowSet(String[] tableNames) {
			for (String tableName : tableNames) {
				put(tableName, new ArrayList<Row>());
			}
		}
		
		public List<String> getNonEmptyTableNames() {
			List<String> result = new ArrayList<String>();
			for (String tableName : keySet())
				if (get(tableName).size() != 0)
					result.add(tableName);
			return result;
		}
		
		/**
		 * returns the total number of rows (summed across the tables)
		 * 
		 * @return
		 */
		public int totalSize() {
			int size = 0;
			for (List<Row> rows : values())
				size += rows.size();
			return size;
		}
		
	}
	
}
