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
package org.ohdsi.utilities.files;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class WriteCSVFileWithHeader {
	
	private WriteCSVFile	out;
	private boolean			headerWritten;
	private boolean			threadSafe	= false;
	private ReentrantLock	lock;
	
	public WriteCSVFileWithHeader(String filename) {
		out = new WriteCSVFile(filename);
		headerWritten = false;
	}
	
	public WriteCSVFileWithHeader(String filename, boolean append) {
		out = new WriteCSVFile(filename, append);
		headerWritten = true;
	}
	
	public void write(Row row) {
		if (threadSafe)
			lock.lock();
		if (!headerWritten)
			writeHeader(row);
		out.write(row.getCells());
		if (threadSafe)
			lock.unlock();
	}
	
	public void close() {
		out.close();
	}
	
	public void flush() {
		if (threadSafe)
			lock.lock();
		out.flush();
		if (threadSafe)
			lock.unlock();
	}
	
	public void setThreadSafe(boolean value) {
		threadSafe = value;
		if (threadSafe)
			lock = new ReentrantLock();
	}
	
	public boolean getThreadSafe() {
		return threadSafe;
	}
	
	private void writeHeader(Row row) {
		headerWritten = true;
		Map<String, Integer> fieldName2ColumnIndex = row.getfieldName2ColumnIndex();
		int size = fieldName2ColumnIndex.size();
		List<String> header = new ArrayList<String>(size);
		for (int i = 0; i < size; i++)
			header.add(null);
		for (Map.Entry<String, Integer> entry : fieldName2ColumnIndex.entrySet())
			header.set(entry.getValue(), entry.getKey());
		out.write(header);
	}
}
