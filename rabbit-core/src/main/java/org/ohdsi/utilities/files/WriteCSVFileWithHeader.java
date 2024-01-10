/*******************************************************************************
 * Copyright 2023 Observational Health Data Sciences and Informatics
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

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public class WriteCSVFileWithHeader {

	private CSVPrinter	printer;
	private boolean		headerWritten	= false;

	public WriteCSVFileWithHeader(String fileName) {
		this(fileName, CSVFormat.RFC4180);
	}

	public WriteCSVFileWithHeader(String fileName, CSVFormat format) {
		try {
			printer = new CSVPrinter(new FileWriter(fileName), format);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public void write(Row row) {
		try {
			if (!headerWritten)
				writeHeader(row);
			printer.printRecord(row.getCells());
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}

	}

	private void writeHeader(Row row) throws IOException {
		headerWritten = true;
		Map<String, Integer> fieldName2ColumnIndex = row.getfieldName2ColumnIndex();
		int size = fieldName2ColumnIndex.size();
		List<String> header = new ArrayList<String>(size);
		for (int i = 0; i < size; i++)
			header.add(null);
		for (Map.Entry<String, Integer> entry : fieldName2ColumnIndex.entrySet())
			header.set(entry.getValue(), entry.getKey());
		printer.printRecord(header);
	}

	public void close() {
		try {
			printer.close();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
}
