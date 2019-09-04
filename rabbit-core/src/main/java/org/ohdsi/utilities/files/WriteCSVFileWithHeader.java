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
