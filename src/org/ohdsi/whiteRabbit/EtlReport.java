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
package org.ohdsi.whiteRabbit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.CountingSet;
import org.ohdsi.utilities.collections.CountingSet.Count;
import org.ohdsi.utilities.collections.IntegerComparator;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.files.MultiRowIterator.MultiRowSet;
import org.ohdsi.whiteRabbit.utilities.CodeToConceptMap;
import org.ohdsi.whiteRabbit.utilities.CodeToConceptMap.CodeData;

public class EtlReport {
	
	public static long				MAX_REPORT_PROBLEM		= 1000;
	
	private Map<String, Problem>	problems				= new HashMap<String, Problem>();
	private CountingSet<String>		incomingTableRowCounts	= new CountingSet<String>();
	private CountingSet<String>		outgoingTableRowCounts	= new CountingSet<String>();
	private String					folder;
	private long					totalProblemCount		= 0;
	
	public EtlReport(String folder) {
		this.folder = folder;
	}
	
	public void reportProblem(String table, String problemType, String personId) {
		totalProblemCount++;
		Problem problem = problems.get(table + "\t" + problemType);
		if (problem == null) {
			problem = new Problem();
			problem.problemType = problemType;
			problem.table = table;
			problems.put(table + "\t" + problemType, problem);
		}
		problem.count++;
		if (problem.count < MAX_REPORT_PROBLEM)
			problem.personId.add(personId);
		if (problem.count == MAX_REPORT_PROBLEM)
			System.out.println("Warning: encountered " + MAX_REPORT_PROBLEM + " problems of type '" + problemType + "' in table " + table);
	}
	
	public long getTotalProblemCount() {
		return totalProblemCount;
	}
	
	public String generateProblemList() {
		StringUtilities.outputWithTime("Generating ETL problem list");
		String filename = generateETLProblemListFilename(folder);
		XSSFWorkbook workbook = new XSSFWorkbook();
		
		XSSFSheet sheet = workbook.createSheet("Problems");
		addRow(sheet, "Table", "Problem", "Person_id");
		for (Problem problem : problems.values()) {
			for (int i = 0; i < Math.min(MAX_REPORT_PROBLEM, problem.count); i++)
				addRow(sheet, problem.table, problem.problemType, problem.personId.get(i));
			if (problem.count > MAX_REPORT_PROBLEM)
				addRow(sheet, problem.table, problem.problemType, "in " + (problem.count - MAX_REPORT_PROBLEM) + " other persons");
		}
		try {
			FileOutputStream out = new FileOutputStream(new File(filename));
			workbook.write(out);
			out.close();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
		return filename;
	}
	
	public String generate(CodeToConceptMap... codeToConceptMaps) {
		StringUtilities.outputWithTime("Generating ETL report");
		String filename = generateFilename(folder);
		XSSFWorkbook workbook = new XSSFWorkbook();
		
		XSSFSheet sheet = workbook.createSheet("Overview");
		addRow(sheet, "Source tables");
		addRow(sheet, "");
		addRow(sheet, "Table name", "Number of records");
		for (String table : incomingTableRowCounts)
			addRow(sheet, table, Integer.valueOf(incomingTableRowCounts.getCount(table)));
		addRow(sheet, "");
		
		addRow(sheet, "CDM tables");
		addRow(sheet, "");
		addRow(sheet, "Table name", "Number of records");
		for (String table : outgoingTableRowCounts)
			addRow(sheet, table, Integer.valueOf(outgoingTableRowCounts.getCount(table)));
		addRow(sheet, "");
		
		addRow(sheet, "Number of problems encountered", Long.valueOf(totalProblemCount));
		addRow(sheet, "");
		addRow(sheet, "Mapping", "Mapped unique codes", "Unmapped unique codes", "Mapped total codes", "Unmapped total codes");
		for (CodeToConceptMap codeToConceptMap : codeToConceptMaps) {
			int uniqueMapped = 0;
			int uniqueUnmapped = 0;
			long totalMapped = 0;
			long totalUnmapped = 0;
			CountingSet<String> codeCounts = codeToConceptMap.getCodeCounts();
			for (String code : codeCounts) {
				if (!codeToConceptMap.getConceptId(code).equals(0)) {
					uniqueMapped++;
					totalMapped += codeCounts.getCount(code);
				} else {
					uniqueUnmapped++;
					totalUnmapped += codeCounts.getCount(code);
				}
			}
			addRow(sheet, codeToConceptMap.getName(), Integer.valueOf(uniqueMapped), Integer.valueOf(uniqueUnmapped), Long.valueOf(totalMapped),
					Long.valueOf(totalUnmapped));
		}
		
		sheet = workbook.createSheet("Problems");
		addRow(sheet, "Table", "Description", "Nr of rows");
		for (Problem problem : problems.values())
			addRow(sheet, problem.table, problem.problemType, Long.valueOf(problem.count));
		
		for (CodeToConceptMap codeToConceptMap : codeToConceptMaps) {
			sheet = workbook.createSheet(codeToConceptMap.getName());
			addRow(sheet, "Frequency", "Source code", "Source code description", "Target concept ID", "Target code", "Target concept description");
			CountingSet<String> codeCounts = codeToConceptMap.getCodeCounts();
			List<Map.Entry<String, CountingSet.Count>> codes = new ArrayList<Map.Entry<String, CountingSet.Count>>(codeCounts.key2count.entrySet());
			reverseFrequencySort(codes);
			for (Map.Entry<String, CountingSet.Count> code : codes) {
				CodeData codeData = codeToConceptMap.getCodeData(code.getKey());
				if (codeData == null)
					addRow(sheet, Integer.valueOf(code.getValue().count), code.getKey(), "", Integer.valueOf(0), "", "");
				else
					for (int i = 0; i < codeData.targetConceptIds.length; i++)
						addRow(sheet, Integer.valueOf(code.getValue().count), code.getKey(), codeData.description,
								Integer.valueOf(codeData.targetConceptIds[i]), codeData.targetCodes[i], codeData.targetDescriptions[i]);
			}
		}
		
		try {
			FileOutputStream out = new FileOutputStream(new File(filename));
			workbook.write(out);
			out.close();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
		
		return filename;
	}
	
	private void reverseFrequencySort(List<Entry<String, Count>> codes) {
		Collections.sort(codes, new Comparator<Entry<String, Count>>() {
			
			@Override
			public int compare(Entry<String, Count> o1, Entry<String, Count> o2) {
				return -IntegerComparator.compare(o1.getValue().count, o2.getValue().count);
			}
		});
		
	}
	
	private void addRow(XSSFSheet sheet, Object... values) {
		Row row = sheet.createRow(sheet.getPhysicalNumberOfRows());
		for (Object value : values) {
			Cell cell = row.createCell(row.getPhysicalNumberOfCells());
			
			if (value instanceof Integer && value instanceof Long && value instanceof Double)
				cell.setCellValue(Double.valueOf(value.toString()));
			else
				cell.setCellValue(value.toString());
			
		}
	}
	
	private String generateFilename(String folder) {
		String filename = folder + "/ETLReport.xlsx";
		int i = 1;
		while (new File(filename).exists())
			filename = folder + "/ETLReport" + (i++) + ".xlsx";
		return filename;
	}
	
	private String generateETLProblemListFilename(String folder) {
		String filename = folder + "/ETLProblems.xlsx";
		int i = 1;
		while (new File(filename).exists())
			filename = folder + "/ETLPRoblems" + (i++) + ".xlsx";
		return filename;
	}
	
	public void registerIncomingData(String table, org.ohdsi.utilities.files.Row row) {
		incomingTableRowCounts.add(table, 1);
	}
	
	public void registerIncomingData(MultiRowSet data) {
		for (String table : data.keySet())
			incomingTableRowCounts.add(table, data.get(table).size());
	}
	
	public void registerOutgoingData(OneToManyList<String, org.ohdsi.utilities.files.Row> data) {
		for (String table : data.keySet())
			outgoingTableRowCounts.add(table, data.get(table).size());
	}
	
	private class Problem {
		public String		table;
		public String		problemType;
		public long			count		= 0;
		public List<String>	personId	= new ArrayList<String>();
	}
}
