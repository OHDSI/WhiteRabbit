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
package org.ohdsi.utilities.files;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Class for sorting (large) comma separated value text files. Assumes first row contains header
 * 
 * @author schuemie
 */
public class FileSorter {
	
	/**
	 * Chunksize is the number of rows sorted in memory in one time. Higher chunk size leads to better performance but requires more memory. Set chunksize to -1
	 * to let the system determine the largest chunk size based on available memory.
	 */
	public static int		chunckSize					= -1;
	public static boolean	verbose						= false;
	public static boolean	caseSensitiveColumnNames	= false;
	public static int		maxNumberOfTempFiles		= 500;
	public static double	minFreeMemFraction			= 0.25;
	public static boolean	checkIfAlreadySorted		= false;
	public static int		maxCheckRows				= 10000000; // Check only first 10000000 rows to see if already sorted
																	
	public static void sort(String filename, String... columnNames) {
		boolean[] sortNumeric = new boolean[columnNames.length];
		Arrays.fill(sortNumeric, false);
		sort(filename, columnNames, sortNumeric);
	}
	
	public static void sort(String filename, String[] columnnames, boolean[] sortNumeric) {
		if (columnnames.length != sortNumeric.length)
			throw new RuntimeException("Numeric sort array lenght does not equal sort column names");
		
		System.gc();
		long availableMem = getFreeMem();
		long minFreeMem = Math.round(availableMem * minFreeMemFraction);
		
		if (verbose) {
			System.out.println("Starting sort of " + filename);
			if (chunckSize == -1)
				System.out.println("Memory available for sorting: " + availableMem + " bytes. Min free = " + minFreeMem);
		}
		
		Iterator<List<String>> iterator = new ReadCSVFile(filename).iterator();
		
		List<String> header = iterator.next();
		
		Comparator<List<String>> comparator = buildComparator(columnnames, sortNumeric, header);
		
		if (checkIfAlreadySorted) {
			if (isSorted(iterator, comparator))
				return;
			else {
				iterator = new ReadCSVFile(filename).iterator();
				iterator.next(); // skip header
			}
		}
		
		int nrOfFiles = 0;
		int sorted = 0;
		while (iterator.hasNext()) {
			List<List<String>> tempRows;
			if (chunckSize == -1)
				tempRows = readUntilMemFull(iterator, minFreeMem);
			else
				tempRows = readRows(iterator, chunckSize);
			
			// sort the rows
			Collections.sort(tempRows, comparator);
			
			String tempFilename = generateFilename(filename, nrOfFiles++);
			
			// write temp file to disk
			writeToDisk(header, tempRows, tempFilename);
			
			sorted += tempRows.size();
			if (verbose)
				System.out.println("-Sorted " + sorted + " lines");
		}
		mergeByBatches(nrOfFiles, filename, comparator);
	}
	
	private static boolean isSorted(Iterator<List<String>> iterator, Comparator<List<String>> comparator) {
		if (verbose)
			System.out.println("Checking whether already sorted");
		List<String> previous = null;
		if (iterator.hasNext()) {
			previous = iterator.next();
		}
		int i = 0;
		while (iterator.hasNext()) {
			List<String> row = iterator.next();
			if (comparator.compare(previous, row) > 0) {
				if (verbose)
					System.out.println("File not yet sorted");
				return false;
			}
			previous = row;
			i++;
			if (i > maxCheckRows)
				break;
		}
		if (verbose)
			System.out.println("File already sorted");
		return true;
	}
	
	private static void mergeByBatches(int nrOfFiles, String source, Comparator<List<String>> comparator) {
		int level = 0;
		String sourceBase = source;
		String targetBase;
		do {
			if (verbose)
				System.out.println("Merging " + nrOfFiles + " files");
			
			int newNrOfFiles = 0;
			int from = 0;
			targetBase = source + "_" + level;
			while (from < nrOfFiles) {
				String targetFilename;
				if (nrOfFiles <= maxNumberOfTempFiles)
					targetFilename = source;
				else
					targetFilename = generateFilename(targetBase, newNrOfFiles++);
				
				int to = Math.min(from + maxNumberOfTempFiles, nrOfFiles);
				mergeFiles(sourceBase, from, to, targetFilename, comparator);
				deleteTempFiles(sourceBase, from, to);
				from = to;
			}
			nrOfFiles = newNrOfFiles;
			level++;
			sourceBase = targetBase;
		} while (nrOfFiles > 1);
	}
	
	private static void writeToDisk(List<String> header, List<List<String>> rows, String filename) {
		WriteCSVFile out = new WriteCSVFile(filename);
		if (header != null)
			out.write(header);
		for (List<String> row : rows)
			out.write(row);
		out.close();
	}
	
	private static List<List<String>> readRows(Iterator<List<String>> iterator, int nrOfRows) {
		List<List<String>> rows = new ArrayList<List<String>>();
		int i = 0;
		while (i++ < nrOfRows && iterator.hasNext())
			rows.add(iterator.next());
		return rows;
	}
	
	private static List<List<String>> readUntilMemFull(Iterator<List<String>> iterator, long minFreeMem) {
		List<List<String>> rows = new ArrayList<List<String>>();
		System.gc();
		if (verbose)
			System.out.println("Loading next batch. Available memory: " + getFreeMem());
		long freeMem = Long.MAX_VALUE;
		while (freeMem > minFreeMem && iterator.hasNext()) {
			rows.add(iterator.next());
			freeMem = getFreeMem();
		}
		return rows;
	}
	
	private static long getFreeMem() {
		return Runtime.getRuntime().freeMemory() + Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory();
	}
	
	private static Comparator<List<String>> buildComparator(String[] columnnames, boolean[] sortNumeric, List<String> header) {
		int[] indexesToCompare = new int[columnnames.length];
		for (int i = 0; i < columnnames.length; i++)
			indexesToCompare[i] = getIndexForColumn(header, new String(columnnames[i]));
		
		return new RowComparator(indexesToCompare, sortNumeric);
	}
	
	private static class RowComparator implements Comparator<List<String>> {
		private int[]		indexes;
		private boolean[]	sortNumeric;
		
		public RowComparator(int[] indexes, boolean[] sortNumeric) {
			this.indexes = indexes;
			this.sortNumeric = sortNumeric;
		}
		
		public int compare(List<String> o1, List<String> o2) {
			int result = 0;
			int i = 0;
			while (result == 0 && i < indexes.length) {
				String value1 = o1.get(indexes[i]);
				String value2 = o2.get(indexes[i]);
				if (sortNumeric[i])
					result = efficientLongCompare(value1, value2);
				else
					result = value1.compareTo(value2);
				i++;
			}
			return result;
		}
		
		private int efficientLongCompare(String value1, String value2) {
			if (value1.length() > value2.length())
				return 1;
			else if (value1.length() < value2.length())
				return -1;
			else
				return value1.compareTo(value2);
		}
		
	}
	
	private static void deleteTempFiles(String base, int start, int end) {
		for (int i = start; i < end; i++)
			(new File(generateFilename(base, i))).delete();
	}
	
	private static String generateFilename(String base, int number) {
		return base + "_" + number + ".tmp";
	}
	
	private static void mergeFiles(String sourceBase, int start, int end, String target, Comparator<List<String>> comparator) {
		List<Iterator<List<String>>> tempFiles = new ArrayList<Iterator<List<String>>>();
		
		List<List<String>> filerows = new ArrayList<List<String>>();
		List<String> header = null;
		boolean done = true;
		for (int i = start; i < end; i++) {
			ReadCSVFile tempFile = new ReadCSVFile(generateFilename(sourceBase, i));
			Iterator<List<String>> iterator = tempFile.getIterator();
			if (iterator.hasNext()) {
				if (tempFiles.size() == 0) // its the first one
					header = iterator.next();
				else
					iterator.next();
			}
			tempFiles.add(iterator);
			
			// initialize
			if (iterator.hasNext()) {
				filerows.add(iterator.next());
				done = false;
			} else
				filerows.add(null);
		}
		WriteCSVFile out = new WriteCSVFile(target);
		out.write(header);
		while (!done) {
			// Find best file to pick from:
			List<String> bestRow = null;
			int bestFile = -1;
			for (int i = 0; i < filerows.size(); i++) {
				if (bestRow == null || (filerows.get(i) != null && comparator.compare(filerows.get(i), bestRow) < 0)) {
					bestRow = filerows.get(i);
					bestFile = i;
				}
			}
			if (bestRow == null)
				done = true;
			else {
				// write it to file:
				out.write(bestRow);
				
				// get next from winning file:
				List<String> newRow;
				Iterator<List<String>> bestFileIterator = tempFiles.get(bestFile);
				if (bestFileIterator.hasNext())
					newRow = bestFileIterator.next();
				else
					newRow = null;
				filerows.set(bestFile, newRow);
			}
		}
		out.close();
	}
	
	private static int getIndexForColumn(List<String> list, String value) throws RuntimeException {
		int result;
		if (caseSensitiveColumnNames)
			result = list.indexOf(value);
		else
			result = caseInsensitiveIndexOf(value, list);
		if (result == -1)
			throw (new RuntimeException("File sorter could not find column \"" + value + "\""));
		return result;
	}
	
	public static int caseInsensitiveIndexOf(String value, List<String> list) {
		String queryLC = value.toLowerCase();
		for (int i = 0; i < list.size(); i++) {
			String string = list.get(i);
			if (string.toLowerCase().equals(queryLC))
				return i;
		}
		return -1;
	}
	
}
