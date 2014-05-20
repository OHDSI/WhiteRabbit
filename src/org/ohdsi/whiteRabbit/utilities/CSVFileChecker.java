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
package org.ohdsi.whiteRabbit.utilities;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.ohdsi.ooxml.ReadXlsxFileWithHeader;
import org.ohdsi.utilities.RandomUtilities;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManyList;
import org.ohdsi.utilities.files.ReadTextFile;
import org.ohdsi.utilities.files.Row;
import org.ohdsi.utilities.files.WriteTextFile;
import org.ohdsi.whiteRabbit.etls.ars.ARSETL;

/**
 * Checks a CSV file for formatting issues
 * 
 * @author MSCHUEMI
 */
public class CSVFileChecker {
	
	private static char			delimiter				= ',';
	private static int			bufferSize				= 10000;
	private static int			sampleSize				= 1000;
	private static double		minFractionDateCorrect	= 0.95;
	private int					columnCount;
	private String				filename;
	private List<LineError>		errors;
	private List<ColumnInfo>	columnInfos;
	private JFrame				frame;
	private List<String>		header;
	
	public static void main(String[] args) {
		JFrame frame = new JFrame("Test");
		frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
		frame.pack();
		frame.setVisible(true);
		
		CSVFileChecker checker = new CSVFileChecker();
		checker.setFrame(frame);
		// checker.checkFile("S:/Data/ARS/Simulation/OUTPAT.csv");
		checker.checkSpecifiedFields("S:/Data/ARS/Simulation/", new ReadXlsxFileWithHeader(ARSETL.class.getResourceAsStream("Fields.xlsx")).iterator());
	}
	
	public void checkFile(String filename) {
		checkFile(filename, null);
	}
	
	private void checkFile(String filename, List<ColumnInfo> columnInfos) {
		this.columnInfos = columnInfos;
		this.filename = filename;
		errors = new ArrayList<LineError>();
		int lineNr = 0;
		List<List<String>> rowBuffer = new ArrayList<List<String>>();
		for (String line : new ReadTextFile(filename)) {
			lineNr++;
			List<String> row = StringUtilities.safeSplit(line, delimiter);
			
			if (lineNr == 1) { // header
				columnCount = row.size();
				if (columnInfos == null)
					header = row;
				else
					for (ColumnInfo info : columnInfos) {
						int column = row.indexOf(info.header);
						if (column == -1)
							throw new RuntimeException("Could not find column " + info.header + " in table " + filename);
						else
							info.column = column;
					}
			} else {
				if (columnInfos == null) {
					if (rowBuffer.size() < bufferSize)
						rowBuffer.add(row);
					else {
						profile(rowBuffer);
						rowBuffer = null;
					}
				} else
					checkRow(lineNr, row);
			}
		}
		if (columnInfos == null && rowBuffer.size() > 1)
			profile(rowBuffer);
		
		if (errors.size() != 0)
			if (frame != null) {
				showDialog();
			} else
				reportToConsole();
	}
	
	private void checkRow(int lineNr, List<String> row) {
		if (row.size() != columnCount) {
			errors.add(new LineError(lineNr, "Incorrect number of columns (expected " + columnCount + ", found " + row.size() + ")"));
		} else {
			for (ColumnInfo info : columnInfos) {
				String value = row.get(info.column);
				if (value.trim().length() != 0) {
					if (info.isInteger && !StringUtilities.isInteger(value))
						errors.add(new LineError(lineNr, "Found non-integer value ('" + value + "') where integer value expected in column "
								+ (info.column + 1) + " (" + info.header + ")"));
					else if (info.isDateFormat1 && !isDateFormat1(value))
						errors.add(new LineError(lineNr, "Error in date ('" + value + "') in column " + (info.column + 1) + " (" + info.header + ")"));
					else if (info.isReal && !StringUtilities.isNumber(value))
						errors.add(new LineError(lineNr, "Error in real number ('" + value + "') in column " + (info.column + 1) + " (" + info.header + ")"));
				}
			}
		}
	}
	
	private void showDialog() {
		if (new ErrorDialog(frame, true, null).getAnswer()) {
			String backupName = createBackup();
			copyWithoutErrorLines(backupName, filename);
		}
	}
	
	private void copyWithoutErrorLines(String source, String target) {
		Set<Integer> errorLineNrs = new HashSet<Integer>();
		for (LineError error : errors)
			errorLineNrs.add(error.lineNr);
		
		WriteTextFile out = new WriteTextFile(target);
		int lineNr = 0;
		for (String line : new ReadTextFile(source)) {
			lineNr++;
			if (!errorLineNrs.contains(lineNr))
				out.writeln(line);
		}
		out.close();
		System.out.println("Removed " + errorLineNrs.size() + " lines from " + target);
	}
	
	private String createBackup() {
		String filenameWithoutExtention = filename.substring(0, filename.lastIndexOf('.'));
		
		String backupName = filenameWithoutExtention + ".backup";
		if (new File(backupName).exists()) {
			int i = 1;
			while (new File(filenameWithoutExtention + ".backup" + i).exists())
				i++;
			backupName = filenameWithoutExtention + ".backup" + i;
		}
		
		WriteTextFile out = new WriteTextFile(backupName);
		for (String line : new ReadTextFile(filename))
			out.writeln(line);
		out.close();
		System.out.println("Backed up " + filename + " to " + backupName);
		return backupName;
	}
	
	private class ErrorDialog extends JDialog implements ActionListener {
		private static final long	serialVersionUID	= 4440825466397612738L;
		private JButton				yesButton			= null;
		private JButton				noButton			= null;
		private boolean				answer				= false;
		
		public boolean getAnswer() {
			return answer;
		}
		
		public ErrorDialog(JFrame frame, boolean modal, String dummy) {
			super(frame, modal);
			String shortFilename = new File(filename).getName();
			setTitle("Errors found in " + shortFilename);
			JPanel panel = new JPanel();
			panel.setPreferredSize(new Dimension(800, 500));
			getContentPane().add(panel);
			panel.setLayout(new BorderLayout());
			
			JLabel message = new JLabel("Found " + errors.size() + " errors in " + shortFilename
					+ ". Do you want to automatically remove these lines (a backup will be created)?");
			panel.add(message, BorderLayout.NORTH);
			
			StringBuilder errorText = new StringBuilder();
			for (LineError error : errors)
				errorText.append(error + "\n");
			JTextArea errorArea = new JTextArea(errorText.toString());
			JScrollPane scrollPane = new JScrollPane(errorArea);
			panel.add(scrollPane, BorderLayout.CENTER);
			
			JPanel buttonPanel = new JPanel();
			yesButton = new JButton("Yes");
			yesButton.addActionListener(this);
			buttonPanel.add(yesButton);
			noButton = new JButton("No");
			noButton.addActionListener(this);
			buttonPanel.add(noButton);
			panel.add(buttonPanel, BorderLayout.SOUTH);
			
			pack();
			setLocationRelativeTo(frame);
			setVisible(true);
		}
		
		public void actionPerformed(ActionEvent e) {
			if (yesButton == e.getSource()) {
				answer = true;
				setVisible(false);
			} else if (noButton == e.getSource()) {
				answer = false;
				setVisible(false);
			}
		}
		
	}
	
	private void reportToConsole() {
		for (LineError error : errors)
			System.out.println(error);
	}
	
	private void profile(List<List<String>> rows) {
		columnInfos = new ArrayList<ColumnInfo>(columnCount);
		for (int i = 0; i < header.size(); i++)
			columnInfos.add(new ColumnInfo(i, header.get(i)));
		
		for (List<String> row : RandomUtilities.sampleWithoutReplacement(new ArrayList<List<String>>(rows), sampleSize)) {
			if (row.size() == columnCount) {
				for (ColumnInfo info : columnInfos) {
					String value = row.get(info.column);
					if (value.trim().length() != 0) {
						if (StringUtilities.isInteger(value))
							info.nNotDateFormat1++;
						else {
							info.isInteger = false;
							if (isDateFormat1(value)) {
								info.nDateFormat1++;
								info.isReal = false;
							} else {
								info.nNotDateFormat1++;
								if (!StringUtilities.isNumber(value))
									info.isReal = false;
							}
						}
					}
				}
			}
		}
		for (ColumnInfo columnInfo : columnInfos)
			if (columnInfo.nDateFormat1 > 0
					&& (columnInfo.nDateFormat1 / (double) (columnInfo.nDateFormat1 + columnInfo.nNotDateFormat1) > minFractionDateCorrect))
				columnInfo.isDateFormat1 = true;
		
		// for (int i = 0; i < columnInfos.size(); i++)
		// System.out.println("Column " + columnInfos.get(i).header + "(" + i + "): IsInteger=" + columnInfos.get(i).isInteger + ", IsDate="
		// + columnInfos.get(i).isDateFormat1);
		
		int lineNr = 1; // Skipping header, so start at 1
		for (List<String> row : rows) {
			lineNr++;
			checkRow(lineNr, row);
		}
	}
	
	public static boolean isDateFormat1(String string) {
		if (string.length() >= 10)
			if ((string.charAt(4) == '-' || string.charAt(4) == '/') || (string.charAt(4) == string.charAt(7)))
				try {
					int year = Integer.parseInt(string.substring(0, 4));
					if (year < 1700 || year > 3000)
						return false;
					int month = Integer.parseInt(string.substring(5, 7));
					if (month < 1 || month > 12)
						return false;
					int day = Integer.parseInt(string.substring(8, 10));
					if (day < 1 || day > 31)
						return false;
					return true;
				} catch (Exception e) {
					return false;
				}
		return false;
	}
	
	public JFrame getFrame() {
		return frame;
	}
	
	public void setFrame(JFrame frame) {
		this.frame = frame;
	}
	
	private class LineError {
		public int		lineNr;
		public String	error;
		
		public LineError(int lineNr, String error) {
			this.lineNr = lineNr;
			this.error = error;
		}
		
		public String toString() {
			return "Error in line " + lineNr + ": " + error;
		}
	}
	
	private class ColumnInfo {
		public ColumnInfo() {
		}
		
		public ColumnInfo(int column, String header) {
			this.header = header;
			this.column = column;
		}
		
		public int		column;
		public String	header;
		public int		nDateFormat1	= 0;
		public int		nNotDateFormat1	= 0;
		public boolean	isDateFormat1	= false;	// yyyy-mm-dd
		public boolean	isInteger		= true;
		public boolean	isReal			= true;
	}
	
	public void checkSpecifiedFields(String folder, Iterator<Row> iterator) {
		OneToManyList<String, Row> tableToFields = new OneToManyList<String, Row>();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			if (row.get("Table").trim().length() != 0)
				tableToFields.put(row.get("Table"), row);
		}
		for (String table : tableToFields.keySet()) {
			System.out.println("Checking " + table);
			List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();
			for (Row row : tableToFields.get(table))
				if (row.get("Check").equals("1")) {
					ColumnInfo info = new ColumnInfo();
					info.header = row.get("Field");
					if (row.get("Type").toLowerCase().equals("integer")) {
						info.isInteger = true;
						info.isDateFormat1 = false;
						info.isReal = false;
					} else if (row.get("Type").toLowerCase().equals("real")) {
						info.isInteger = false;
						info.isDateFormat1 = false;
						info.isReal = true;
					} else if (row.get("Type").toLowerCase().equals("date")) {
						info.isInteger = false;
						info.isDateFormat1 = true;
						info.isReal = false;
					} else {
						info.isInteger = false;
						info.isDateFormat1 = false;
						info.isReal = false;
					}
					columnInfos.add(info);
				}
			checkFile(folder + "/" + table, columnInfos);
		}
	}
}
