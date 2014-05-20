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

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.MediaTracker;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.DirectoryUtilities;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.whiteRabbit.etls.ars.ARSETL;
import org.ohdsi.whiteRabbit.etls.hcup.HCUPETL;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;
import org.ohdsi.whiteRabbit.utilities.SqlDump;
import org.ohdsi.whiteRabbit.vocabulary.InsertVocabularyInServer;

public class WhiteRabbitMain {
	
	private JFrame				frame;
	private JTextField			folderField;
	private JTextField			vocabFileField;
	
	private JComboBox			etlType;
	private JComboBox			sourceType;
	private JComboBox			targetType;
	private JTextField			targetUserField;
	private JTextField			targetPasswordField;
	private JTextField			targetServerField;
	private JTextField			targetDatabaseField;
	private JTextField			sourceDelimiterField;
	private JTextField			sourceServerField;
	private JTextField			sourceUserField;
	private JTextField			sourcePasswordField;
	private JTextField			sourceDatabaseField;
	private JButton				addAllButton;
	private JList				tableList;
	private List<String>		tables							= new ArrayList<String>();
	private JTextArea			sqlArea;
	private JTextField			sqlDumpFilenameField;
	private JButton				dumpButton;
	private boolean				sourceIsFiles					= true;
	
	private List<JComponent>	componentsToDisableWhenRunning	= new ArrayList<JComponent>();
	
	public static void main(String[] args) {
		new WhiteRabbitMain(args);
	}
	
	public WhiteRabbitMain(String[] args) {
		frame = new JFrame("White Rabbit");
		
		frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
		frame.setLayout(new BorderLayout());
		
		JComponent tabsPanel = createTabsPanel();
		JComponent consolePanel = createConsolePanel();
		
		frame.add(consolePanel, BorderLayout.CENTER);
		frame.add(tabsPanel, BorderLayout.NORTH);
		
		loadIcons(frame);
		frame.pack();
		frame.setVisible(true);
		ObjectExchange.frame = frame;
		executeParameters(args);
	}
	
	private JComponent createTabsPanel() {
		JTabbedPane tabbedPane = new JTabbedPane();
		
		JPanel locationPanel = createLocationsPanel();
		tabbedPane.addTab("Locations", null, locationPanel, "Specify the location of the source data, the CDM, and the working folder");
		
		JPanel dumpPanel = createSqlDumpPanel();
		tabbedPane.addTab("Bulk SQL dump", null, dumpPanel, "Write the results of a SQL query to a CSV file");
		
		JPanel scanPanel = createScanPanel();
		tabbedPane.addTab("Scan", null, scanPanel, "Create a scan of the source data");
		
		JPanel vocabPanel = createVocabPanel();
		tabbedPane.addTab("Vocabulary", null, vocabPanel, "Upload the vocabulary to the server");
		
		JPanel etlPanel = createEtlPanel();
		tabbedPane.addTab("ETL", null, etlPanel, "Extract, Transform and Load the data into the OMOP CDM");
		
		return tabbedPane;
	}
	
	private JPanel createLocationsPanel() {
		JPanel panel = new JPanel();
		
		panel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.5;
		
		JPanel folderPanel = new JPanel();
		folderPanel.setLayout(new BoxLayout(folderPanel, BoxLayout.X_AXIS));
		folderPanel.setBorder(BorderFactory.createTitledBorder("Working folder"));
		folderField = new JTextField();
		folderField.setText((new File("").getAbsolutePath()));
		folderField.setToolTipText("The folder where all output will be written");
		folderPanel.add(folderField);
		JButton pickButton = new JButton("Pick folder");
		pickButton.setToolTipText("Pick a different working folder");
		folderPanel.add(pickButton);
		pickButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickFile();
			}
		});
		componentsToDisableWhenRunning.add(pickButton);
		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = 2;
		panel.add(folderPanel, c);
		
		JPanel sourcePanel = new JPanel();
		sourcePanel.setLayout(new GridLayout(0, 2));
		sourcePanel.setBorder(BorderFactory.createTitledBorder("Source data location"));
		sourcePanel.add(new JLabel("Data type"));
		sourceType = new JComboBox(new String[] { "Delimited text files", "MySQL", "Oracle", "SQL Server", "PostgreSQL" });
		sourceType.setToolTipText("Select the type of source data available");
		sourceType.addItemListener(new ItemListener() {
			
			@Override
			public void itemStateChanged(ItemEvent arg0) {
				sourceIsFiles = arg0.getItem().toString().equals("Delimited text files");
				sourceServerField.setEnabled(!sourceIsFiles);
				sourceUserField.setEnabled(!sourceIsFiles);
				sourcePasswordField.setEnabled(!sourceIsFiles);
				sourceDatabaseField.setEnabled(!sourceIsFiles);
				sourceDelimiterField.setEnabled(sourceIsFiles);
				addAllButton.setEnabled(!sourceIsFiles);
				dumpButton.setEnabled(!sourceIsFiles);
				
				if (!sourceIsFiles && arg0.getItem().toString().equals("Oracle")) {
					sourceServerField
							.setToolTipText("For Oracle servers this field contains the SID, servicename, and optionally the port: '<host>/<sid>', '<host>:<port>/<sid>', '<host>/<service name>', or '<host>:<port>/<service name>'");
					sourceUserField.setToolTipText("For Oracle servers this field contains the name of the user used to log in");
					sourcePasswordField.setToolTipText("For Oracle servers this field contains the password corresponding to the user");
					sourceDatabaseField
							.setToolTipText("For Oracle servers this field contains the schema (i.e. 'user' in Oracle terms) containing the source tables");
				} else if (!sourceIsFiles && arg0.getItem().toString().equals("PostgreSQL")) {
					sourceServerField.setToolTipText("For PostgreSQL servers this field contains the host name and database name (<host>/<database>)");
					sourceUserField.setToolTipText("The user used to log in to the server");
					sourcePasswordField.setToolTipText("The password used to log in to the server");
					sourceDatabaseField.setToolTipText("For PostgreSQL servers this field contains the schema containing the source tables");
				} else if (!sourceIsFiles) {
					sourceServerField.setToolTipText("This field contains the name or IP address of the database server");
					if (arg0.getItem().toString().equals("SQL Server"))
						sourceUserField
								.setToolTipText("The user used to log in to the server. Optionally, the domain can be specified as <domain>/<user> (e.g. 'MyDomain/Joe')");
					else
						sourceUserField.setToolTipText("The user used to log in to the server");
					sourcePasswordField.setToolTipText("The password used to log in to the server");
					sourceDatabaseField.setToolTipText("The name of the database containing the source tables");
				}
			}
		});
		sourcePanel.add(sourceType);
		
		sourcePanel.add(new JLabel("Server location"));
		sourceServerField = new JTextField("127.0.0.1");
		sourceServerField.setEnabled(false);
		sourcePanel.add(sourceServerField);
		sourcePanel.add(new JLabel("User name"));
		sourceUserField = new JTextField("");
		sourceUserField.setEnabled(false);
		sourcePanel.add(sourceUserField);
		sourcePanel.add(new JLabel("Password"));
		sourcePasswordField = new JPasswordField("");
		sourcePasswordField.setEnabled(false);
		sourcePanel.add(sourcePasswordField);
		sourcePanel.add(new JLabel("Database name"));
		sourceDatabaseField = new JTextField("");
		sourceDatabaseField.setEnabled(false);
		sourcePanel.add(sourceDatabaseField);
		
		sourcePanel.add(new JLabel("Delimiter"));
		sourceDelimiterField = new JTextField(",");
		sourceDelimiterField.setToolTipText("The delimiter that separates values. Enter 'tab' for tab.");
		sourcePanel.add(sourceDelimiterField);
		
		c.gridx = 0;
		c.gridy = 1;
		c.gridwidth = 1;
		panel.add(sourcePanel, c);
		
		JPanel targetPanel = new JPanel();
		targetPanel.setLayout(new GridLayout(0, 2));
		targetPanel.setBorder(BorderFactory.createTitledBorder("Target data location"));
		targetPanel.add(new JLabel("Data type"));
		targetType = new JComboBox(new String[] { "MySQL", "Oracle", "SQL Server", "PostgreSQL" });
		targetType.setToolTipText("Select the type of server where the CDM and vocabulary will be stored");
		targetPanel.add(targetType);
		targetPanel.add(new JLabel("Server location"));
		targetServerField = new JTextField("127.0.0.1");
		targetPanel.add(targetServerField);
		targetPanel.add(new JLabel("User name"));
		targetUserField = new JTextField("root");
		targetPanel.add(targetUserField);
		targetPanel.add(new JLabel("Password"));
		targetPasswordField = new JPasswordField("");
		targetPanel.add(targetPasswordField);
		targetPanel.add(new JLabel("CDM database name"));
		targetDatabaseField = new JTextField("CDM_v4");
		targetPanel.add(targetDatabaseField);
		targetPanel.add(new JLabel(""));
		targetPanel.add(new JLabel(""));
		
		c.gridx = 1;
		c.gridy = 1;
		c.gridwidth = 1;
		panel.add(targetPanel, c);
		
		return panel;
	}
	
	private JPanel createSqlDumpPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());
		
		JPanel inputPanel = new JPanel();
		inputPanel.setLayout(new BorderLayout());
		
		sqlArea = new JTextArea();
		sqlArea.setToolTipText("Enter the SQL statement to retrieve the date here");
		JScrollPane sqlPane = new JScrollPane(sqlArea);
		sqlPane.setBorder(BorderFactory.createTitledBorder("SQL"));
		sqlPane.setAutoscrolls(true);
		inputPanel.add(sqlPane, BorderLayout.CENTER);
		
		sqlDumpFilenameField = new JTextField("Dump.csv");
		sqlDumpFilenameField.setBorder(BorderFactory.createTitledBorder("Output filename"));
		sqlDumpFilenameField.setToolTipText("The name of the CSV file where the result of the SQL query will be written");
		inputPanel.add(sqlDumpFilenameField, BorderLayout.SOUTH);
		
		panel.add(inputPanel, BorderLayout.CENTER);
		
		JPanel dumpButtonPanel = new JPanel();
		dumpButtonPanel.setLayout(new BoxLayout(dumpButtonPanel, BoxLayout.X_AXIS));
		dumpButtonPanel.add(Box.createHorizontalGlue());
		
		dumpButton = new JButton("Dump SQL results to file");
		dumpButton.setBackground(new Color(151, 220, 141));
		dumpButton.setToolTipText("Execute the SQL statement, and write the output to the file");
		dumpButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				sqlDumpRun();
			}
		});
		dumpButton.setEnabled(false);
		componentsToDisableWhenRunning.add(dumpButton);
		dumpButtonPanel.add(dumpButton);
		panel.add(dumpButtonPanel, BorderLayout.SOUTH);
		
		return panel;
	}
	
	private JPanel createScanPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());
		
		JPanel tablePanel = new JPanel();
		tablePanel.setLayout(new BorderLayout());
		tablePanel.setBorder(new TitledBorder("Tables"));
		tableList = new JList();
		tableList.setToolTipText("Specify the tables (or CSV files) to be scanned here");
		tablePanel.add(new JScrollPane(tableList), BorderLayout.CENTER);
		
		JPanel tableButtonPanel = new JPanel();
		tableButtonPanel.setLayout(new GridLayout(3, 1));
		addAllButton = new JButton("Add all in DB");
		addAllButton.setToolTipText("Add all tables in the database");
		addAllButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addAllTables();
			}
		});
		addAllButton.setEnabled(false);
		tableButtonPanel.add(addAllButton);
		JButton addButton = new JButton("Add");
		addButton.setToolTipText("Add tables to list");
		addButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickTables();
			}
		});
		tableButtonPanel.add(addButton);
		JButton removeButton = new JButton("Remove");
		removeButton.setToolTipText("Remove tables from list");
		removeButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				removeTables();
			}
		});
		tableButtonPanel.add(removeButton);
		tablePanel.add(tableButtonPanel, BorderLayout.EAST);
		
		panel.add(tablePanel, BorderLayout.CENTER);
		
		JPanel scanButtonPanel = new JPanel();
		scanButtonPanel.setLayout(new BoxLayout(scanButtonPanel, BoxLayout.X_AXIS));
		scanButtonPanel.add(Box.createHorizontalGlue());
		
		JButton scan100kButton = new JButton("Scan 100k rows per table");
		scan100kButton.setBackground(new Color(151, 220, 141));
		scan100kButton.setToolTipText("Perform the scan on a random sample of 100,000 rows per table");
		scan100kButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				scanRun(100000);
			}
		});
		componentsToDisableWhenRunning.add(scan100kButton);
		scanButtonPanel.add(scan100kButton);
		
		JButton scan1mButton = new JButton("Scan 1m rows per table");
		scan1mButton.setBackground(new Color(151, 220, 141));
		scan1mButton.setToolTipText("Perform the scan on a random sample of 1 million rows per table");
		scan1mButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				scanRun(1000000);
			}
		});
		componentsToDisableWhenRunning.add(scan1mButton);
		scanButtonPanel.add(scan1mButton);
		
		JButton scanButton = new JButton("Scan full tables");
		scanButton.setBackground(new Color(151, 220, 141));
		scanButton.setToolTipText("Perform the scan on the full tables");
		scanButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				scanRun(-1);
			}
		});
		componentsToDisableWhenRunning.add(scanButton);
		scanButtonPanel.add(scanButton);
		panel.add(scanButtonPanel, BorderLayout.SOUTH);
		
		return panel;
	}
	
	private JPanel createVocabPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
		
		JPanel vocabFilePanel = new JPanel();
		vocabFilePanel.setLayout(new BoxLayout(vocabFilePanel, BoxLayout.X_AXIS));
		vocabFilePanel.setBorder(BorderFactory.createTitledBorder("Vocabulary data file"));
		
		vocabFileField = new JTextField();
		vocabFileField.setText("Vocabulary.dat");
		vocabFileField.setToolTipText("Specify the name of the file containing the vocabulary here");
		vocabFilePanel.add(vocabFileField);
		JButton pickButton = new JButton("Pick file");
		pickButton.setToolTipText("Select a different vocabulary file");
		vocabFilePanel.add(pickButton);
		pickButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickVocabFile();
			}
		});
		vocabFilePanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, vocabFilePanel.getPreferredSize().height));
		panel.add(vocabFilePanel);
		
		panel.add(Box.createVerticalGlue());
		
		JPanel vocabButtonPanel = new JPanel();
		vocabButtonPanel.setLayout(new BoxLayout(vocabButtonPanel, BoxLayout.X_AXIS));
		
		vocabButtonPanel.add(Box.createHorizontalGlue());
		JButton vocabButton = new JButton("Insert vocabulary");
		vocabButton.setBackground(new Color(151, 220, 141));
		vocabButton.setToolTipText("Insert the vocabulary database into the server");
		vocabButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				vocabRun();
			}
		});
		componentsToDisableWhenRunning.add(vocabButton);
		vocabButtonPanel.add(vocabButton);
		
		panel.add(vocabButtonPanel);
		
		return panel;
	}
	
	private JPanel createEtlPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
		
		JPanel etlTypePanel = new JPanel();
		etlTypePanel.setLayout(new BoxLayout(etlTypePanel, BoxLayout.X_AXIS));
		etlTypePanel.setBorder(BorderFactory.createTitledBorder("ETL type"));
		etlType = new JComboBox(new String[] { "ARS -> OMOP CDM V4", "HCUP -> OMOP CDM V4" });
		etlType.setToolTipText("Select the appropriate ETL process");
		etlTypePanel.add(etlType);
		etlTypePanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, etlTypePanel.getPreferredSize().height));
		panel.add(etlTypePanel);
		
		panel.add(Box.createVerticalGlue());
		
		JPanel etlButtonPanel = new JPanel();
		etlButtonPanel.setLayout(new BoxLayout(etlButtonPanel, BoxLayout.X_AXIS));
		etlButtonPanel.add(Box.createHorizontalGlue());
		
		JButton etl10kButton = new JButton("Perform 10k persons ETL");
		etl10kButton.setBackground(new Color(151, 220, 141));
		etl10kButton.setToolTipText("Perform the ETL for the first 10,000 persons");
		etl10kButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				etlRun(10000);
			}
		});
		componentsToDisableWhenRunning.add(etl10kButton);
		etlButtonPanel.add(etl10kButton);
		
		// etlButtonPanel.add(Box.createHorizontalStrut(10));
		
		JButton etlButton = new JButton("Perform ETL");
		etlButton.setBackground(new Color(151, 220, 141));
		etlButton.setToolTipText("Extract, Transform and Load the data into the OMOP CDM");
		etlButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				etlRun(Integer.MAX_VALUE);
			}
		});
		componentsToDisableWhenRunning.add(etlButton);
		etlButtonPanel.add(etlButton);
		panel.add(etlButtonPanel);
		
		return panel;
	}
	
	private JComponent createConsolePanel() {
		JTextArea consoleArea = new JTextArea();
		consoleArea.setToolTipText("General progress information");
		consoleArea.setEditable(false);
		Console console = new Console();
		console.setTextArea(consoleArea);
		// console.setDebugFile("c:/temp/debug.txt");
		System.setOut(new PrintStream(console));
		System.setErr(new PrintStream(console));
		JScrollPane consoleScrollPane = new JScrollPane(consoleArea);
		consoleScrollPane.setBorder(BorderFactory.createTitledBorder("Console"));
		consoleScrollPane.setPreferredSize(new Dimension(800, 200));
		consoleScrollPane.setAutoscrolls(true);
		ObjectExchange.console = console;
		return consoleScrollPane;
	}
	
	private void loadIcons(JFrame f) {
		List<Image> icons = new ArrayList<Image>();
		icons.add(loadIcon("WhiteRabbit16.png", f));
		icons.add(loadIcon("WhiteRabbit32.png", f));
		icons.add(loadIcon("WhiteRabbit48.png", f));
		icons.add(loadIcon("WhiteRabbit64.png", f));
		icons.add(loadIcon("WhiteRabbit128.png", f));
		icons.add(loadIcon("WhiteRabbit256.png", f));
		f.setIconImages(icons);
	}
	
	private Image loadIcon(String name, JFrame f) {
		Image icon = Toolkit.getDefaultToolkit().getImage(WhiteRabbitMain.class.getResource(name));
		MediaTracker mediaTracker = new MediaTracker(f);
		mediaTracker.addImage(icon, 0);
		try {
			mediaTracker.waitForID(0);
			return icon;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return null;
	}
	
	private void executeParameters(String[] args) {
		String mode = null;
		for (String arg : args) {
			if (arg.startsWith("-")) {
				mode = arg.toLowerCase();
			} else {
				if (mode.equals("-folder"))
					folderField.setText(arg);
				if (mode.equals("-targetpassword"))
					targetPasswordField.setText(arg);
				if (mode.equals("-targetserver"))
					targetServerField.setText(arg);
				if (mode.equals("-targettype"))
					targetType.setSelectedItem(arg);
				if (mode.equals("-targetdatabase"))
					targetDatabaseField.setText(arg);
				if (mode.equals("-targetuser"))
					targetUserField.setText(arg);
				if (mode.equals("-sourceserver"))
					sourceServerField.setText(arg);
				if (mode.equals("-sourcetype"))
					sourceType.setSelectedItem(arg);
				if (mode.equals("-sourcedatabase"))
					sourceDatabaseField.setText(arg);
				if (mode.equals("-sourceuser"))
					sourceUserField.setText(arg);
				mode = null;
			}
		}
	}
	
	private void pickFile() {
		JFileChooser fileChooser = new JFileChooser(new File(folderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			folderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}
	
	private void removeTables() {
		for (Object item : tableList.getSelectedValues()) {
			tables.remove(item);
			tableList.setListData(tables.toArray());
		}
	}
	
	private void addAllTables() {
		DbSettings sourceDbSettings = getSourceDbSettings();
		if (sourceDbSettings != null) {
			RichConnection connection = new RichConnection(sourceDbSettings.server, sourceDbSettings.domain, sourceDbSettings.user, sourceDbSettings.password,
					sourceDbSettings.dbType);
			for (String table : connection.getTableNames(sourceDbSettings.database)) {
				if (!tables.contains(table))
					tables.add((String) table);
				tableList.setListData(tables.toArray());
			}
			connection.close();
		}
	}
	
	private void pickTables() {
		DbSettings sourceDbSettings = getSourceDbSettings();
		if (sourceDbSettings != null) {
			if (sourceDbSettings.dataType == DbSettings.CSVFILES) {
				JFileChooser fileChooser = new JFileChooser(new File(folderField.getText()));
				fileChooser.setMultiSelectionEnabled(true);
				fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				FileNameExtensionFilter filter = new FileNameExtensionFilter("Delimited text files", "csv", "txt");
				fileChooser.setFileFilter(filter);
				
				int returnVal = fileChooser.showDialog(frame, "Select tables");
				if (returnVal == JFileChooser.APPROVE_OPTION) {
					for (File table : fileChooser.getSelectedFiles()) {
						String tableName = DirectoryUtilities.getRelativePath(new File(folderField.getText()), table);
						if (!tables.contains(tableName))
							tables.add(tableName);
						tableList.setListData(tables.toArray());
					}
					
				}
			} else if (sourceDbSettings.dataType == DbSettings.DATABASE) {
				RichConnection connection = new RichConnection(sourceDbSettings.server, sourceDbSettings.domain, sourceDbSettings.user,
						sourceDbSettings.password, sourceDbSettings.dbType);
				String tableNames = StringUtilities.join(connection.getTableNames(sourceDbSettings.database), "\t");
				DBTableSelectionDialog selectionDialog = new DBTableSelectionDialog(frame, true, tableNames);
				if (selectionDialog.getAnswer()) {
					for (Object item : selectionDialog.getSelectedItems()) {
						if (!tables.contains(item))
							tables.add((String) item);
						tableList.setListData(tables.toArray());
					}
				}
				connection.close();
			}
		}
	}
	
	private void pickVocabFile() {
		JFileChooser fileChooser = new JFileChooser(new File(folderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select vocabulary file");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			vocabFileField.setText(DirectoryUtilities.getRelativePath(new File(folderField.getText()), fileChooser.getSelectedFile()));
	}
	
	private DbSettings getSourceDbSettings() {
		DbSettings dbSettings = new DbSettings();
		if (sourceType.getSelectedItem().equals("Delimited text files")) {
			dbSettings.dataType = DbSettings.CSVFILES;
			if (sourceDelimiterField.getText().length() == 0) {
				JOptionPane.showMessageDialog(frame, "Delimiter field cannot be empty for source database", "Error connecting to server",
						JOptionPane.ERROR_MESSAGE);
				return null;
			}
			if (sourceDelimiterField.getText().toLowerCase().equals("tab"))
				dbSettings.delimiter = '\t';
			else
				dbSettings.delimiter = sourceDelimiterField.getText().charAt(0);
		} else {
			dbSettings.dataType = DbSettings.DATABASE;
			dbSettings.user = sourceUserField.getText();
			dbSettings.password = sourcePasswordField.getText();
			dbSettings.server = sourceServerField.getText();
			dbSettings.database = sourceDatabaseField.getText().trim().length() == 0 ? null : sourceDatabaseField.getText();
			if (sourceType.getSelectedItem().toString().equals("MySQL"))
				dbSettings.dbType = DbType.MYSQL;
			else if (sourceType.getSelectedItem().toString().equals("Oracle"))
				dbSettings.dbType = DbType.ORACLE;
			else if (sourceType.getSelectedItem().toString().equals("PostgreSQL"))
				dbSettings.dbType = DbType.POSTGRESQL;
			else if (sourceType.getSelectedItem().toString().equals("SQL Server")) {
				dbSettings.dbType = DbType.MSSQL;
				if (sourceUserField.getText().length() != 0) { // Not using windows authentication
					String[] parts = sourceUserField.getText().split("/");
					if (parts.length < 2) {
						throw new RuntimeException("For SQL server you need to specify the domain in the user name field (e.g. Mydomain/Joe)");
					} else {
						dbSettings.user = parts[1];
						dbSettings.domain = parts[0];
					}
				}
				
			}
			
			if (dbSettings.database == null) {
				String message = "Please specify a name for the source database";
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Database error", JOptionPane.ERROR_MESSAGE);
				return null;
			}
			
		}
		return dbSettings;
	}
	
	private void testConnection(DbSettings dbSettings, boolean testConnectionToDb) {
		RichConnection connection;
		try {
			connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		} catch (Exception e) {
			String message = "Could not connect to source server: " + e.getMessage();
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
			return;
		}
		
		if (testConnectionToDb)
			try {
				connection.getTableNames(dbSettings.database);
			} catch (Exception e) {
				String message = "Could not connect to database: " + e.getMessage();
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
				return;
			}
		
		connection.close();
	}
	
	private DbSettings getTargetDbSettings() {
		DbSettings dbSettings = new DbSettings();
		dbSettings.dataType = DbSettings.DATABASE;
		dbSettings.user = targetUserField.getText();
		dbSettings.password = targetPasswordField.getText();
		dbSettings.server = targetServerField.getText();
		dbSettings.database = targetDatabaseField.getText();
		if (targetType.getSelectedItem().toString().equals("MySQL"))
			dbSettings.dbType = DbType.MYSQL;
		else if (targetType.getSelectedItem().toString().equals("Oracle"))
			dbSettings.dbType = DbType.ORACLE;
		else if (sourceType.getSelectedItem().toString().equals("PostgreSQL"))
			dbSettings.dbType = DbType.POSTGRESQL;
		else if (sourceType.getSelectedItem().toString().equals("SQL Server")) {
			dbSettings.dbType = DbType.MSSQL;
			if (sourceUserField.getText().length() != 0) { // Not using windows authentication
				String[] parts = sourceUserField.getText().split("/");
				if (parts.length < 2) {
					throw new RuntimeException("For SQL server you need to specify the domain in the user name field (e.g. Mydomain/Joe)");
				} else {
					dbSettings.user = parts[1];
					dbSettings.domain = parts[0];
				}
			}
		}
		
		if (dbSettings.database.trim().length() == 0) {
			String message = "Please specify a name for the target database";
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Database error", JOptionPane.ERROR_MESSAGE);
			return null;
		}
		
		return dbSettings;
	}
	
	private void etlRun(int maxPersons) {
		EtlThread etlThread = new EtlThread(maxPersons);
		etlThread.start();
	}
	
	private void scanRun(int maxRows) {
		ScanThread scanThread = new ScanThread(maxRows);
		scanThread.start();
	}
	
	private void sqlDumpRun() {
		SqlDumpThread sqlDumpThread = new SqlDumpThread();
		sqlDumpThread.start();
	}
	
	private void vocabRun() {
		VocabRunThread thread = new VocabRunThread();
		thread.start();
	}
	
	private class SqlDumpThread extends Thread {
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getSourceDbSettings();
				testConnection(dbSettings, true);
				if (dbSettings != null) {
					SqlDump sqlDump = new SqlDump();
					sqlDump.process(dbSettings, sqlArea.getText(), folderField.getText() + "/" + sqlDumpFilenameField.getText());
				}
				
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
				dumpButton.setEnabled(!sourceIsFiles);
			}
		}
		
	}
	
	private class EtlThread extends Thread {
		
		private int	maxPersons;
		
		public EtlThread(int maxPersons) {
			this.maxPersons = maxPersons;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			
			try {
				if (etlType.getSelectedItem().equals("ARS -> OMOP CDM V4")) {
					ARSETL etl = new ARSETL();
					DbSettings dbSettings = getTargetDbSettings();
					testConnection(dbSettings, false);
					if (dbSettings != null)
						etl.process(folderField.getText(), dbSettings, maxPersons);
				}
				if (etlType.getSelectedItem().equals("HCUP -> OMOP CDM V4")) {
					HCUPETL etl = new HCUPETL();
					DbSettings sourceDbSettings = getSourceDbSettings();
					DbSettings targetDbSettings = getTargetDbSettings();
					if (sourceDbSettings != null && targetDbSettings != null) {
						testConnection(sourceDbSettings, true);
						testConnection(targetDbSettings, false);
						etl.process(folderField.getText(), sourceDbSettings, targetDbSettings, maxPersons);
					}
				}
				
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
				dumpButton.setEnabled(!sourceIsFiles);
			}
		}
		
	}
	
	private class ScanThread extends Thread {
		
		private int	maxRows;
		
		public ScanThread(int maxRows) {
			this.maxRows = maxRows;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				SourceDataScan sourceDataScan = new SourceDataScan();
				DbSettings dbSettings = getSourceDbSettings();
				if (dbSettings != null) {
					for (String table : tables) {
						if (dbSettings.dataType == DbSettings.CSVFILES)
							table = folderField.getText() + "/" + table;
						dbSettings.tables.add(table);
					}
					sourceDataScan.process(dbSettings, maxRows, folderField.getText() + "/ScanReport.xlsx");
				}
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
				dumpButton.setEnabled(!sourceIsFiles);
			}
		}
		
	}
	
	private class VocabRunThread extends Thread {
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				InsertVocabularyInServer process = new InsertVocabularyInServer();
				DbSettings dbSettings = getTargetDbSettings();
				if (dbSettings != null)
					process.process(folderField.getText() + "/" + vocabFileField.getText(), dbSettings);
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
				dumpButton.setEnabled(!sourceIsFiles);
			}
			
		}
	}
	
	private class DBTableSelectionDialog extends JDialog implements ActionListener {
		private static final long	serialVersionUID	= 4527207331482143091L;
		private JButton				yesButton			= null;
		private JButton				noButton			= null;
		private boolean				answer				= false;
		private JList				list;
		
		public boolean getAnswer() {
			return answer;
		}
		
		public DBTableSelectionDialog(JFrame frame, boolean modal, String tableNames) {
			super(frame, modal);
			
			setTitle("Select tables");
			JPanel panel = new JPanel();
			panel.setPreferredSize(new Dimension(800, 500));
			getContentPane().add(panel);
			panel.setLayout(new BorderLayout());
			
			JLabel message = new JLabel("Select tables");
			panel.add(message, BorderLayout.NORTH);
			
			list = new JList(tableNames.split("\t"));
			JScrollPane scrollPane = new JScrollPane(list);
			panel.add(scrollPane, BorderLayout.CENTER);
			
			JPanel buttonPanel = new JPanel();
			yesButton = new JButton("Select tables");
			yesButton.addActionListener(this);
			buttonPanel.add(yesButton);
			noButton = new JButton("Cancel");
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
		
		public Object[] getSelectedItems() {
			return list.getSelectedValues();
		}
		
	}
	
	private void handleError(Exception e) {
		System.err.println("Error: " + e.getMessage());
		String errorReportFilename = ErrorReport.generate(folderField.getText(), e);
		String message = "Error: " + e.getLocalizedMessage();
		message += "\nAn error report has been generated:\n" + errorReportFilename;
		System.out.println(message);
		JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error", JOptionPane.ERROR_MESSAGE);
	}
	
}
