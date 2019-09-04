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
package org.ohdsi.whiteRabbit;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Desktop;
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
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.commons.csv.CSVFormat;
import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.utilities.DirectoryUtilities;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.IniFile;
import org.ohdsi.whiteRabbit.fakeDataGenerator.FakeDataGenerator;
import org.ohdsi.whiteRabbit.scan.SourceDataScan;

/**
 * This is the WhiteRabbit main class
 */
public class WhiteRabbitMain implements ActionListener {

	public final static String	WIKI_URL						= "http://www.ohdsi.org/web/wiki/doku.php?id=documentation:software:whiterabbit";
	public final static String	ACTION_CMD_HELP					= "Open help Wiki";

	private JFrame				frame;
	private JTextField			folderField;
	private JTextField			scanReportFileField;

	private JComboBox<String>	scanRowCount;
	private JComboBox<String>	scanValuesCount;
	private JCheckBox			scanValueScan;
	private JSpinner			scanMinCellCount;
	private JSpinner			generateRowCount;
	private JComboBox<String>	sourceType;
	private JComboBox<String>	targetType;
	private JTextField			targetUserField;
	private JTextField			targetPasswordField;
	private JTextField			targetServerField;
	private JTextField			targetDatabaseField;
	private JTextField			sourceDelimiterField;
	private JComboBox<String> 	targetCSVFormat;
	private JTextField			sourceServerField;
	private JTextField			sourceUserField;
	private JTextField			sourcePasswordField;
	private JTextField			sourceDatabaseField;
	private JButton				addAllButton;
	private JList<String>		tableList;
	private Vector<String>		tables							= new Vector<String>();
	private boolean				sourceIsFiles					= true;
	private boolean				targetIsFiles					= false;

	private List<JComponent>	componentsToDisableWhenRunning	= new ArrayList<JComponent>();

	public static void main(String[] args) {
		new WhiteRabbitMain(args);
	}

	public WhiteRabbitMain(String[] args) {
		if (args.length == 2 && args[0].equalsIgnoreCase("-ini"))
			launchCommandLine(args[1]);
		else {
			frame = new JFrame("White Rabbit");

			frame.addWindowListener(new WindowAdapter() {
				public void windowClosing(WindowEvent e) {
					System.exit(0);
				}
			});
			frame.setLayout(new BorderLayout());
			frame.setJMenuBar(createMenuBar());

			JComponent tabsPanel = createTabsPanel();
			JComponent consolePanel = createConsolePanel();

			frame.add(consolePanel, BorderLayout.CENTER);
			frame.add(tabsPanel, BorderLayout.NORTH);

			loadIcons(frame);
			frame.pack();
			frame.setVisible(true);
			ObjectExchange.frame = frame;
		}
	}

	private void launchCommandLine(String iniFileName) {
		IniFile iniFile = new IniFile(iniFileName);
		DbSettings dbSettings = new DbSettings();
		if (iniFile.get("DATA_TYPE").equalsIgnoreCase("Delimited text files")) {
			dbSettings.dataType = DbSettings.CSVFILES;
			if (iniFile.get("DELIMITER").equalsIgnoreCase("tab"))
				dbSettings.delimiter = '\t';
			else
				dbSettings.delimiter = iniFile.get("DELIMITER").charAt(0);
		} else {
			dbSettings.dataType = DbSettings.DATABASE;
			dbSettings.user = iniFile.get("USER_NAME");
			dbSettings.password = iniFile.get("PASSWORD");
			dbSettings.server = iniFile.get("SERVER_LOCATION");
			dbSettings.database = iniFile.get("DATABASE_NAME");
			if (iniFile.get("DATA_TYPE").equalsIgnoreCase("MySQL"))
				dbSettings.dbType = DbType.MYSQL;
			else if (iniFile.get("DATA_TYPE").equalsIgnoreCase("Oracle"))
				dbSettings.dbType = DbType.ORACLE;
			else if (iniFile.get("DATA_TYPE").equalsIgnoreCase("PostgreSQL"))
				dbSettings.dbType = DbType.POSTGRESQL;
			else if (iniFile.get("DATA_TYPE").equalsIgnoreCase("Redshift"))
				dbSettings.dbType = DbType.REDSHIFT;
			else if (iniFile.get("DATA_TYPE").equalsIgnoreCase("SQL Server")) {
				dbSettings.dbType = DbType.MSSQL;
				if (iniFile.get("USER_NAME").length() != 0) { // Not using windows authentication
					String[] parts = iniFile.get("USER_NAME").split("/");
					if (parts.length == 2) {
						dbSettings.user = parts[1];
						dbSettings.domain = parts[0];
					}
				}
			} else if (iniFile.get("DATA_TYPE").equalsIgnoreCase("PDW")) {
				dbSettings.dbType = DbType.PDW;
				if (iniFile.get("USER_NAME").length() != 0) { // Not using windows authentication
					String[] parts = iniFile.get("USER_NAME").split("/");
					if (parts.length == 2) {
						dbSettings.user = parts[1];
						dbSettings.domain = parts[0];
					}
				}
			} else if (iniFile.get("DATA_TYPE").equalsIgnoreCase("MS Access"))
				dbSettings.dbType = DbType.MSACCESS;
			else if (iniFile.get("DATA_TYPE").equalsIgnoreCase("Teradata"))
				dbSettings.dbType = DbType.TERADATA;
		}
		if (iniFile.get("TABLES_TO_SCAN").equalsIgnoreCase("*")) {
			try (RichConnection connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType)) {
				dbSettings.tables.addAll(connection.getTableNames(dbSettings.database));
			}
		} else {
			for (String table : iniFile.get("TABLES_TO_SCAN").split(",")) {
				if (dbSettings.dataType == DbSettings.CSVFILES)
					table = iniFile.get("WORKING_FOLDER") + "/" + table;
				dbSettings.tables.add(table);
			}
		}

		SourceDataScan sourceDataScan = new SourceDataScan();
		int maxRows = Integer.parseInt(iniFile.get("ROWS_PER_TABLE"));
		boolean scanValues = iniFile.get("SCAN_FIELD_VALUES").equalsIgnoreCase("yes");
		int minCellCount = Integer.parseInt(iniFile.get("MIN_CELL_COUNT"));
		int maxValues = Integer.parseInt(iniFile.get("MAX_DISTINCT_VALUES"));
		sourceDataScan.process(dbSettings, maxRows, scanValues, minCellCount, maxValues, iniFile.get("WORKING_FOLDER") + "/ScanReport.xlsx");
	}

	private JComponent createTabsPanel() {
		JTabbedPane tabbedPane = new JTabbedPane();

		JPanel locationPanel = createLocationsPanel();
		tabbedPane.addTab("Locations", null, locationPanel, "Specify the location of the source data and the working folder");

		JPanel scanPanel = createScanPanel();
		tabbedPane.addTab("Scan", null, scanPanel, "Create a scan of the source data");

		JPanel fakeDataPanel = createFakeDataPanel();
		tabbedPane.addTab("Fake data generation", null, fakeDataPanel, "Create fake data based on a scan report for development purposes");

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
				pickFolder();
			}
		});
		componentsToDisableWhenRunning.add(pickButton);
		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = 1;
		panel.add(folderPanel, c);

		JPanel sourcePanel = new JPanel();
		sourcePanel.setLayout(new GridLayout(0, 2));
		sourcePanel.setBorder(BorderFactory.createTitledBorder("Source data location"));
		sourcePanel.add(new JLabel("Data type"));
		sourceType = new JComboBox<String>(new String[] { "Delimited text files", "MySQL", "Oracle", "SQL Server", "PostgreSQL", "MS Access", "PDW", "Redshift", "Teradata" });
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

		JPanel testConnectionButtonPanel = new JPanel();
		testConnectionButtonPanel.setLayout(new BoxLayout(testConnectionButtonPanel, BoxLayout.X_AXIS));
		testConnectionButtonPanel.add(Box.createHorizontalGlue());

		JButton testConnectionButton = new JButton("Test connection");
		testConnectionButton.setBackground(new Color(151, 220, 141));
		testConnectionButton.setToolTipText("Test the connection");
		testConnectionButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				testConnection(getSourceDbSettings());
			}
		});
		componentsToDisableWhenRunning.add(testConnectionButton);
		testConnectionButtonPanel.add(testConnectionButton);

		c.gridx = 0;
		c.gridy = 2;
		c.gridwidth = 1;
		panel.add(testConnectionButtonPanel, c);

		return panel;
	}

	private JPanel createScanPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());

		JPanel tablePanel = new JPanel();
		tablePanel.setLayout(new BorderLayout());
		tablePanel.setBorder(new TitledBorder("Tables to scan"));
		tableList = new JList<String>();
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

		JPanel southPanel = new JPanel();
		southPanel.setLayout(new BoxLayout(southPanel, BoxLayout.Y_AXIS));

		JPanel scanOptionsPanel = new JPanel();
		scanOptionsPanel.setLayout(new BoxLayout(scanOptionsPanel, BoxLayout.X_AXIS));

		scanValueScan = new JCheckBox("Scan field values", true);
		scanValueScan.setToolTipText("Include a frequency count of field values in the scan report");
		scanValueScan.addChangeListener(new ChangeListener() {

			@Override
			public void stateChanged(ChangeEvent arg0) {
				scanMinCellCount.setEnabled(((JCheckBox) arg0.getSource()).isSelected());
				scanRowCount.setEnabled(((JCheckBox) arg0.getSource()).isSelected());
				scanValuesCount.setEnabled(((JCheckBox) arg0.getSource()).isSelected());
			}
		});
		scanOptionsPanel.add(scanValueScan);
		scanOptionsPanel.add(Box.createHorizontalGlue());

		scanOptionsPanel.add(new JLabel("Min cell count "));
		scanMinCellCount = new JSpinner();
		scanMinCellCount.setValue(5);
		scanMinCellCount.setToolTipText("Minimum frequency for a field value to be included in the report");
		scanOptionsPanel.add(scanMinCellCount);
		scanOptionsPanel.add(Box.createHorizontalGlue());

		scanOptionsPanel.add(new JLabel("Max distinct values "));
		scanValuesCount = new JComboBox<String>(new String[] { "100", "1,000", "10,000" });
		scanValuesCount.setSelectedIndex(1);
		scanValuesCount.setToolTipText("Maximum number of distinct values per field to be reported");
		scanOptionsPanel.add(scanValuesCount);
		scanOptionsPanel.add(Box.createHorizontalGlue());

		scanOptionsPanel.add(new JLabel("Rows per table "));
		scanRowCount = new JComboBox<String>(new String[] { "100,000", "500,000", "1 million", "all" });
		scanRowCount.setSelectedIndex(0);
		scanRowCount.setToolTipText("Maximum number of rows per table to be scanned for field values");
		scanOptionsPanel.add(scanRowCount);

		southPanel.add(scanOptionsPanel);

		southPanel.add(Box.createVerticalStrut(3));

		JPanel scanButtonPanel = new JPanel();
		scanButtonPanel.setLayout(new BoxLayout(scanButtonPanel, BoxLayout.X_AXIS));
		scanButtonPanel.add(Box.createHorizontalGlue());

		JButton scanButton = new JButton("Scan tables");
		scanButton.setBackground(new Color(151, 220, 141));
		scanButton.setToolTipText("Scan the selected tables");
		scanButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				scanRun();
			}
		});
		componentsToDisableWhenRunning.add(scanButton);
		scanButtonPanel.add(scanButton);
		southPanel.add(scanButtonPanel);

		panel.add(southPanel, BorderLayout.SOUTH);

		return panel;
	}

	private JPanel createFakeDataPanel() {
		JPanel panel = new JPanel();

		panel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.5;

		JPanel folderPanel = new JPanel();
		folderPanel.setLayout(new BoxLayout(folderPanel, BoxLayout.X_AXIS));
		folderPanel.setBorder(BorderFactory.createTitledBorder("Scan report file"));
		scanReportFileField = new JTextField();
		scanReportFileField.setText((new File("ScanReport.xlsx").getAbsolutePath()));
		scanReportFileField.setToolTipText("The path to the scan report that will be used as a template to generate the fake data");
		folderPanel.add(scanReportFileField);
		JButton pickButton = new JButton("Pick file");
		pickButton.setToolTipText("Pick a scan report file");
		folderPanel.add(pickButton);
		pickButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickScanReportFile();
			}
		});
		componentsToDisableWhenRunning.add(pickButton);
		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = 1;
		panel.add(folderPanel, c);

		JPanel targetPanel = new JPanel();
		targetPanel.setLayout(new GridLayout(0, 2));
		targetPanel.setBorder(BorderFactory.createTitledBorder("Target data location"));
		targetPanel.add(new JLabel("Data type"));
		targetType = new JComboBox<String>(new String[] { "Delimited text files", "MySQL", "Oracle", "SQL Server", "PostgreSQL" });
		// targetType = new JComboBox(new String[] { "Delimited text files", "MySQL" });
		targetType.setToolTipText("Select the type of source data available");
		targetType.addItemListener(new ItemListener() {

			@Override
			public void itemStateChanged(ItemEvent arg0) {
				targetIsFiles = arg0.getItem().toString().equals("Delimited text files");
				targetServerField.setEnabled(!targetIsFiles);
				targetUserField.setEnabled(!targetIsFiles);
				targetPasswordField.setEnabled(!targetIsFiles);
				targetDatabaseField.setEnabled(!targetIsFiles);
				targetCSVFormat.setEnabled(targetIsFiles);

				if (!targetIsFiles && arg0.getItem().toString().equals("Oracle")) {
					targetServerField
							.setToolTipText("For Oracle servers this field contains the SID, servicename, and optionally the port: '<host>/<sid>', '<host>:<port>/<sid>', '<host>/<service name>', or '<host>:<port>/<service name>'");
					targetUserField.setToolTipText("For Oracle servers this field contains the name of the user used to log in");
					targetPasswordField.setToolTipText("For Oracle servers this field contains the password corresponding to the user");
					targetDatabaseField
							.setToolTipText("For Oracle servers this field contains the schema (i.e. 'user' in Oracle terms) containing the source tables");
				} else if (!targetIsFiles && arg0.getItem().toString().equals("PostgreSQL")) {
					targetServerField.setToolTipText("For PostgreSQL servers this field contains the host name and database name (<host>/<database>)");
					targetUserField.setToolTipText("The user used to log in to the server");
					targetPasswordField.setToolTipText("The password used to log in to the server");
					targetDatabaseField.setToolTipText("For PostgreSQL servers this field contains the schema containing the source tables");
				} else if (!targetIsFiles) {
					targetServerField.setToolTipText("This field contains the name or IP address of the database server");
					if (arg0.getItem().toString().equals("SQL Server"))
						targetUserField
								.setToolTipText("The user used to log in to the server. Optionally, the domain can be specified as <domain>/<user> (e.g. 'MyDomain/Joe')");
					else
						targetUserField.setToolTipText("The user used to log in to the server");
					targetPasswordField.setToolTipText("The password used to log in to the server");
					targetDatabaseField.setToolTipText("The name of the database containing the source tables");
				}
			}
		});
		targetPanel.add(targetType);

		targetPanel.add(new JLabel("Server location"));
		targetServerField = new JTextField("127.0.0.1");
		targetServerField.setEnabled(false);
		targetPanel.add(targetServerField);
		targetPanel.add(new JLabel("User name"));
		targetUserField = new JTextField("");
		targetUserField.setEnabled(false);
		targetPanel.add(targetUserField);
		targetPanel.add(new JLabel("Password"));
		targetPasswordField = new JPasswordField("");
		targetPasswordField.setEnabled(false);
		targetPanel.add(targetPasswordField);
		targetPanel.add(new JLabel("Database name"));
		targetDatabaseField = new JTextField("");
		targetDatabaseField.setEnabled(false);
		targetPanel.add(targetDatabaseField);

		targetPanel.add(new JLabel("CSV Format"));
		targetCSVFormat = new JComboBox<>(
				new String[] { "Default (comma, CRLF)", "TDF (tab, CRLF)", "MySQL (tab, LF)", "RFC4180", "Excel CSV" });
		targetCSVFormat.setToolTipText("The format of the output");
		targetCSVFormat.setEnabled(true);
		targetPanel.add(targetCSVFormat);

		c.gridx = 0;
		c.gridy = 1;
		c.gridwidth = 1;
		panel.add(targetPanel, c);

		JPanel fakeDataButtonPanel = new JPanel();
		fakeDataButtonPanel.setLayout(new BoxLayout(fakeDataButtonPanel, BoxLayout.X_AXIS));

		fakeDataButtonPanel.add(new JLabel("Max rows per table"));
		generateRowCount = new JSpinner();
		generateRowCount.setValue(10000);
		fakeDataButtonPanel.add(generateRowCount);
		fakeDataButtonPanel.add(Box.createHorizontalGlue());

		JButton testConnectionButton = new JButton("Test connection");
		testConnectionButton.setBackground(new Color(151, 220, 141));
		testConnectionButton.setToolTipText("Test the connection");
		testConnectionButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				testConnection(getTargetDbSettings());
			}
		});
		componentsToDisableWhenRunning.add(testConnectionButton);
		fakeDataButtonPanel.add(testConnectionButton);

		JButton fakeDataButton = new JButton("Generate fake data");
		fakeDataButton.setBackground(new Color(151, 220, 141));
		fakeDataButton.setToolTipText("Generate fake data based on the scan report");
		fakeDataButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				fakeDataRun();
			}
		});
		componentsToDisableWhenRunning.add(fakeDataButton);
		fakeDataButtonPanel.add(fakeDataButton);

		c.gridx = 0;
		c.gridy = 2;
		c.gridwidth = 1;
		panel.add(fakeDataButtonPanel, c);

		return panel;
	}

	private JComponent createConsolePanel() {
		JTextArea consoleArea = new JTextArea();
		consoleArea.setToolTipText("General progress information");
		consoleArea.setEditable(false);
		Console console = new Console();
		console.setTextArea(consoleArea);
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

	private void pickFolder() {
		JFileChooser fileChooser = new JFileChooser(new File(folderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			folderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}

	private void pickScanReportFile() {
		JFileChooser fileChooser = new JFileChooser(new File(folderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select scan report file");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			scanReportFileField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}

	private void removeTables() {
		for (String item : tableList.getSelectedValuesList()) {
			tables.remove(item);
			tableList.setListData(tables);
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
				tableList.setListData(tables);
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
						tableList.setListData(tables);
					}

				}
			} else if (sourceDbSettings.dataType == DbSettings.DATABASE) {
				RichConnection connection = new RichConnection(sourceDbSettings.server, sourceDbSettings.domain, sourceDbSettings.user,
						sourceDbSettings.password, sourceDbSettings.dbType);
				String tableNames = StringUtilities.join(connection.getTableNames(sourceDbSettings.database), "\t");
				if (tableNames.length() == 0) {
					JOptionPane.showMessageDialog(frame, "No tables found in database " + sourceDbSettings.database, "Error fetching table names",
							JOptionPane.ERROR_MESSAGE);
				} else {
					DBTableSelectionDialog selectionDialog = new DBTableSelectionDialog(frame, true, tableNames);
					if (selectionDialog.getAnswer()) {
						for (Object item : selectionDialog.getSelectedItems()) {
							if (!tables.contains(item))
								tables.add((String) item);
							tableList.setListData(tables);
						}
					}
				}
				connection.close();
			}
		}
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
			else if (sourceType.getSelectedItem().toString().equals("Redshift"))
				dbSettings.dbType = DbType.REDSHIFT;
			else if (sourceType.getSelectedItem().toString().equals("SQL Server")) {
				dbSettings.dbType = DbType.MSSQL;
				if (sourceUserField.getText().length() != 0) { // Not using windows authentication
					String[] parts = sourceUserField.getText().split("/");
					if (parts.length == 2) {
						dbSettings.user = parts[1];
						dbSettings.domain = parts[0];
					}
				}
			} if (sourceType.getSelectedItem().toString().equals("PDW")) {
				dbSettings.dbType = DbType.PDW;
				if (sourceUserField.getText().length() != 0) { // Not using windows authentication
					String[] parts = sourceUserField.getText().split("/");
					if (parts.length == 2) {
						dbSettings.user = parts[1];
						dbSettings.domain = parts[0];
					}
				}
			} else if (sourceType.getSelectedItem().toString().equals("MS Access"))
				dbSettings.dbType = DbType.MSACCESS;
			else if (sourceType.getSelectedItem().toString().equals("Teradata"))
				dbSettings.dbType = DbType.TERADATA;
		}
		return dbSettings;
	}

	private void testConnection(DbSettings dbSettings) {
		if (dbSettings.dataType == DbSettings.CSVFILES) {
			if (new File(folderField.getText()).exists()) {
				String message = "Folder " + folderField.getText() + " found";
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Working folder found", JOptionPane.INFORMATION_MESSAGE);
			} else {
				String message = "Folder " + folderField.getText() + " not found";
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Working folder not found", JOptionPane.ERROR_MESSAGE);
			}
		} else {
			if (dbSettings.database == null || dbSettings.database.equals("")) {
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap("Please specify database name", 80), "Error connecting to server",
						JOptionPane.ERROR_MESSAGE);
				return;
			}
			if (dbSettings.server == null || dbSettings.server.equals("")) {
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap("Please specify the server", 80), "Error connecting to server",
						JOptionPane.ERROR_MESSAGE);
				return;
			}

			RichConnection connection;
			try {
				connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
			} catch (Exception e) {
				String message = "Could not connect: " + e.getMessage();
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
				return;
			}

			try {
				List<String> tableNames = connection.getTableNames(dbSettings.database);
				if (tableNames.size() == 0)
					throw new RuntimeException("Unable to retrieve table names for database " + dbSettings.database);
			} catch (Exception e) {
				String message = "Could not connect to database: " + e.getMessage();
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
				return;
			}

			connection.close();
			String message = "Succesfully connected to " + dbSettings.database + " on server " + dbSettings.server;
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Connection succesful", JOptionPane.INFORMATION_MESSAGE);

		}
	}

	private DbSettings getTargetDbSettings() {
		DbSettings dbSettings = new DbSettings();
		if (targetType.getSelectedItem().equals("Delimited text files")) {
			dbSettings.dataType = DbSettings.CSVFILES;

			switch((String) targetCSVFormat.getSelectedItem()) {
				case "Default (comma, CRLF)":
					dbSettings.csvFormat = CSVFormat.DEFAULT;
					break;
				case "RFC4180":
					dbSettings.csvFormat = CSVFormat.RFC4180;
					break;
				case "Excel CSV":
					dbSettings.csvFormat = CSVFormat.EXCEL;
					break;
				case "TDF (tab, CRLF)":
					dbSettings.csvFormat = CSVFormat.TDF;
					break;
				case "MySQL (tab, LF)":
					dbSettings.csvFormat = CSVFormat.MYSQL;
					break;
				default:
					dbSettings.csvFormat = CSVFormat.RFC4180;
			}

		} else {
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
					if (parts.length == 2) {
						dbSettings.user = parts[1];
						dbSettings.domain = parts[0];
					}
				}
			} else if (sourceType.getSelectedItem().toString().equals("PDW")) {
				dbSettings.dbType = DbType.PDW;
				if (sourceUserField.getText().length() != 0) { // Not using windows authentication
					String[] parts = sourceUserField.getText().split("/");
					if (parts.length == 2) {
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
		}
		return dbSettings;
	}

	private void scanRun() {
		if (tables.size() == 0) {
			if (sourceIsFiles) {
				String message = "No files selected for scanning";
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "No files selected", JOptionPane.ERROR_MESSAGE);
				return;
			} else {
				String message = "No tables were selected for scanning. Do you want to select all tables in the database for scanning?";
				String title = "No tables selected";
				int answer = JOptionPane.showConfirmDialog(ObjectExchange.frame, message, title, JOptionPane.YES_NO_OPTION);
				if (answer == JOptionPane.YES_OPTION) {
					addAllTables();
				} else
					return;
			}
		}
		int rowCount = 0;
		if (scanRowCount.getSelectedItem().toString().equals("100,000"))
			rowCount = 100000;
		else if (scanRowCount.getSelectedItem().toString().equals("500,000"))
			rowCount = 500000;
		else if (scanRowCount.getSelectedItem().toString().equals("1 million"))
			rowCount = 1000000;
		if (scanRowCount.getSelectedItem().toString().equals("all"))
			rowCount = -1;

		int valuesCount = 0;
		if (scanValuesCount.getSelectedItem().toString().equals("100"))
			valuesCount = 100;
		else if (scanValuesCount.getSelectedItem().toString().equals("1,000"))
			valuesCount = 1000;
		else if (scanValuesCount.getSelectedItem().toString().equals("10,000"))
			valuesCount = 10000;

		ScanThread scanThread = new ScanThread(rowCount, valuesCount, scanValueScan.isSelected(), Integer.parseInt(scanMinCellCount.getValue().toString()));
		scanThread.start();
	}

	private void fakeDataRun() {
		String filename = scanReportFileField.getText();
		if (!new File(filename).exists()) {
			String message = "File " + filename + " not found";
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "File not found", JOptionPane.ERROR_MESSAGE);
		} else {
			FakeDataThread thread = new FakeDataThread(Integer.parseInt(generateRowCount.getValue().toString()), filename);
			thread.start();
		}
	}

	private class ScanThread extends Thread {

		private int		maxRows;
		private int		maxValues;
		private boolean	scanValues;
		private int		minCellCount;

		public ScanThread(int maxRows, int maxValues, boolean scanValues, int minCellCount) {
			this.maxRows = maxRows;
			this.scanValues = scanValues;
			this.minCellCount = minCellCount;
			this.maxValues = maxValues;
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
					sourceDataScan.process(dbSettings, maxRows, scanValues, minCellCount, maxValues, folderField.getText() + "/ScanReport.xlsx");
				}
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}

	}

	private class FakeDataThread extends Thread {
		private int		maxRowCount;
		private String	filename;

		public FakeDataThread(int maxRowCount, String filename) {
			this.maxRowCount = maxRowCount;
			this.filename = filename;
		}

		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				FakeDataGenerator process = new FakeDataGenerator();
				DbSettings dbSettings = getTargetDbSettings();
				if (dbSettings != null)
					process.generateData(dbSettings, maxRowCount, filename, folderField.getText());
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}

		}
	}

	private class DBTableSelectionDialog extends JDialog implements ActionListener {
		private static final long	serialVersionUID	= 4527207331482143091L;
		private JButton				yesButton			= null;
		private JButton				noButton			= null;
		private boolean				answer				= false;
		private JList<String>		list;

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

			list = new JList<String>(tableNames.split("\t"));
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

		public List<String> getSelectedItems() {
			return list.getSelectedValuesList();
		}
	}

	@Override
	public void actionPerformed(ActionEvent event) {
		switch (event.getActionCommand()) {
			case ACTION_CMD_HELP:
				doOpenWiki();
				break;

		}
	}

	private void doOpenWiki() {
		try {
			Desktop desktop = Desktop.getDesktop();
			desktop.browse(new URI(WIKI_URL));
		} catch (URISyntaxException | IOException ex) {

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

	private JMenuBar createMenuBar() {
		JMenuBar menuBar = new JMenuBar();
		JMenu helpMenu = new JMenu("Help");
		menuBar.add(helpMenu);
		JMenuItem helpItem = new JMenuItem(ACTION_CMD_HELP);
		helpItem.addActionListener(this);
		helpItem.setActionCommand(ACTION_CMD_HELP);
		helpMenu.add(helpItem);

		return menuBar;
	}

}
