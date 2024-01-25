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
package org.ohdsi.whiterabbit;

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
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.output.TeeOutputStream;
import org.ohdsi.databases.*;
import org.ohdsi.databases.configuration.*;
import org.ohdsi.utilities.DirectoryUtilities;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.Version;
import org.ohdsi.utilities.files.IniFile;
import org.ohdsi.whiterabbit.fakeDataGenerator.FakeDataGenerator;
import org.ohdsi.whiterabbit.gui.LocationsPanel;
import org.ohdsi.whiterabbit.scan.SourceDataScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the WhiteRabbit main class
 */
public class WhiteRabbitMain implements ActionListener, PanelsManager {

	Logger logger = LoggerFactory.getLogger(WhiteRabbitMain.class);

	public static final String DOCUMENTATION_URL = "http://ohdsi.github.io/WhiteRabbit";
	public static final String ACTION_CMD_HELP = "Open documentation";

	public static final String DELIMITED_TEXT_FILES = DbType.DELIMITED_TEXT_FILES.label();

	public static final String LABEL_TEST_CONNECTION = "Test connection";
	public static final String LABEL_CONNECTION_SUCCESSFUL = "Connection successful";
	public static final String NAME_TABBED_PANE = "TabbedPane";
	public static final String LABEL_LOCATIONS = "Locations";
	public static final String LABEL_SCAN = "Scan";
	public static final String LABEL_SCAN_TABLES = "Scan tables";

	public static final String LABEL_ADD_ALL_IN_DB = "Add all in DB";

	public static final String TITLE_ERRORS_IN_DATABASE_CONFIGURATION = "There are errors in the database configuration";
	public static final String TITLE_WARNINGS_ABOUT_DATABASE_CONFIGURATION = "There are warnings about the database configuration";
	public static final String NAME_CHECKBOX_CALC_NUMERIC_STATS = "CheckboxCalcNumericStats";
	public static final String NAME_STATS_SAMPLE_SIZE = "StatsSampleSize";

	private JFrame				frame;
	private JTextField			scanReportFileField;

	private JComboBox<String>	scanRowCount;
	private JComboBox<String>	scanValuesCount;
	private JCheckBox			scanValueScan;
	private JCheckBox 			calculateNumericStats;
	private JComboBox<String>	numericStatsSampleSize;
	private JSpinner			scanMinCellCount;
	private JSpinner			generateRowCount;
	private JComboBox<String>	targetType;
	private JTextField			targetUserField;
	private JTextField			targetPasswordField;
	private JTextField			targetServerField;
	private JTextField			targetDatabaseField;
	private JComboBox<String> 	targetCSVFormat;
	private JCheckBox		 	doUniformSampling;
	private JButton				addAllButton;
	private JList<String>		tableList;
	private Vector<String>		tables							= new Vector<String>();
	private boolean				targetIsFiles					= false;
	private LocationsPanel locationsPanel;

	private Console	console;

	private boolean				teeOutputStreams; // for testing/debugging purposes

	private List<JComponent>	componentsToDisableWhenRunning	= new ArrayList<JComponent>();

	public String reportFilePath = "";

	public static void main(String[] args) throws IOException {
		new WhiteRabbitMain(false, args);
	}

	public WhiteRabbitMain(boolean teeOutputStreams, String[] args) throws IOException {
		this.teeOutputStreams = teeOutputStreams;
		if (args.length == 2 && (args[0].equalsIgnoreCase("-ini") || args[0].equalsIgnoreCase("--ini")))
			launchCommandLine(args[1]);
		else {
			frame = new JFrame("White Rabbit");

			frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

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

	public JButton getAddAllButton() {
		return this.addAllButton;
	}

	public List<JComponent> getComponentsToDisableWhenRunning() {
		return this.componentsToDisableWhenRunning;
	}

	private void launchCommandLine(String iniFileName) throws IOException {
		IniFile iniFile = new IniFile(iniFileName);
		DbSettings dbSettings = getDbSettings(iniFile);
		findTablesToScan(iniFile, dbSettings);
		performSourceDataScan(iniFile, dbSettings);
	}

	private DbSettings getDbSettings(IniFile iniFile) {
		DbSettings dbSettings;

		DbType dbType = DbType.getDbType(iniFile.getDataType());
		if (dbType.supportsStorageHandler()) {
			dbSettings = dbType.getStorageHandler().getDbSettings(iniFile, null, System.out);
		} else {
			dbSettings = new DbSettings();
			if (iniFile.get(DBConfiguration.DATA_TYPE_FIELD).equalsIgnoreCase(DELIMITED_TEXT_FILES)) {
				dbSettings.sourceType = DbSettings.SourceType.CSV_FILES;
				if (iniFile.get("DELIMITER").equalsIgnoreCase("tab"))
					dbSettings.delimiter = '\t';
				else
					dbSettings.delimiter = iniFile.get("DELIMITER").charAt(0);
			} else if (iniFile.get(DBConfiguration.DATA_TYPE_FIELD).equalsIgnoreCase(DbType.SAS7BDAT.label())) {
				dbSettings.sourceType = DbSettings.SourceType.SAS_FILES;
			} else {
				dbSettings.sourceType = DbSettings.SourceType.DATABASE;
				dbSettings.user = iniFile.get("USER_NAME");
				dbSettings.password = iniFile.get("PASSWORD");
				dbSettings.server = iniFile.get("SERVER_LOCATION");
				dbSettings.database = iniFile.get("DATABASE_NAME");
				dbSettings.dbType = dbType;
				if (dbType == DbType.SQL_SERVER) {
					if (!iniFile.get("USER_NAME").isEmpty()) { // Not using windows authentication
						String[] parts = iniFile.get("USER_NAME").split("/");
						if (parts.length == 2) {
							dbSettings.user = parts[1];
							dbSettings.domain = parts[0];
						}
					}
				} else if (dbType == DbType.AZURE) {
					if (!iniFile.get("USER_NAME").isEmpty()) { // Not using windows authentication
						String[] parts = iniFile.get("USER_NAME").split("/");
						if (parts.length == 2) {
							dbSettings.user = parts[1];
							dbSettings.domain = parts[0];
						}
					}
				} else if (dbType == DbType.PDW) {
					if (!iniFile.get("USER_NAME").isEmpty()) { // Not using windows authentication
						String[] parts = iniFile.get("USER_NAME").split("/");
						if (parts.length == 2) {
							dbSettings.user = parts[1];
							dbSettings.domain = parts[0];
						}
					}
				} else if (dbType == DbType.BIGQUERY) {
					/* GBQ requires database. Putting database into domain var for connect() */
					dbSettings.domain = dbSettings.database;
				}
			}
		}

		return dbSettings;
	}

	private void findTablesToScan(IniFile iniFile, DbSettings dbSettings) {
		if (iniFile.get("TABLES_TO_SCAN").equalsIgnoreCase("*")) {
			if (dbSettings.sourceType == DbSettings.SourceType.DATABASE) {
				try (RichConnection connection = new RichConnection(dbSettings)) {
					dbSettings.tables.addAll(connection.getTableNames(dbSettings.database));
				}
			} else {
				String extension;
				if (dbSettings.sourceType == DbSettings.SourceType.CSV_FILES) {
					extension = ".csv";
				} else {
					extension = ".sas7bdat";
				}
				File folder = new File(iniFile.get("WORKING_FOLDER"));
				if (folder.isDirectory()) {
					for (File file : folder.listFiles()) {
						if (file.isFile()) {
							String filename = file.getAbsolutePath();
							if (filename.endsWith(extension)) {
								dbSettings.tables.add(filename);
							}
						}
					}
				}
			}
		} else {
			for (String table : iniFile.get("TABLES_TO_SCAN").split(",")) {
				if (dbSettings.sourceType == DbSettings.SourceType.CSV_FILES)
					table = iniFile.get("WORKING_FOLDER") + "/" + table;
				dbSettings.tables.add(table);
			}
		}
	}

	private void performSourceDataScan(IniFile iniFile, DbSettings dbSettings) throws IOException {
		SourceDataScan sourceDataScan = new SourceDataScan();
		int maxRows = Integer.parseInt(iniFile.get("ROWS_PER_TABLE"));
		boolean scanValues = iniFile.get("SCAN_FIELD_VALUES").equalsIgnoreCase("yes");
		int minCellCount = 0;
		int maxValues = 0;
		boolean calculateNumericStats = false;
		int numericStatsSamplerSize = 0;
		if (scanValues) {
			minCellCount = Integer.parseInt(iniFile.get("MIN_CELL_COUNT"));
			maxValues = Integer.parseInt(iniFile.get("MAX_DISTINCT_VALUES"));
			calculateNumericStats = iniFile.get("CALCULATE_NUMERIC_STATS").equalsIgnoreCase("yes");
			if (calculateNumericStats) {
				numericStatsSamplerSize = Integer.parseInt(iniFile.get("NUMERIC_STATS_SAMPLER_SIZE"));
			}
		}

		sourceDataScan.setSampleSize(maxRows);
		sourceDataScan.setScanValues(scanValues);
		sourceDataScan.setMinCellCount(minCellCount);
		sourceDataScan.setMaxValues(maxValues);
		sourceDataScan.setCalculateNumericStats(calculateNumericStats);
		sourceDataScan.setNumStatsSamplerSize(numericStatsSamplerSize);
		reportFilePath = iniFile.get("WORKING_FOLDER") + "/" + SourceDataScan.SCAN_REPORT_FILE_NAME;
		sourceDataScan.process(dbSettings, reportFilePath);
	}

	private JComponent createTabsPanel() {
		JTabbedPane tabbedPane = new JTabbedPane();
		tabbedPane.setName(NAME_TABBED_PANE);

		this.locationsPanel = createLocationsPanel(componentsToDisableWhenRunning);
		tabbedPane.addTab(LABEL_LOCATIONS, null, locationsPanel, "Specify the location of the source data and the working folder");

		JPanel scanPanel = createScanPanel();
		tabbedPane.addTab(LABEL_SCAN, null, scanPanel, "Create a scan of the source data");

		JPanel fakeDataPanel = createFakeDataPanel();
		tabbedPane.addTab("Fake data generation", null, fakeDataPanel, "Create fake data based on a scan report for development purposes");

		return tabbedPane;
	}

	private LocationsPanel createLocationsPanel(List<JComponent> componentsToDisableWhenRunning) {
		return new LocationsPanel(frame, this);
	}
	
	private JPanel createScanPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new BorderLayout());

		JPanel tablePanel = new JPanel();
		tablePanel.setLayout(new BorderLayout());
		tablePanel.setBorder(new TitledBorder("Tables to scan"));
		tableList = new JList<String>();
		tableList.setName("TableList");
		tableList.setToolTipText("Specify the tables (or CSV files) to be scanned here");
		tablePanel.add(new JScrollPane(tableList), BorderLayout.CENTER);

		JPanel tableButtonPanel = new JPanel();
		tableButtonPanel.setLayout(new GridLayout(3, 1));
		addAllButton = new JButton(LABEL_ADD_ALL_IN_DB);
		addAllButton.setName(LABEL_ADD_ALL_IN_DB);
		addAllButton.setToolTipText("Add all tables in the database");
		addAllButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addAllTables();
			}
		});
		addAllButton.setEnabled(false);
		tableButtonPanel.add(addAllButton);
		JButton addButton = new JButton("Add");
		addButton.setName("Add");
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

		JPanel scanOptionsTopPanel = new JPanel();
		scanOptionsTopPanel.setLayout(new BoxLayout(scanOptionsTopPanel, BoxLayout.X_AXIS));

		scanValueScan = new JCheckBox("Scan field values", true);
		scanValueScan.setToolTipText("Include a frequency count of field values in the scan report");
		scanValueScan.addChangeListener(event -> {
			scanMinCellCount.setEnabled(((JCheckBox) event.getSource()).isSelected());
			scanRowCount.setEnabled(((JCheckBox) event.getSource()).isSelected());
			scanValuesCount.setEnabled(((JCheckBox) event.getSource()).isSelected());
			calculateNumericStats.setEnabled(((JCheckBox) event.getSource()).isSelected());
			numericStatsSampleSize.setEnabled(((JCheckBox) event.getSource()).isSelected());
		});
		scanOptionsTopPanel.add(scanValueScan);
		scanOptionsTopPanel.add(Box.createHorizontalGlue());

		scanOptionsTopPanel.add(new JLabel("Min cell count "));
		scanMinCellCount = new JSpinner();
		scanMinCellCount.setValue(5);
		scanMinCellCount.setToolTipText("Minimum frequency for a field value to be included in the report");
		scanOptionsTopPanel.add(scanMinCellCount);
		scanOptionsTopPanel.add(Box.createHorizontalGlue());

		scanOptionsTopPanel.add(new JLabel("Max distinct values "));
		scanValuesCount = new JComboBox<>(new String[] { "100", "1,000", "10,000", "100,000" });
		scanValuesCount.setSelectedIndex(1);
		scanValuesCount.setToolTipText("Maximum number of distinct values per field to be reported");
		scanOptionsTopPanel.add(scanValuesCount);
		scanOptionsTopPanel.add(Box.createHorizontalGlue());

		scanOptionsTopPanel.add(new JLabel("Rows per table "));
		scanRowCount = new JComboBox<>(new String[] { "100,000", "500,000", "1 million", "all" });
		scanRowCount.setSelectedIndex(0);
		scanRowCount.setToolTipText("Maximum number of rows per table to be scanned for field values");
		scanOptionsTopPanel.add(scanRowCount);

		southPanel.add(scanOptionsTopPanel);

		JPanel scanOptionsLowerPanel = new JPanel();
		scanOptionsLowerPanel.setLayout(new BoxLayout(scanOptionsLowerPanel, BoxLayout.X_AXIS));

		calculateNumericStats = new JCheckBox("Numeric stats", false);
		calculateNumericStats.setName(NAME_CHECKBOX_CALC_NUMERIC_STATS);
		calculateNumericStats.setToolTipText("Include average, standard deviation and quartiles of numeric fields");
		calculateNumericStats.addChangeListener(event -> numericStatsSampleSize.setEnabled(((JCheckBox) event.getSource()).isSelected()));
		scanOptionsLowerPanel.add(calculateNumericStats);
		scanOptionsLowerPanel.add(Box.createHorizontalGlue());

		scanOptionsLowerPanel.add(new JLabel("Numeric stats reservoir size: "));
		numericStatsSampleSize = new JComboBox<>(new String[] { "100,000", "500,000", "1 million" });
		numericStatsSampleSize.setName(NAME_STATS_SAMPLE_SIZE);
		numericStatsSampleSize.setSelectedIndex(0);
		numericStatsSampleSize.setToolTipText("Maximum number of rows used to calculate numeric statistics");
		scanOptionsLowerPanel.add(numericStatsSampleSize);
		scanOptionsLowerPanel.add(Box.createHorizontalGlue());

		southPanel.add(scanOptionsLowerPanel);

		southPanel.add(Box.createVerticalStrut(3));

		JButton scanButton = new JButton(LABEL_SCAN_TABLES);
		scanButton.setName(LABEL_SCAN_TABLES);
		scanButton.setBackground(new Color(151, 220, 141));
		scanButton.setToolTipText("Scan the selected tables");
		scanButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				scanRun();
			}
		});
		componentsToDisableWhenRunning.add(scanButton);
		scanOptionsLowerPanel.add(scanButton);

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
		scanReportFileField.setText((new File(SourceDataScan.SCAN_REPORT_FILE_NAME).getAbsolutePath()));
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
		targetType = new JComboBox<>(new String[] {DELIMITED_TEXT_FILES, "MySQL", "Oracle", "SQL Server", "PostgreSQL", "PDW"});
		targetType.setToolTipText("Select the type of source data available");
		targetType.addItemListener(event -> {
			targetIsFiles = event.getItem().toString().equals(DELIMITED_TEXT_FILES);
			targetServerField.setEnabled(!targetIsFiles);
			targetUserField.setEnabled(!targetIsFiles);
			targetPasswordField.setEnabled(!targetIsFiles);
			targetDatabaseField.setEnabled(!targetIsFiles);
			targetCSVFormat.setEnabled(targetIsFiles);

			if (targetIsFiles) {
				return;
			}
			// Default tooltips, to be overridden per database if necessary
			targetServerField.setToolTipText("The name or IP address of the database server");
			targetUserField.setToolTipText("The user used to log in to the server");
			targetPasswordField.setToolTipText("The password used to log in to the server");
			targetDatabaseField.setToolTipText("The name of the database containing the source tables");

			switch (event.getItem().toString()) {
				case "Oracle":
					targetServerField.setToolTipText("For Oracle servers this field contains the SID, servicename, and optionally the port: '<host>/<sid>', '<host>:<port>/<sid>', '<host>/<service name>', or '<host>:<port>/<service name>'");
					targetDatabaseField.setToolTipText("For Oracle servers this field contains the schema (i.e. 'user' in Oracle terms) containing the source tables");
					break;
				case "PostgreSQL":
					targetServerField.setToolTipText("For PostgreSQL servers this field contains the host name and database name (<host>/<database>)");
					targetDatabaseField.setToolTipText("For PostgreSQL servers this field contains the *schema* containing the source tables");
					break;
				case "BigQuery":
					targetServerField.setToolTipText("GBQ ProjectID");
					targetUserField.setToolTipText("GBQ OAuthServiceAccountEmail");
					targetPasswordField.setToolTipText("GBQ OAuthPvtKeyPath");
					targetDatabaseField.setToolTipText("GBQ Data Set within ProjectID");
					break;
				case "SQL Server":
					targetUserField.setToolTipText("The user used to log in to the server. Optionally, the domain can be specified as <domain>/<user> (e.g. 'MyDomain/Joe')");
					break;
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

		targetPanel.add(new JLabel(""));
		doUniformSampling = new JCheckBox("Uniform Sampling", false);
		doUniformSampling.setToolTipText("For all fields, choose every possible value with the same probability");
		doUniformSampling.setEnabled(true);
		targetPanel.add(doUniformSampling);

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

		JButton testConnectionButton = new JButton(LABEL_TEST_CONNECTION);
		testConnectionButton.setName(LABEL_TEST_CONNECTION);
		testConnectionButton.setBackground(new Color(151, 220, 141));
		testConnectionButton.setToolTipText("Test the connection");
		testConnectionButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				testConnection(getTargetDbSettings(), null);
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
		consoleArea.setName("Console");
		consoleArea.setToolTipText("General progress information");
		consoleArea.setEditable(false);
		this.console = new Console();
		console.setTextArea(consoleArea);
		setOutputStreamsToConsole(console);
		JScrollPane consoleScrollPane = new JScrollPane(consoleArea);
		consoleScrollPane.setBorder(BorderFactory.createTitledBorder("Console"));
		consoleScrollPane.setPreferredSize(new Dimension(800, 180));
		consoleScrollPane.setAutoscrolls(true);
		ObjectExchange.console = console;
		return consoleScrollPane;
	}

	private void setOutputStreamsToConsole(Console console) {
		if (teeOutputStreams) {
			System.setOut(new PrintStream(new TeeOutputStream(System.out, new PrintStream(console))));
			System.setErr(new PrintStream(new TeeOutputStream(System.err, new PrintStream(console))));
		} else {
			System.setOut(new PrintStream(console));
			System.setErr(new PrintStream(console));
		}

		Thread resetOutputStreams = new Thread(() -> {
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
			System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err)));
			logger = LoggerFactory.getLogger(WhiteRabbitMain.class);
		});
		Runtime.getRuntime().addShutdownHook(resetOutputStreams);
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


	private void pickScanReportFile() {
		JFileChooser fileChooser = new JFileChooser(new File(locationsPanel.getFolderField().getText()));
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
		DbSettings dbSettings = getSourceDbSettings(null);
		if (dbSettings != null) {
			RichConnection connection = new RichConnection(dbSettings);
			for (String table : connection.getTableNames(dbSettings.database)) {
				if (!tables.contains(table))
					tables.add(table);
				tableList.setListData(tables);
			}
			connection.close();
		}
	}

	private void pickTables() {
		DbSettings sourceDbSettings = getSourceDbSettings(null);
		if (sourceDbSettings != null) {
			if (sourceDbSettings.sourceType == DbSettings.SourceType.CSV_FILES || sourceDbSettings.sourceType == DbSettings.SourceType.SAS_FILES) {
				JFileChooser fileChooser = getjFileChooser(sourceDbSettings, locationsPanel);

				int returnVal = fileChooser.showDialog(frame, "Select tables");
				if (returnVal == JFileChooser.APPROVE_OPTION) {
					for (File table : fileChooser.getSelectedFiles()) {
						String tableName = DirectoryUtilities.getRelativePath(new File(locationsPanel.getFolderField().getText()), table);
						if (!tables.contains(tableName))
							tables.add(tableName);
						tableList.setListData(tables);
					}

				}
			} else if (sourceDbSettings.sourceType == DbSettings.SourceType.DATABASE) {
				RichConnection connection = new RichConnection(sourceDbSettings);
				String tableNames = StringUtilities.join(connection.getTableNames(sourceDbSettings.database), "\t");
				if (tableNames.isEmpty()) {
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

	private static JFileChooser getjFileChooser(DbSettings sourceDbSettings, LocationsPanel locationsPanel) {
		JFileChooser fileChooser = new JFileChooser(new File(locationsPanel.getFolderField().getText()));
		fileChooser.setName("FileChooser");
		fileChooser.setMultiSelectionEnabled(true);
		fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);

		if (sourceDbSettings.sourceType == DbSettings.SourceType.CSV_FILES) {
			fileChooser.setFileFilter(new FileNameExtensionFilter(DELIMITED_TEXT_FILES, "csv", "txt"));
		} else if (sourceDbSettings.sourceType == DbSettings.SourceType.SAS_FILES) {
			fileChooser.setFileFilter(new FileNameExtensionFilter("SAS Data Files", DbType.SAS7BDAT.name().toLowerCase()));
		}
		return fileChooser;
	}

	private DbSettings getSourceDbSettings(ValidationFeedback feedback) {
        DbType dbChoice = locationsPanel.getCurrentDbChoice();
        DbSettings dbSettings;
        if (dbChoice != null && dbChoice.supportsStorageHandler()) {
            dbSettings = locationsPanel.getCurrentDbChoice().getStorageHandler().getDbSettings(feedback);
			return dbSettings;
        } else {
            String sourceDelimiterField = locationsPanel.getSourceDelimiterField().getText();
            String sourceType = locationsPanel.getSelectedSourceType();
            dbSettings = new DbSettings();
            if (sourceType.equals(DbType.DELIMITED_TEXT_FILES.label())) {
                dbSettings.dbType = DbType.DELIMITED_TEXT_FILES;
                dbSettings.sourceType = DbSettings.SourceType.CSV_FILES;
                if (sourceDelimiterField.isEmpty()) {
                    JOptionPane.showMessageDialog(frame, "Delimiter field cannot be empty for source database", "Error connecting to server",
                            JOptionPane.ERROR_MESSAGE);
                    return null;
                }
                if (sourceDelimiterField.equalsIgnoreCase("tab"))
                    dbSettings.delimiter = '\t';
                else
                    dbSettings.delimiter = locationsPanel.getSourceDelimiterField().getText().charAt(0);
            } else if (sourceType.equalsIgnoreCase(DbType.SAS7BDAT.label())) {
                dbSettings.sourceType = DbSettings.SourceType.SAS_FILES;
                dbSettings.dbType = DbType.SAS7BDAT;
            } else {
                dbSettings.sourceType = DbSettings.SourceType.DATABASE;
                dbSettings.user = locationsPanel.getSourceUserField();
                dbSettings.password = locationsPanel.getSourcePasswordField();
                dbSettings.server = locationsPanel.getSourceServerField();
                String sourceDatabaseField = locationsPanel.getSourceDatabaseField();
                dbSettings.database = sourceDatabaseField.trim().isEmpty() ? null : sourceDatabaseField;
				dbSettings.dbType = dbChoice;
                if (dbChoice == DbType.SQL_SERVER) {
                    if (!dbSettings.user.isEmpty()) { // Not using windows authentication
                        String[] parts = dbSettings.user.split("/");
                        if (parts.length == 2) {
                            dbSettings.user = parts[1];
                            dbSettings.domain = parts[0];
                        }
                    }
                } else if (dbChoice == DbType.PDW) {
					if (!dbSettings.user.isEmpty()) { // Not using windows authentication
						String[] parts = dbSettings.user.split("/");
						if (parts.length == 2) {
							dbSettings.user = parts[1];
							dbSettings.domain = parts[0];
						}
					}
				} else if (dbChoice == DbType.AZURE) {
                    dbSettings.database = "";
                }
            }
            return dbSettings;
        }
    }

	public void runConnectionTest() {
		ValidationFeedback feedback = new ValidationFeedback();
		DbSettings dbSettings = getSourceDbSettings(feedback);
		if (dbSettings != null) {
			testConnection(dbSettings, feedback);
		} else {
			throw new DBConfigurationException("Source database settings were not initialized");
		}
	}

	private void testConnection(DbSettings dbSettings, ValidationFeedback feedback) {
		if (feedbackBlocksContinuation(feedback)) {
			return;
		}
		String folder = locationsPanel.getFolderField().getText();
		if (dbSettings.sourceType == DbSettings.SourceType.CSV_FILES || dbSettings.sourceType == DbSettings.SourceType.SAS_FILES) {
			if (new File(folder).exists()) {
				String message = "Folder " + folder + " found";
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Working folder found", JOptionPane.INFORMATION_MESSAGE);
			} else {
				String message = "Folder " + folder + " not found";
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Working folder not found", JOptionPane.ERROR_MESSAGE);
			}
		} else {
			if (locationsPanel.isSourceDatabaseFieldEnabled() && (dbSettings.database == null || dbSettings.database.equals(""))) {
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
			    connection = new RichConnection(dbSettings);
			} catch (Exception e) {
				String message = "Could not connect: " + e.getMessage();
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
				return;
			}
			try {
				List<String> tableNames = connection.getTableNames(dbSettings.database);
				if (tableNames.isEmpty())
					throw new RuntimeException("Unable to retrieve table names for database " + dbSettings.database);
			} catch (Exception e) {
				String message = "Could not connect to database: " + e.getMessage();
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
				return;
			}

			connection.close();
			String message = "Successfully connected to " + dbSettings.database + " on server " + dbSettings.server;
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), LABEL_CONNECTION_SUCCESSFUL, JOptionPane.INFORMATION_MESSAGE);
		}
	}

	private boolean feedbackBlocksContinuation(ValidationFeedback feedback) {
		if (feedback == null || (!feedback.hasWarnings() && !feedback.hasErrors())) {
			return false;
		} else {
			if (feedback.hasErrors()) {
				showFeedback(feedback);
				return true;
			}
			if (feedback.hasWarnings()) {
				showFeedback(feedback);
				return false;
			}
		}
		return false;
	}

	private void showFeedback(ValidationFeedback feedback) {
		if (feedback == null) {
			return;
		}
		String message = "";
		String title = "";
		int messageType = JOptionPane.INFORMATION_MESSAGE;
		if (feedback.hasErrors()) {
			title = TITLE_ERRORS_IN_DATABASE_CONFIGURATION;
			message = createMessage(feedback.getErrors().keySet());
			messageType = JOptionPane.ERROR_MESSAGE;
		} else if (feedback.hasWarnings()) {
			title = TITLE_WARNINGS_ABOUT_DATABASE_CONFIGURATION;
			message = createMessage(feedback.getWarnings().keySet());
			messageType = JOptionPane.WARNING_MESSAGE;
		}
		JOptionPane.showMessageDialog(ObjectExchange.frame, message, title, messageType);
	}

	private static String createMessage(Set<String> messages) {
		StringBuilder messageBuilder = new StringBuilder();
		for (String message : messages) {
			messageBuilder.append(String.format("%s%n", message));
		}
		return messageBuilder.toString();
	}

	private DbSettings getTargetDbSettings() {
		DbSettings dbSettings = new DbSettings();
		if (targetType.getSelectedItem().equals(DELIMITED_TEXT_FILES)) {
			dbSettings.sourceType = DbSettings.SourceType.CSV_FILES;

			switch(targetCSVFormat.getSelectedItem().toString()) {
				case "Default (comma, CRLF)":
					dbSettings.csvFormat = CSVFormat.DEFAULT;
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
			dbSettings.sourceType = DbSettings.SourceType.DATABASE;
			dbSettings.user = targetUserField.getText();
			dbSettings.password = targetPasswordField.getText();
			dbSettings.server = targetServerField.getText();
			dbSettings.database = targetDatabaseField.getText();
			switch(targetType.getSelectedItem().toString()) {
				case "MySQL":
					dbSettings.dbType = DbType.MYSQL;
					break;
				case "Oracle":
					dbSettings.dbType = DbType.ORACLE;
					break;
				case "PostgreSQL":
					dbSettings.dbType = DbType.POSTGRESQL;
					break;
				case "SQL Server":
					dbSettings.dbType = DbType.SQL_SERVER;
					if (targetUserField.getText().length() != 0) { // Not using windows authentication
						String[] parts = targetUserField.getText().split("/");
						if (parts.length == 2) {
							dbSettings.user = parts[1];
							dbSettings.domain = parts[0];
						}
					}
					break;
				case "PDW":
					dbSettings.dbType = DbType.PDW;
					if (targetUserField.getText().length() != 0) { // Not using windows authentication
						String[] parts = targetUserField.getText().split("/");
						if (parts.length == 2) {
							dbSettings.user = parts[1];
							dbSettings.domain = parts[0];
						}
					}
					break;
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
			if (locationsPanel.sourceIsFiles() || locationsPanel.sourceIsSas()) {
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

		int rowCount = StringUtilities.numericOptionToInt(scanRowCount.getSelectedItem().toString());
		int valuesCount = StringUtilities.numericOptionToInt(scanValuesCount.getSelectedItem().toString());
		int numStatsSamplerSize = StringUtilities.numericOptionToInt(numericStatsSampleSize.getSelectedItem().toString());

		ScanRunner scanscanRunner = new ScanRunner(
				rowCount,
				valuesCount,
				scanValueScan.isSelected(),
				Integer.parseInt(scanMinCellCount.getValue().toString()),
				calculateNumericStats.isSelected(),
				numStatsSamplerSize
		);
		scanscanRunner.run();
	}

	private void fakeDataRun() {
		String filename = scanReportFileField.getText();
		if (!new File(filename).exists()) {
			String message = "File " + filename + " not found";
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "File not found", JOptionPane.ERROR_MESSAGE);
		} else {
			FakeDataRunner runner = new FakeDataRunner();
			runner.run();
		}
	}

	private class ScanRunner implements Runnable {

		SourceDataScan sourceDataScan = new SourceDataScan();

		public ScanRunner(int maxRows, int maxValues, boolean scanValues, int minCellCount, boolean calculateNumericStats, int numericStatsSampleSize) {
			sourceDataScan.setSampleSize(maxRows);
			sourceDataScan.setScanValues(scanValues);
			sourceDataScan.setMinCellCount(minCellCount);
			sourceDataScan.setMaxValues(maxValues);
			sourceDataScan.setCalculateNumericStats(calculateNumericStats);
			sourceDataScan.setNumStatsSamplerSize(numericStatsSampleSize);
		}

		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getSourceDbSettings(null);
				if (dbSettings != null) {
					for (String table : tables) {
						if (dbSettings.sourceType == DbSettings.SourceType.CSV_FILES || dbSettings.sourceType == DbSettings.SourceType.SAS_FILES)
							table = locationsPanel.getFolderField().getText() + "/" + table;
						dbSettings.tables.add(table);
					}
					sourceDataScan.process(dbSettings, locationsPanel.getFolderField().getText() + "/" + SourceDataScan.SCAN_REPORT_FILE_NAME);
				}
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}
	}

	private class FakeDataRunner implements Runnable {

		public void run() {
			for (JComponent component : componentsToDisableWhenRunning) {
				component.setEnabled(false);
			}
			try {
				FakeDataGenerator process = new FakeDataGenerator();
				DbSettings dbSettings = getTargetDbSettings();
				if (dbSettings != null) {
					process.generateData(
							dbSettings,
							Integer.parseInt(generateRowCount.getValue().toString()),
							scanReportFileField.getText(),
							locationsPanel.getFolderField().getText(),
							doUniformSampling.isSelected()
					);
				}
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}

		}
	}

	private static class DBTableSelectionDialog extends JDialog implements ActionListener {
		private static final long	serialVersionUID	= 4527207331482143091L;
		private final JButton				yesButton;
		private final JButton				noButton;
		private boolean				answer				= false;
		private final JList<String>		list;

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

			list = new JList<>(tableNames.split("\t"));
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
		if (ACTION_CMD_HELP.equals(event.getActionCommand())) {
			doOpenDocumentation();
		}
	}

	private void doOpenDocumentation() {
		try {
			Desktop desktop = Desktop.getDesktop();
			desktop.browse(new URI(DOCUMENTATION_URL));
		} catch (URISyntaxException | IOException ex) {

		}
	}

	private void handleError(Exception e) {
		logger.error(e.getMessage(), e);
		String errorReportFilename = ErrorReport.generate(locationsPanel.getFolderField().getText(), e);
		String message = "Error: " + e.getLocalizedMessage();
		message += "\nAn error report has been generated:\n" + errorReportFilename;
		logger.error(message);
		JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error", JOptionPane.ERROR_MESSAGE);
	}

	private JMenuBar createMenuBar() {
		JMenuBar menuBar = new JMenuBar();
		JMenu helpMenu = new JMenu("Help");

		JMenuItem versionItem = new JMenuItem("White Rabbit v" +  Version.getVersion(this.getClass()));
		versionItem.setEnabled(false);
		helpMenu.add(versionItem);

		menuBar.add(helpMenu);
		JMenuItem helpItem = new JMenuItem(ACTION_CMD_HELP);
		helpItem.addActionListener(this);
		helpItem.setActionCommand(ACTION_CMD_HELP);
		helpMenu.add(helpItem);

		return menuBar;
	}

	public JFrame getFrame() {
		return frame;
	}

	public Console getConsole() {
		return console;
	}
}
