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
package org.ohdsi.rabbitInAHat;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.Desktop;
import java.awt.Dimension;
import java.awt.Image;
import java.awt.MediaTracker;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.filechooser.FileFilter;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.ohdsi.rabbitInAHat.ETLMarkupDocumentGenerator.DocumentType;
import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.Database.CDMVersion;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.MappableItem;
import org.ohdsi.rabbitInAHat.dataModel.StemTableFactory;
import org.ohdsi.rabbitInAHat.dataModel.Table;

/**
 * This is the main class for the RabbitInAHat application
 */
public class RabbitInAHatMain implements ResizeListener, ActionListener {

	public final static String		ACTION_SAVE							= "Save";
	public final static String		ACTION_SAVE_AS						= "Save As";
	public final static String		ACTION_OPEN_ETL_SPECS				= "Open ETL Specs";
	public final static String		ACTION_OPEN_SCAN_REPORT				= "Open Scan Report";
	public final static String		ACTION_GENERATE_ETL_WORD_DOCUMENT	= "Generate ETL Word Document";
	public final static String		ACTION_GENERATE_ETL_HTML_DOCUMENT	= "Generate ETL HTML Documents";
	public final static String		ACTION_GENERATE_ETL_MD_DOCUMENT		= "Generate ETL Markdown Documents";
	public final static String 		ACTION_GENERATE_SOURCE_FIELD_LIST   = "Generate Source Field list";
	public final static String 		ACTION_GENERATE_TARGET_FIELD_LIST   = "Generate Target Field list";
	public final static String 		ACTION_GENERATE_TABLE_MAPPING_LIST  = "Generate Table Mapping list";
	public final static String		ACTION_GENERATE_TEST_FRAMEWORK		= "Generate ETL Test Framework";
	public final static String		ACTION_GENERATE_SQL                 = "Generate SQL Skeleton Files";
	public final static String		ACTION_DISCARD_COUNTS				= "Discard Value Counts";
	public final static String		ACTION_FILTER						= "Filter";
	public final static String		ACTION_MAKE_MAPPING					= "Make Mappings";
	public final static String		ACTION_REMOVE_MAPPING				= "Remove Mappings";
	public final static String		ACTION_SET_TARGET_V4				= "CDM v4";
	public final static String		ACTION_SET_TARGET_V5				= "CDM v5.0.0";
	public final static String		ACTION_SET_TARGET_V501				= "CDM v5.0.1";
	public final static String		ACTION_SET_TARGET_V510				= "CDM v5.1.0";
	public final static String		ACTION_SET_TARGET_V520				= "CDM v5.2.0";
	public final static String		ACTION_SET_TARGET_V530				= "CDM v5.3.0";
	public final static String		ACTION_SET_TARGET_V531				= "CDM v5.3.1";
	public final static String		ACTION_SET_TARGET_V60				= "CDM v6.0";
	public final static String		ACTION_ADD_STEM_TABLE				= "Add stem table";
	public final static String		ACTION_REMOVE_STEM_TABLE			= "Remove stem table";
	public final static String		ACTION_SET_TARGET_CUSTOM			= "Load Custom...";
	public final static String		ACTION_MARK_COMPLETED				= "Mark Highlighted As Complete";
	public final static String		ACTION_UNMARK_COMPLETED				= "Mark Highlighted As Incomplete";
	public final static String		ACTION_HELP							= "Open documentation";

	public final static String DOCUMENTATION_URL = "http://ohdsi.github.io/WhiteRabbit";
	private final static FileFilter FILE_FILTER_GZ = new FileNameExtensionFilter("GZIP Files (*.gz)", "gz");
	private final static FileFilter FILE_FILTER_JSON = new FileNameExtensionFilter("JSON Files (*.json)", "json");
	private final static FileFilter FILE_FILTER_DOCX = new FileNameExtensionFilter("Microsoft Word documents (*.docx)", "docx");
	private final static FileFilter FILE_FILTER_HTML = new FileNameExtensionFilter("HTML documents (*.html)", "html");
	private final static FileFilter FILE_FILTER_MD = new FileNameExtensionFilter("Markdown documents (*.md)", "md");
	private final static FileFilter FILE_FILTER_CSV = new FileNameExtensionFilter("Text Files (*.csv)", "csv");
	private final static FileFilter FILE_FILTER_R = new FileNameExtensionFilter("R script (*.r)", "r");
	private final static FileFilter FILE_FILTER_XLSX = new FileNameExtensionFilter("XLSX files (*.xlsx)", "xlsx");

	public final static String		DBMS_SQLSERVER						= "SQL Server";
	public final static String		DBMS_REDSHIFT						= "Redshift";

	private JFrame					frame;
	private JScrollPane				scrollPane1;
	private JScrollPane				scrollPane2;
	private MappingPanel			tableMappingPanel;
	private MappingPanel			fieldMappingPanel;
	private DetailsPanel			detailsPanel;
	private JSplitPane				tableFieldSplitPane;
	private JFileChooser			chooser;

	public static void main(String[] args) {
		new RabbitInAHatMain(args);
	}

	public RabbitInAHatMain(String[] args) {

		// Set look and feel to the system look and feel
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		frame = new JFrame("Rabbit in a Hat");

		frame.setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
		frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				String ObjButtons[] = {"Yes","No"};
				int PromptResult = JOptionPane.showOptionDialog(
						null,
						"Do you want to exit?\nPlease make sure that any work is saved",
						"Rabbit In A Hat",JOptionPane.DEFAULT_OPTION,JOptionPane.WARNING_MESSAGE,
						null,ObjButtons,ObjButtons[1]
				);
				if (PromptResult==JOptionPane.YES_OPTION) {
					System.exit(0);
				}
			}
		});
		frame.setPreferredSize(new Dimension(700, 600));
		frame.setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));
		frame.setJMenuBar(createMenuBar());

		ETL etl = new ETL();
		etl.setTargetDatabase(Database.generateCDMModel(CDMVersion.CDMV60));

		ObjectExchange.etl = etl;

		tableMappingPanel = new MappingPanel(etl.getTableToTableMapping());
		tableMappingPanel.addResizeListener(this);
		scrollPane1 = new JScrollPane(tableMappingPanel);
		scrollPane1.setBorder(new TitledBorder("Tables"));
		scrollPane1.getVerticalScrollBar().setUnitIncrement(16);
		scrollPane1.getViewport().setScrollMode(JViewport.BACKINGSTORE_SCROLL_MODE);
		scrollPane1.setAutoscrolls(true);
		scrollPane1.setOpaque(true);
		scrollPane1.setBackground(Color.WHITE);

		fieldMappingPanel = new MappingPanel(etl.getTableToTableMapping());
		tableMappingPanel.setSlaveMappingPanel(fieldMappingPanel);
		fieldMappingPanel.addResizeListener(this);
		scrollPane2 = new JScrollPane(fieldMappingPanel);
		scrollPane2.getVerticalScrollBar().setUnitIncrement(16);
		scrollPane2.getViewport().setScrollMode(JViewport.BACKINGSTORE_SCROLL_MODE);
		scrollPane2.setVisible(false);
		scrollPane2.setBorder(new TitledBorder("Fields"));

		tableFieldSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, scrollPane1, scrollPane2);
		tableFieldSplitPane.setDividerLocation(600);
		tableFieldSplitPane.setDividerSize(0);

		detailsPanel = new DetailsPanel();
		detailsPanel.setBorder(new TitledBorder("Details"));
		detailsPanel.setPreferredSize(new Dimension(200, 500));
		detailsPanel.setMinimumSize(new Dimension(0, 0));
		tableMappingPanel.setDetailsListener(detailsPanel);
		fieldMappingPanel.setDetailsListener(detailsPanel);
		JSplitPane leftRightSplinePane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, tableFieldSplitPane, detailsPanel);
		leftRightSplinePane.setResizeWeight(0.40);
		frame.add(leftRightSplinePane);

		loadIcons(frame);
		frame.pack();
		frame.setExtendedState(java.awt.Frame.MAXIMIZED_BOTH);
		frame.setVisible(true);

		if (args.length == 1) {
			doOpenSpecs(args[0]);
			chooser = new JFileChooser();
			chooser.setSelectedFile(new File(args[0]));
		}
		if (args.length > 1 && args[0].equals("-folder")) {
			chooser = new JFileChooser(args[1]);
		}
	}

	private void loadIcons(JFrame f) {
		List<Image> icons = new ArrayList<Image>();
		icons.add(loadIcon("RabbitInAHat16.png", f));
		icons.add(loadIcon("RabbitInAHat32.png", f));
		icons.add(loadIcon("RabbitInAHat48.png", f));
		icons.add(loadIcon("RabbitInAHat64.png", f));
		icons.add(loadIcon("RabbitInAHat128.png", f));
		icons.add(loadIcon("RabbitInAHat256.png", f));
		f.setIconImages(icons);
	}

	private Image loadIcon(String name, JFrame f) {
		Image icon = Toolkit.getDefaultToolkit().getImage(RabbitInAHatMain.class.getResource(name));
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

	private JMenuBar createMenuBar() {
		JMenuBar menuBar = new JMenuBar();

		JMenu fileMenu = new JMenu("File");
		menuBar.add(fileMenu);
		addMenuItem(fileMenu, ACTION_OPEN_SCAN_REPORT);
		addMenuItem(fileMenu, ACTION_OPEN_ETL_SPECS, KeyEvent.VK_O);
		addMenuItem(fileMenu, ACTION_SAVE, KeyEvent.VK_S);
		addMenuItem(fileMenu, ACTION_SAVE_AS);

		JMenu editMenu = new JMenu("Edit");
		menuBar.add(editMenu);
		addMenuItem(editMenu, ACTION_DISCARD_COUNTS);
		addMenuItem(editMenu, ACTION_FILTER, KeyEvent.VK_F);
		addMenuItem(editMenu, ACTION_ADD_STEM_TABLE);
		addMenuItem(editMenu, ACTION_REMOVE_STEM_TABLE);

		JMenu setTargetMenu = new JMenu("Set Target Database");
		editMenu.add(setTargetMenu);
		addRadioMenuItems(setTargetMenu,
				ACTION_SET_TARGET_V60,
				ACTION_SET_TARGET_V4,
				ACTION_SET_TARGET_V5,
				ACTION_SET_TARGET_V501,
				ACTION_SET_TARGET_V510,
				ACTION_SET_TARGET_V520,
				ACTION_SET_TARGET_V530,
				ACTION_SET_TARGET_V531,
				ACTION_SET_TARGET_CUSTOM
		);

		JMenu arrowMenu = new JMenu("Arrows");
		menuBar.add(arrowMenu);
		addMenuItem(arrowMenu, ACTION_MAKE_MAPPING, KeyEvent.VK_M);
		addMenuItem(arrowMenu, ACTION_REMOVE_MAPPING, KeyEvent.VK_R);
		addMenuItem(arrowMenu, ACTION_MARK_COMPLETED, KeyEvent.VK_D);
		addMenuItem(arrowMenu, ACTION_UNMARK_COMPLETED, KeyEvent.VK_I);

		JMenu exportMenu = new JMenu("Generate");
		menuBar.add(exportMenu);
		JMenu generateEtlDocumentMenu = new JMenu("ETL Document");
		exportMenu.add(generateEtlDocumentMenu);
		addMenuItem(generateEtlDocumentMenu, ACTION_GENERATE_ETL_WORD_DOCUMENT);
		addMenuItem(generateEtlDocumentMenu, ACTION_GENERATE_ETL_HTML_DOCUMENT);
		addMenuItem(generateEtlDocumentMenu, ACTION_GENERATE_ETL_MD_DOCUMENT);
		JMenu generateOverviewsMenu = new JMenu("Overview Table");
		exportMenu.add(generateOverviewsMenu);
		addMenuItem(generateOverviewsMenu, ACTION_GENERATE_SOURCE_FIELD_LIST);
		addMenuItem(generateOverviewsMenu, ACTION_GENERATE_TARGET_FIELD_LIST);
		addMenuItem(generateOverviewsMenu, ACTION_GENERATE_TABLE_MAPPING_LIST);
		addMenuItem(exportMenu, ACTION_GENERATE_TEST_FRAMEWORK);
		addMenuItem(exportMenu, ACTION_GENERATE_SQL);

		JMenu helpMenu = new JMenu("Help");
		menuBar.add(helpMenu);
		addMenuItem(helpMenu, ACTION_HELP);

		return menuBar;
	}

	public void addMenuItem(JMenu menu, String action) {
		addMenuItem(menu, action, null);
	}

	public void addMenuItem(JMenu menu, String action, Integer keyEvent) {
		JMenuItem menuItem = new JMenuItem(action);
		menuItem.addActionListener(this);
		if (keyEvent != null) {
			menuItem.setAccelerator(KeyStroke.getKeyStroke(keyEvent, Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()));
		}
		menu.add(menuItem);
	}

	public void addRadioMenuItems(JMenu menu, String... actions) {
		ButtonGroup modeGroup = new ButtonGroup();
		// The first is the default
		boolean isFirstAction = true;
		for (String action : actions) {
			JRadioButtonMenuItem menuItem = new JRadioButtonMenuItem(action, isFirstAction);
			menuItem.addActionListener(this);
			menu.add(menuItem);
			modeGroup.add(menuItem);
			isFirstAction = false;
		}
	}

	@Override
	public void notifyResized(int height, boolean minimized, boolean maximized) {
		if (scrollPane2.isVisible() == maximized)
			scrollPane2.setVisible(!maximized);

		if (!maximized)
			scrollPane1.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);
		else
			scrollPane1.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);

		if (!minimized)
			scrollPane2.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);
		else
			scrollPane2.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);

		tableFieldSplitPane.setDividerLocation(height);
	}

	/**
	 * Display file chooser for a path to save or open to
	 *
	 * @param saveMode true to display a save dialog, false for open
	 * @param directoryMode true to select folder instead of file
	 * @param presetFileName the filename to assign by default (in saveMode)
	 * @param filter   restrict files displayed
	 * @return if file selected, absolute path of selected file otherwise null
	 */
	private String choosePath(boolean saveMode, boolean directoryMode, String presetFileName, FileFilter... filter) {
		String result = null;
		FileFilter primaryFileFilter = filter[0];

		// Create chooser and the last selected file
		if (chooser == null) {
			chooser = new JFileChooser();
		}

		if (presetFileName != null) {
			chooser.setSelectedFile(new File(chooser.getCurrentDirectory(), presetFileName));
		}
		chooser.resetChoosableFileFilters();

		if (directoryMode) {
			chooser.setDialogTitle("Select Folder");
			chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
			chooser.setAcceptAllFileFilterUsed(false);
		} else {
			chooser.setDialogTitle("Select File");
			chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
			chooser.setFileFilter(primaryFileFilter);
			for (int i = 1; i < filter.length; i++) {
				chooser.addChoosableFileFilter(filter[i]);
			}
		}

		int dialogResult = saveMode ? chooser.showSaveDialog(frame) : chooser.showOpenDialog(frame);
		if (dialogResult == JFileChooser.APPROVE_OPTION) {
			if (directoryMode && !chooser.getSelectedFile().isDirectory()) {
				result = chooser.getCurrentDirectory().getAbsolutePath();
			} else {
				result = chooser.getSelectedFile().getAbsolutePath();
			}
		}

		return result;
	}

	private String choosePath(boolean saveMode, boolean directoryMode, FileFilter... filter) {
		return choosePath(saveMode, directoryMode, null, filter);
	}

	private String chooseSavePath(String presetFileName, FileFilter... fileFilters) {
		String path = choosePath(true, false, presetFileName, fileFilters);
		if (path != null && fileFilters[0] == FILE_FILTER_GZ && !path.toLowerCase().endsWith(".json.gz") && !path.toLowerCase().endsWith(".json"))
			path += ".json.gz";
		if (path != null && fileFilters[0] == FILE_FILTER_DOCX && !path.toLowerCase().endsWith(".docx"))
			path += ".docx";
		if (path != null && fileFilters[0] == FILE_FILTER_HTML && !path.toLowerCase().endsWith(".html"))
			path += ".html";
		if (path != null && fileFilters[0] == FILE_FILTER_MD && !path.toLowerCase().endsWith(".md"))
			path += ".md";
		if (path != null && fileFilters[0] == FILE_FILTER_R && !path.toLowerCase().endsWith(".R"))
			path += ".R";
		return path;
	}

	private String chooseSavePath(FileFilter... fileFilter) {
		return chooseSavePath(null, fileFilter);
	}

	private String chooseOpenPath(FileFilter... fileFilter) {
		return choosePath(false, false, fileFilter);
	}

	private String chooseSaveDirectory() {
		return choosePath(true, true, new FileNameExtensionFilter("Directories","."));
	}

	@Override
	public void actionPerformed(ActionEvent event) {
		String filename = ObjectExchange.etl.getFilename();
		switch (event.getActionCommand()) {
			case ACTION_SAVE:
				doSave((filename == null || !filename.toLowerCase().endsWith(".json.gz")) ? chooseSavePath(FILE_FILTER_GZ) : filename);
				break;
			case ACTION_SAVE_AS:
				doSave(chooseSavePath(FILE_FILTER_GZ, FILE_FILTER_JSON));
				break;
			case ACTION_OPEN_ETL_SPECS:
				doOpenSpecs(chooseOpenPath(FILE_FILTER_GZ, FILE_FILTER_JSON));
				break;
			case ACTION_OPEN_SCAN_REPORT:
				doOpenScanReport(chooseOpenPath(FILE_FILTER_XLSX));
				break;
			case ACTION_GENERATE_ETL_WORD_DOCUMENT:
				filename = filename.replaceAll("\\..*$", ".docx");
				doGenerateEtlWordDoc(chooseSavePath(filename, FILE_FILTER_DOCX));
				break;
			case ACTION_GENERATE_ETL_HTML_DOCUMENT:
				doGenerateEtlHtmlDoc(chooseSaveDirectory());
				break;
			case ACTION_GENERATE_ETL_MD_DOCUMENT:
				doGenerateEtlMdDoc(chooseSaveDirectory());
				break;
			case ACTION_GENERATE_TEST_FRAMEWORK:
				doGenerateTestFramework(chooseSavePath("TestFramework.R", FILE_FILTER_R));
				break;
			case ACTION_GENERATE_SOURCE_FIELD_LIST:
				doGenerateSourceFields(chooseSavePath("source_field_list.csv", FILE_FILTER_CSV));
				break;
			case ACTION_GENERATE_TARGET_FIELD_LIST:
				doGenerateTargetFields(chooseSavePath("target_field_list.csv", FILE_FILTER_CSV));
				break;
			case ACTION_GENERATE_TABLE_MAPPING_LIST:
				doGenerateTableMappings(chooseSavePath("table_mapping_list.csv", FILE_FILTER_CSV));
				break;
			case ACTION_GENERATE_SQL:
				doGenerateSql(chooseSaveDirectory());
				break;
			case ACTION_DISCARD_COUNTS:
				doDiscardCounts();
				break;
			case ACTION_FILTER:
				doOpenFilterDialog();
				break;
			case ACTION_MAKE_MAPPING:
				doMakeMappings();
				break;
			case ACTION_REMOVE_MAPPING:
				doRemoveMappings();
				break;
			case ACTION_SET_TARGET_V4:
				doSetTargetCDM(CDMVersion.CDMV4);
				break;
			case ACTION_SET_TARGET_V5:
				doSetTargetCDM(CDMVersion.CDMV5);
				break;
			case ACTION_SET_TARGET_V501:
				doSetTargetCDM(CDMVersion.CDMV501);
				break;
			case ACTION_SET_TARGET_V510:
				doSetTargetCDM(CDMVersion.CDMV510);
				break;
			case ACTION_SET_TARGET_V520:
				doSetTargetCDM(CDMVersion.CDMV520);
				break;
			case ACTION_SET_TARGET_V530:
				doSetTargetCDM(CDMVersion.CDMV530);
				break;
			case ACTION_SET_TARGET_V531:
				doSetTargetCDM(CDMVersion.CDMV531);
				break;
			case ACTION_SET_TARGET_V60:
				doSetTargetCDM(CDMVersion.CDMV60);
				break;
			case ACTION_SET_TARGET_CUSTOM:
				doSetTargetCustom(chooseOpenPath(FILE_FILTER_CSV));
				break;
			case ACTION_ADD_STEM_TABLE:
				doAddStemTable();
				break;
			case ACTION_REMOVE_STEM_TABLE:
				doRemoveStemTable();
				break;
			case ACTION_MARK_COMPLETED:
				doMarkCompleted();
				break;
			case ACTION_UNMARK_COMPLETED:
				doUnmarkCompleted();
				break;
			case ACTION_HELP:
				doOpenDocumentation();
				break;
		}
	}

	private void doAddStemTable() {
		if (!ObjectExchange.etl.hasStemTable()) {
			StemTableFactory.addStemTable(ObjectExchange.etl);
			tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
		}
	}

	private void doRemoveStemTable() {
		if (ObjectExchange.etl.hasStemTable()) {
			String[] ObjButtons = {"Yes","No"};
			int PromptResult = JOptionPane.showOptionDialog(
					null,
					"Any mappings to/from the stem table will be lost. Are you sure?",
					"Rabbit In A Hat",JOptionPane.DEFAULT_OPTION,JOptionPane.WARNING_MESSAGE,
					null,ObjButtons,ObjButtons[1]
			);

			if (PromptResult==JOptionPane.YES_OPTION) {
				StemTableFactory.removeStemTable(ObjectExchange.etl);
				tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
			}
		}
	}

	private void doGenerateTestFramework(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETLTestFrameWorkGenerator etlTestFrameWorkGenerator = new ETLTestFrameWorkGenerator();
			etlTestFrameWorkGenerator.generate(ObjectExchange.etl, filename);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doOpenDocumentation() {
		try {
			Desktop desktop = Desktop.getDesktop();
			desktop.browse(new URI(DOCUMENTATION_URL));
		} catch (URISyntaxException | IOException ex) {

		}
	}

	private void doSetTargetCustom(String filename) {

		if (filename != null) {
			File file = new File(filename);
			InputStream stream;

			try {
				stream = new FileInputStream(file);
				ETL etl = new ETL(ObjectExchange.etl.getSourceDatabase(), Database.generateModelFromCSV(stream, file.getName()));

				etl.copyETLMappings(ObjectExchange.etl);
				tableMappingPanel.setMapping(etl.getTableToTableMapping());
				ObjectExchange.etl = etl;
			} catch (IOException e) {
				// Do nothing if error
			}
		}

	}

	private void doSetTargetCDM(CDMVersion cdmVersion) {
		ETL etl = new ETL(ObjectExchange.etl.getSourceDatabase(), Database.generateCDMModel(cdmVersion));
		etl.copyETLMappings(ObjectExchange.etl);
		tableMappingPanel.setMapping(etl.getTableToTableMapping());
		ObjectExchange.etl = etl;
	}

	private void doOpenFilterDialog() {
		FilterDialog filter;
		filter = new FilterDialog(frame);

		filter.setFilterPanel(tableMappingPanel);

		filter.setVisible(true);
	}

	private void doMakeMappings() {
		if (this.tableMappingPanel.isMaximized()) {
			this.tableMappingPanel.makeMapSelectedSourceAndTarget();
		} else {
			this.fieldMappingPanel.makeMapSelectedSourceAndTarget();
		}

	}

	private void doRemoveMappings() {
		if (this.tableMappingPanel.isMaximized()) {
			this.tableMappingPanel.removeMapSelectedSourceAndTarget();
		} else {
			this.fieldMappingPanel.removeMapSelectedSourceAndTarget();
		}

	}

	private void doDiscardCounts() {
		ObjectExchange.etl.discardCounts();
		detailsPanel.refresh();
	}

	private void doSave(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETL.FileFormat fileFormat = filename.endsWith("json.gz") ? ETL.FileFormat.GzipJson
					: filename.endsWith("json") ? ETL.FileFormat.Json : ETL.FileFormat.Binary;
			ObjectExchange.etl.save(filename, fileFormat);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doOpenSpecs(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETL.FileFormat fileFormat = filename.endsWith(".json.gz") ? ETL.FileFormat.GzipJson
					: filename.endsWith(".json") ? ETL.FileFormat.Json : ETL.FileFormat.Binary;
			try {
				ObjectExchange.etl = ETL.fromFile(filename, fileFormat);
				tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
			} catch (Exception e) {
				e.printStackTrace();
				JOptionPane.showMessageDialog(null, "Invalid File Format", "Error", JOptionPane.ERROR_MESSAGE);
			}
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doOpenScanReport(String filename) {
		if (filename != null) {
			boolean replace = true;
			if (ObjectExchange.etl.getSourceDatabase().getTables().size() != 0) {
				Object[] options = { "Replace current data", "Load data on field values only" };
				int result = JOptionPane.showOptionDialog(frame, "You already have source data loaded. Do you want to", "Replace source data?",
						JOptionPane.YES_NO_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
				if (result == -1)
					return;
				if (result == 1)
					replace = false;
			}
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			if (replace) {
				ETL etl = new ETL();
				doRemoveStemTable();
				try {
					etl.setSourceDatabase(Database.generateModelFromScanReport(filename));
					etl.setTargetDatabase(ObjectExchange.etl.getTargetDatabase());
					tableMappingPanel.setMapping(etl.getTableToTableMapping());
					ObjectExchange.etl = etl;
				} catch (Exception e) {
					e.printStackTrace();
					JOptionPane.showMessageDialog(null, "Invalid File Format", "Error", JOptionPane.ERROR_MESSAGE);
				}
			} else {
				try {
					Database newData = Database.generateModelFromScanReport(filename);
					Database oldData = ObjectExchange.etl.getSourceDatabase();
					for (Table newTable : newData.getTables()) {
						Table oldTable = (Table) findByName(newTable.getName(), oldData.getTables());
						if (oldTable != null) {
							for (Field newField : newTable.getFields()) {
								Field oldField = (Field) findByName(newField.getName(), oldTable.getFields());
								if (oldField != null) {
									oldField.setValueCounts(newField.getValueCounts());
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					JOptionPane.showMessageDialog(null, "Invalid File Format", "Error", JOptionPane.ERROR_MESSAGE);
				}

			}
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private MappableItem findByName(String name, List<? extends MappableItem> list) {
		for (MappableItem item : list)
			if (item.getName().toLowerCase().equals(name.toLowerCase()))
				return item;
		return null;
	}

	private void doGenerateEtlWordDoc(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETLWordDocumentGenerator.generate(ObjectExchange.etl, filename);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doGenerateEtlHtmlDoc(String directoryName) {
		if (directoryName != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETLMarkupDocumentGenerator generator = new ETLMarkupDocumentGenerator(ObjectExchange.etl);
			generator.generate(directoryName, DocumentType.HTML);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doGenerateEtlMdDoc(String directoryName) {
		if (directoryName != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETLMarkupDocumentGenerator generator = new ETLMarkupDocumentGenerator(ObjectExchange.etl);
			generator.generate(directoryName, DocumentType.MARKDOWN);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

    private void doGenerateSourceFields(String filename) {
        if (filename != null) {
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            ETLSummaryGenerator.generateSourceFieldListCsv(ObjectExchange.etl, filename);
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
    }

    private void doGenerateTargetFields(String filename) {
        if (filename != null) {
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            ETLSummaryGenerator.generateTargetFieldListCsv(ObjectExchange.etl, filename);
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
    }

    private void doGenerateTableMappings(String filename) {
        if (filename != null) {
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
            ETLSummaryGenerator.generateTableMappingsCsv(ObjectExchange.etl, filename);
            frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
    }

	private void doGenerateSql(String directoryName) {
		if (directoryName != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			SQLGenerator sqlGenerator = new SQLGenerator(ObjectExchange.etl, directoryName);
			sqlGenerator.generate();
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doMarkCompleted() {
		this.tableMappingPanel.markCompleted();
		this.fieldMappingPanel.markCompleted();
	}

	private void doUnmarkCompleted() {
		this.tableMappingPanel.unmarkCompleted();
		this.fieldMappingPanel.unmarkCompleted();
	}
}
