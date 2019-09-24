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
	public final static String		ACTION_GENERATE_ETL_HTML_DOCUMENT	= "Generate ETL HTML Document";
	public final static String		ACTION_GENERATE_ETL_MD_DOCUMENT		= "Generate ETL Markdown Document";
	public final static String		ACTION_GENERATE_TEST_FRAMEWORK		= "Generate ETL Test Framework";
    public final static String		ACTION_GENERATE_SQL                 = "Generate SQL Skeleton";
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
	public final static String		ACTION_HELP							= "Open help Wiki";

	public final static String		WIKI_URL							= "http://www.ohdsi.org/web/wiki/doku.php?id=documentation:software:whiterabbit#rabbit-in-a-hat";
	private final static FileFilter	FILE_FILTER_GZ						= new FileNameExtensionFilter("GZIP Files (*.gz)", "gz");
	private final static FileFilter	FILE_FILTER_JSON					= new FileNameExtensionFilter("JSON Files (*.json)", "json");
	private final static FileFilter	FILE_FILTER_DOCX					= new FileNameExtensionFilter("Microsoft Word documents (*.docx)", "docx");
	private final static FileFilter	FILE_FILTER_HTML					= new FileNameExtensionFilter("HTML documents (*.html)", "html");
	private final static FileFilter	FILE_FILTER_MD						= new FileNameExtensionFilter("Markdown documents (*.md)", "md");
	private final static FileFilter	FILE_FILTER_CSV						= new FileNameExtensionFilter("Text Files (*.csv)", "csv");
	private final static FileFilter	FILE_FILTER_R						= new FileNameExtensionFilter("R script (*.r)", "r");
	private final static FileFilter	FILE_FILTER_XLSX					= new FileNameExtensionFilter("XLSX files (*.xlsx)", "xlsx");

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
		int menuShortcutMask = Toolkit.getDefaultToolkit().getMenuShortcutKeyMask();

		menuBar.add(fileMenu);

		JMenuItem openScanReportItem = new JMenuItem(ACTION_OPEN_SCAN_REPORT);
		openScanReportItem.addActionListener(this);
		fileMenu.add(openScanReportItem);

		JMenuItem openItem = new JMenuItem(ACTION_OPEN_ETL_SPECS);
		openItem.addActionListener(this);
		openItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, menuShortcutMask));
		fileMenu.add(openItem);

		JMenuItem saveItem = new JMenuItem(ACTION_SAVE);
		saveItem.addActionListener(this);
		saveItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, menuShortcutMask));
		fileMenu.add(saveItem);

		JMenuItem saveAsItem = new JMenuItem(ACTION_SAVE_AS);
		saveAsItem.addActionListener(this);
		fileMenu.add(saveAsItem);

		JMenu generateEtlDocument = new JMenu("Generate ETL document");
		fileMenu.add(generateEtlDocument);

		JMenuItem generateWordDocItem = new JMenuItem(ACTION_GENERATE_ETL_WORD_DOCUMENT);
		generateWordDocItem.addActionListener(this);
		generateEtlDocument.add(generateWordDocItem);

		JMenuItem generateHtmlDocItem = new JMenuItem(ACTION_GENERATE_ETL_HTML_DOCUMENT);
		generateHtmlDocItem.addActionListener(this);
		generateEtlDocument.add(generateHtmlDocItem);

		JMenuItem generateMdDocItem = new JMenuItem(ACTION_GENERATE_ETL_MD_DOCUMENT);
		generateMdDocItem.addActionListener(this);
		generateEtlDocument.add(generateMdDocItem);

		JMenuItem generateTestFrameworkItem = new JMenuItem(ACTION_GENERATE_TEST_FRAMEWORK);
		generateTestFrameworkItem.addActionListener(this);
		fileMenu.add(generateTestFrameworkItem);

		JMenuItem generateSql = new JMenuItem(ACTION_GENERATE_SQL);
		generateSql.addActionListener(this);
		generateSql.setActionCommand(ACTION_GENERATE_SQL);
		fileMenu.add(generateSql);

		JMenu editMenu = new JMenu("Edit");
		menuBar.add(editMenu);

		JMenuItem discardCounts = new JMenuItem(ACTION_DISCARD_COUNTS);
		discardCounts.addActionListener(this);
		editMenu.add(discardCounts);

		JMenuItem filter = new JMenuItem(ACTION_FILTER);
		filter.addActionListener(this);
		filter.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_F, menuShortcutMask));
		editMenu.add(filter);

		JMenu setTarget = new JMenu("Set Target Database");

		JRadioButtonMenuItem targetCDMV4 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V4);
		targetCDMV4.addActionListener(this);
		setTarget.add(targetCDMV4);

		JRadioButtonMenuItem targetCDMV5 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V5);
		targetCDMV5.addActionListener(this);
		setTarget.add(targetCDMV5);

		JRadioButtonMenuItem targetCDMV501 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V501);
		targetCDMV501.addActionListener(this);
		setTarget.add(targetCDMV501);

		JRadioButtonMenuItem targetCDMV510 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V510);
		targetCDMV510.addActionListener(this);
		setTarget.add(targetCDMV510);

		JRadioButtonMenuItem targetCDMV520 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V520);
		targetCDMV520.addActionListener(this);
		setTarget.add(targetCDMV520);

		JRadioButtonMenuItem targetCDMV530 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V530);
		targetCDMV530.addActionListener(this);
		setTarget.add(targetCDMV530);

		JRadioButtonMenuItem targetCDMV531 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V531);
		targetCDMV531.addActionListener(this);
		setTarget.add(targetCDMV531);

		JRadioButtonMenuItem targetCDMV60 = new JRadioButtonMenuItem(ACTION_SET_TARGET_V60, true);
		targetCDMV60.addActionListener(this);
		setTarget.add(targetCDMV60);

		JRadioButtonMenuItem loadTarget = new JRadioButtonMenuItem(ACTION_SET_TARGET_CUSTOM);
		loadTarget.addActionListener(this);
		setTarget.add(loadTarget);
		editMenu.add(setTarget);

		ButtonGroup targetGroup = new ButtonGroup();
		targetGroup.add(targetCDMV4);
		targetGroup.add(targetCDMV5);
		targetGroup.add(targetCDMV501);
		targetGroup.add(targetCDMV510);
		targetGroup.add(targetCDMV520);
		targetGroup.add(targetCDMV530);
		targetGroup.add(targetCDMV531);
		targetGroup.add(targetCDMV60);
		targetGroup.add(loadTarget);

		JMenuItem addStemTable = new JMenuItem(ACTION_ADD_STEM_TABLE);
		addStemTable.addActionListener(this);
		editMenu.add(addStemTable);

		JMenuItem removeStemTable = new JMenuItem(ACTION_REMOVE_STEM_TABLE);
		removeStemTable.addActionListener(this);
		editMenu.add(removeStemTable);

		JMenu arrowMenu = new JMenu("Arrows");
		menuBar.add(arrowMenu);

		JMenuItem makeMappings = new JMenuItem(ACTION_MAKE_MAPPING);
		makeMappings.addActionListener(this);
		makeMappings.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_M, menuShortcutMask));
		arrowMenu.add(makeMappings);

		JMenuItem removeMappings = new JMenuItem(ACTION_REMOVE_MAPPING);
		removeMappings.addActionListener(this);
		removeMappings.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_R, menuShortcutMask));
		arrowMenu.add(removeMappings);

		JMenuItem markCompleted = new JMenuItem(ACTION_MARK_COMPLETED);
		markCompleted.addActionListener(this);
		markCompleted.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_D, menuShortcutMask));
		arrowMenu.add(markCompleted);

		JMenuItem unmarkCompleted = new JMenuItem(ACTION_UNMARK_COMPLETED);
		unmarkCompleted.addActionListener(this);
		unmarkCompleted.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_I, menuShortcutMask));
		arrowMenu.add(unmarkCompleted);

		JMenu helpMenu = new JMenu("Help");
		menuBar.add(helpMenu);
		JMenuItem helpItem = new JMenuItem(ACTION_HELP);
		helpItem.addActionListener(this);
		helpMenu.add(helpItem);

		return menuBar;
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
	 * @param saveMode
	 *            true to display a save dialog, false for open
	 * @param filter
	 *            restrict files displayed
	 * @return if file selected, absolute path of selected file otherwise null
	 */
	private String choosePath(boolean saveMode, boolean directoryMode, FileFilter... filter) {
		String result = null;

		if (chooser == null) {
			chooser = new JFileChooser();
		} else if (saveMode && chooser.getSelectedFile() != null){
			if (filter[0] == FILE_FILTER_GZ)
				chooser.setSelectedFile(new File(chooser.getSelectedFile().getAbsolutePath().replaceAll("\\..*$", ".json.gz")));
			else if (filter[0] == FILE_FILTER_DOCX)
				chooser.setSelectedFile(new File(chooser.getSelectedFile().getAbsolutePath().replaceAll("\\..*$", ".docx")));
			else if (filter[0] == FILE_FILTER_HTML)
				chooser.setSelectedFile(new File(chooser.getSelectedFile().getAbsolutePath().replaceAll("\\..*$", ".html")));
			else if (filter[0] == FILE_FILTER_MD)
				chooser.setSelectedFile(new File(chooser.getSelectedFile().getAbsolutePath().replaceAll("\\..*$", ".md")));
			else if (filter[0] == FILE_FILTER_R)
				chooser.setSelectedFile(new File(chooser.getSelectedFile().getAbsolutePath().replaceAll("\\..*$", ".R")));
		}
		chooser.resetChoosableFileFilters();

		if (directoryMode) {
			chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
			chooser.setAcceptAllFileFilterUsed(false);
		} else {
			chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
			chooser.setFileFilter(filter[0]);
			for (int i = 1; i < filter.length; i++)
				chooser.addChoosableFileFilter(filter[i]);
		}

		int dialogResult = saveMode ? chooser.showSaveDialog(frame) : chooser.showOpenDialog(frame);
		if (dialogResult == JFileChooser.APPROVE_OPTION) {
			if (directoryMode)
				result = chooser.getCurrentDirectory().getAbsolutePath();
			else
				result = chooser.getSelectedFile().getAbsolutePath();
		}

		return result;
	}

	private String choosePath(boolean saveMode, FileFilter... filter) {
		return choosePath(saveMode, false, filter);
	}

	private String chooseSavePath(FileFilter... fileFilter) {
		String path = choosePath(true, fileFilter);
		if (path != null && fileFilter[0] == FILE_FILTER_GZ && !path.toLowerCase().endsWith(".json.gz") && !path.toLowerCase().endsWith(".json"))
			path += ".json.gz";
		if (path != null && fileFilter[0] == FILE_FILTER_DOCX && !path.toLowerCase().endsWith(".docx"))
			path += ".docx";
		if (path != null && fileFilter[0] == FILE_FILTER_HTML && !path.toLowerCase().endsWith(".html"))
			path += ".html";
		if (path != null && fileFilter[0] == FILE_FILTER_MD && !path.toLowerCase().endsWith(".md"))
			path += ".md";
		if (path != null && fileFilter[0] == FILE_FILTER_R && !path.toLowerCase().endsWith(".R"))
			path += ".R";
		return path;
	}

	private String chooseOpenPath(FileFilter... fileFilter) {
		return choosePath(false, fileFilter);
	}

	private String chooseSaveDirectory() {
		return choosePath(true, true, new FileNameExtensionFilter("Directories","."));
	}

	@Override
	public void actionPerformed(ActionEvent event) {
		switch (event.getActionCommand()) {
			case ACTION_SAVE:
				String filename = ObjectExchange.etl.getFilename();
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
				doGenerateEtlWordDoc(chooseSavePath(FILE_FILTER_DOCX));
				break;
			case ACTION_GENERATE_ETL_HTML_DOCUMENT:
				doGenerateEtlHtmlDoc(chooseSavePath(FILE_FILTER_HTML));
				break;
			case ACTION_GENERATE_ETL_MD_DOCUMENT:
				doGenerateEtlMdDoc(chooseSavePath(FILE_FILTER_MD));
				break;
			case ACTION_GENERATE_TEST_FRAMEWORK:
				doGenerateTestFramework(chooseSavePath(FILE_FILTER_R));
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
				doOpenWiki();
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

	private void doOpenWiki() {
		try {
			Desktop desktop = Desktop.getDesktop();
			desktop.browse(new URI(WIKI_URL));
		} catch (URISyntaxException | IOException ex) {

		}
	}

	private void doSetTargetCustom(String fileName) {

		if (fileName != null) {
			File file = new File(fileName);
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

	private void doGenerateEtlHtmlDoc(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETLMarkupDocumentGenerator generator = new ETLMarkupDocumentGenerator(ObjectExchange.etl);
			generator.generate(filename, DocumentType.HTML);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doGenerateEtlMdDoc(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETLMarkupDocumentGenerator generator = new ETLMarkupDocumentGenerator(ObjectExchange.etl);
			generator.generate(filename, DocumentType.MARKDOWN);
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
