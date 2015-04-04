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
package org.ohdsi.rabbitInAHat;

import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.Image;
import java.awt.MediaTracker;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.filechooser.FileFilter;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.whiteRabbit.ObjectExchange;

/**
 * This is the main class for the RabbitInAHat application
 */
public class RabbitInAHatMain implements ResizeListener, ActionListener {

	public final static String ACTION_CMD_SAVE = "Save";
	public final static String ACTION_CMD_SAVE_AS = "Save as";
	public final static String ACTION_CMD_OPEN_ETL_SPECS = "Open ETL Specs";
	public final static String ACTION_CMD_OPEN_SCAN_REPORT = "Open Scan Report";
	public final static String ACTION_CMD_GENERATE_ETL_DOCUMENT = "Generate ETL Document";

	private final static FileFilter FILE_FILTER_JSON = new FileNameExtensionFilter("JSON Files (*.json)", "json");
	private final static FileFilter FILE_FILTER_DOCX = new FileNameExtensionFilter("Microsoft Word documents (*.docx)", "docx");
	private JFrame			frame;
	private JScrollPane		scrollPane1;
	private JScrollPane		scrollPane2;
	private MappingPanel	tableMappingPanel;
	private JSplitPane		tableFieldSplitPane;
	private boolean 		includeCounts;

	public static void main(String[] args) {
		boolean includeCounts = true;
		for (String arg: args) {
			if (arg.equals("--no-counts")) includeCounts = false;
		}
		new RabbitInAHatMain(includeCounts);
	}

	public RabbitInAHatMain(boolean includeCounts) {
		this.includeCounts =  includeCounts;

		frame = new JFrame("Rabbit in a hat");
		frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
		frame.setPreferredSize(new Dimension(700, 600));
		frame.setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));
		frame.setJMenuBar(createMenuBar());

		ETL etl = new ETL();
		etl.setCDMDatabase(Database.generateCDMModel());
		ObjectExchange.etl = etl;

		tableMappingPanel = new MappingPanel(etl.getTableToTableMapping());
		tableMappingPanel.addResizeListener(this);
		scrollPane1 = new JScrollPane(tableMappingPanel);
		scrollPane1.setBorder(new TitledBorder("Tables"));
		scrollPane1.getVerticalScrollBar().setUnitIncrement(16);
		scrollPane1.setAutoscrolls(true);
		frame.addKeyListener(tableMappingPanel);

		MappingPanel fieldMappingPanel = new MappingPanel(etl.getTableToTableMapping());
		tableMappingPanel.setSlaveMappingPanel(fieldMappingPanel);
		fieldMappingPanel.addResizeListener(this);
		scrollPane2 = new JScrollPane(fieldMappingPanel);
		scrollPane2.getVerticalScrollBar().setUnitIncrement(16);
		scrollPane2.setVisible(false);
		scrollPane2.setBorder(new TitledBorder("Fields"));
		frame.addKeyListener(fieldMappingPanel);
		tableFieldSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, scrollPane1, scrollPane2);
		tableFieldSplitPane.setDividerLocation(600);
		tableFieldSplitPane.setDividerSize(0);

		DetailsPanel detailsPanel = new DetailsPanel();
		detailsPanel.setBorder(new TitledBorder("Details"));
		detailsPanel.setPreferredSize(new Dimension(200, 500));
		tableMappingPanel.setDetailsListener(detailsPanel);
		fieldMappingPanel.setDetailsListener(detailsPanel);
		JSplitPane leftRightSplinePane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, tableFieldSplitPane, detailsPanel);
		leftRightSplinePane.setDividerLocation(700);
		frame.add(leftRightSplinePane);

		loadIcons(frame);
		frame.pack();
		frame.setExtendedState(java.awt.Frame.MAXIMIZED_BOTH);
		frame.setVisible(true);
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

		JMenuItem openItem = new JMenuItem("Open ETL specs");
		openItem.addActionListener(this);
		openItem.setActionCommand(ACTION_CMD_OPEN_ETL_SPECS);
		openItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, InputEvent.CTRL_MASK));
		fileMenu.add(openItem);

		JMenuItem openScanReportItem = new JMenuItem("Open scan report");
		openScanReportItem.addActionListener(this);
		openScanReportItem.setActionCommand(ACTION_CMD_OPEN_SCAN_REPORT);
		fileMenu.add(openScanReportItem);

		JMenuItem saveItem = new JMenuItem("Save");
		saveItem.addActionListener(this);
		saveItem.setActionCommand(ACTION_CMD_SAVE);
		saveItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, InputEvent.CTRL_MASK));
		fileMenu.add(saveItem);

		JMenuItem saveAsItem = new JMenuItem("Save as");
		saveAsItem.addActionListener(this);
		saveAsItem.setActionCommand(ACTION_CMD_SAVE_AS);
		fileMenu.add(saveAsItem);

		JMenuItem generateDocItem = new JMenuItem("Generate ETL document");
		generateDocItem.addActionListener(this);
		generateDocItem.setActionCommand(ACTION_CMD_GENERATE_ETL_DOCUMENT);
		fileMenu.add(generateDocItem);

//		JMenu editMenu = new JMenu("Edit");
//		menuBar.add(editMenu);

//		JMenu viewMenu = new JMenu("View");
//		menuBar.add(viewMenu);

//		JMenu helpMenu = new JMenu("Help");
//		menuBar.add(helpMenu);
		return menuBar;
	}

	@Override
	public void notifyResized(int height, boolean minimized, boolean maximized) {
		if (scrollPane2.isVisible() == maximized)
			scrollPane2.setVisible(!maximized);

		if (!maximized)
			scrollPane1.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_NEVER);
		else
			scrollPane1.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);

		if (!minimized)
			scrollPane2.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_NEVER);
		else
			scrollPane2.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);

		tableFieldSplitPane.setDividerLocation(height);
	}

	/**
	 * Display file chooser for a path to save or open to
	 * @param saveMode true to display a save dialog, false for open
	 * @param filter restrict files displayed
	 * @return if file selected, absolute path of selected file otherwise null
	 */
	private String choosePath(boolean saveMode, FileFilter filter) {
		String result = null;
		JFileChooser chooser = new JFileChooser();
		chooser.setFileFilter(filter);
		int dialogResult = saveMode ? chooser.showSaveDialog(frame) : chooser.showOpenDialog(frame);
		if (dialogResult == JFileChooser.APPROVE_OPTION) result = chooser.getSelectedFile().getAbsolutePath();
		return result;
	}

	private String chooseSavePath(FileFilter fileFilter) { return choosePath(true, fileFilter); }

	private String chooseOpenPath(FileFilter fileFilter) { return choosePath(false, fileFilter); }

	private String chooseSavePath() { return chooseSavePath(null); }

	private String chooseOpenPath() { return chooseOpenPath(null); }

	@Override
	public void actionPerformed(ActionEvent event) {
		switch (event.getActionCommand()) {
			case ACTION_CMD_SAVE:
				String filename = ObjectExchange.etl.getFilename();
				doSave(filename == null ? chooseSavePath(FILE_FILTER_JSON) : filename);
				break;
			case ACTION_CMD_SAVE_AS:
				doSave(chooseSavePath(FILE_FILTER_JSON));
				break;
			case ACTION_CMD_OPEN_ETL_SPECS:
				doOpenSpecs(chooseOpenPath(FILE_FILTER_JSON));
				break;
			case ACTION_CMD_OPEN_SCAN_REPORT:
				doOpenScanReport(chooseOpenPath());
				break;
			case ACTION_CMD_GENERATE_ETL_DOCUMENT:
				doGenerateEtlDoc(chooseSavePath(FILE_FILTER_DOCX));
				break;
		}
	}

	private void doSave(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETL.FileFormat fileFormat = filename.endsWith("json") ? ETL.FileFormat.Json : ETL.FileFormat.Binary;
			ObjectExchange.etl.save(filename, fileFormat, includeCounts);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doOpenSpecs(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETL.FileFormat fileFormat = filename.endsWith("json") ? ETL.FileFormat.Json : ETL.FileFormat.Binary;
			try {
				ObjectExchange.etl = ETL.FromFile(filename, fileFormat);
				tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
			} catch (Exception e) {
				e.printStackTrace();
			}
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doOpenScanReport(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETL etl = new ETL();
			etl.setSourceDatabase(Database.generateModelFromScanReport(filename));
			etl.setCDMDatabase(Database.generateCDMModel());
			ObjectExchange.etl = etl;
			tableMappingPanel.setMapping(etl.getTableToTableMapping());
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}

	private void doGenerateEtlDoc(String filename) {
		if (filename != null) {
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
			ETLDocumentGenerator.generate(ObjectExchange.etl, filename, includeCounts);
			frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
		}
	}
}
