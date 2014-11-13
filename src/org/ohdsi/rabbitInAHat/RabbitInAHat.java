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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.swing.BoxLayout;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.KeyStroke;
import javax.swing.border.TitledBorder;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.whiteRabbit.ObjectExchange;

public class RabbitInAHat implements ResizeListener, ActionListener {

	private JFrame			frame;
	private JScrollPane		scrollPane1;
	private JScrollPane		scrollPane2;
	private MappingPanel	tableMappingPanel;
	private JSplitPane		tableFieldSplitPane;
	private String			filename;

	public static void main(String[] args) {
		new RabbitInAHat();
	}

	public RabbitInAHat() {
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
		Image icon = Toolkit.getDefaultToolkit().getImage(RabbitInAHat.class.getResource(name));
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
		openItem.setActionCommand("Open ETL specs");
		openItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, InputEvent.CTRL_MASK));
		fileMenu.add(openItem);

		JMenuItem openScanReportItem = new JMenuItem("Open scan report");
		openScanReportItem.addActionListener(this);
		openScanReportItem.setActionCommand("Open scan report");
		fileMenu.add(openScanReportItem);

		JMenuItem saveItem = new JMenuItem("Save");
		saveItem.addActionListener(this);
		saveItem.setActionCommand("Save");
		saveItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, InputEvent.CTRL_MASK));
		fileMenu.add(saveItem);

		JMenuItem saveAsItem = new JMenuItem("Save as");
		saveAsItem.addActionListener(this);
		saveAsItem.setActionCommand("Save as");
		fileMenu.add(saveAsItem);

		JMenuItem generateDocItem = new JMenuItem("Generate ETL document");
		generateDocItem.addActionListener(this);
		generateDocItem.setActionCommand("Generate ETL document");
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

	@Override
	public void actionPerformed(ActionEvent event) {
		if (event.getActionCommand().equals("Save") || event.getActionCommand().equals("Save as")) {
			if (filename == null || event.getActionCommand().equals("Save as")) {
				JFileChooser fileChooser = new JFileChooser();
				if (fileChooser.showSaveDialog(frame) == JFileChooser.APPROVE_OPTION) {
					File file = fileChooser.getSelectedFile();
					filename = file.getAbsolutePath();
				}
			}
			if (filename != null) {
				frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
				FileOutputStream fileOutputStream = null;
				try {
					fileOutputStream = new FileOutputStream(filename);
				} catch (IOException e) {
					e.printStackTrace();
					fileOutputStream = null;
				}
				GZIPOutputStream gzipOutputStream = null;
				if (fileOutputStream != null)
					try {
						gzipOutputStream = new GZIPOutputStream(fileOutputStream);
					} catch (IOException e) {
						e.printStackTrace();
						gzipOutputStream = null;
						try {
							fileOutputStream.close();
						} catch (IOException e2) {
							e2.printStackTrace();
						}
					}
				ObjectOutputStream out = null;
				if (gzipOutputStream != null)
					try {
						out = new ObjectOutputStream(gzipOutputStream);
					} catch (IOException e) {
						e.printStackTrace();
						out = null;
						try {
							gzipOutputStream.close();
						} catch (IOException e2) {
							e2.printStackTrace();
						}
					}

				try {
					out.writeObject(ObjectExchange.etl);
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						out.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));

			}
		} else if (event.getActionCommand().equals("Open ETL specs")) {
			JFileChooser fileChooser = new JFileChooser();
			if (fileChooser.showOpenDialog(frame) == JFileChooser.APPROVE_OPTION) {
				File file = fileChooser.getSelectedFile();
				filename = file.getAbsolutePath();
				try {
					frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
					FileInputStream fileOutputStream = new FileInputStream(filename);
					GZIPInputStream gzipOutputStream = new GZIPInputStream(fileOutputStream);
					ObjectInputStream out = new ObjectInputStream(gzipOutputStream);
					ObjectExchange.etl = (ETL) out.readObject();
					out.close();
					tableMappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
					frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} else if (event.getActionCommand().equals("Open scan report")) {
			JFileChooser fileChooser = new JFileChooser();
			if (fileChooser.showOpenDialog(frame) == JFileChooser.APPROVE_OPTION) {
				frame.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
				File file = fileChooser.getSelectedFile();
				ETL etl = new ETL();
				etl.setSourceDatabase(Database.generateModelFromScanReport(file.getAbsolutePath()));
				etl.setCDMDatabase(Database.generateCDMModel());
				ObjectExchange.etl = etl;
				tableMappingPanel.setMapping(etl.getTableToTableMapping());
				frame.setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
			}

		} else if (event.getActionCommand().equals("Generate ETL document")) {
			JFileChooser fileChooser = new JFileChooser();
			fileChooser.setFileFilter(new FileNameExtensionFilter("DocX Files", "docX"));
			if (fileChooser.showSaveDialog(frame) == JFileChooser.APPROVE_OPTION) {
				File file = fileChooser.getSelectedFile();
				filename = file.getAbsolutePath();
				if (!filename.toLowerCase().endsWith(".docx"))
					filename = filename + ".docx";
				ETLDocumentGenerator.generate(ObjectExchange.etl, filename);
			}
		}

	}
}
