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

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.UIManager;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;

public class DetailsPanel extends JPanel implements DetailsListener {

	public static Font			font				= new Font("default", Font.PLAIN, 18);

	private static final long	serialVersionUID	= 4477553676983048468L;
	private Object				object;
	private TablePanel			tablePanel;
	private FieldPanel			fieldPanel;
	private ItemToItemMapPanel	itemToItemMapPanel;
	private CardLayout			cardLayout			= new CardLayout();

	public DetailsPanel() {
		UIManager.put("Label.font", font);

		setLayout(cardLayout);

		tablePanel = new TablePanel();
		add(tablePanel, Table.class.getName());

		fieldPanel = new FieldPanel();
		add(fieldPanel, Field.class.getName());

		itemToItemMapPanel = new ItemToItemMapPanel();
		add(itemToItemMapPanel, ItemToItemMap.class.getName());

		JPanel nullPanel = new JPanel();
		add(nullPanel, "");

		cardLayout.show(this, "");
	}

	@Override
	public void showDetails(Object object) {
		this.object = object;
		if (object instanceof Table) {
			tablePanel.showTable((Table) object);
			cardLayout.show(this, Table.class.getName());
		} else if (object instanceof Field) {
			fieldPanel.showField((Field) object);
			cardLayout.show(this, Field.class.getName());
		} else if (object instanceof ItemToItemMap) {
			itemToItemMapPanel.showItemToItemMap((ItemToItemMap) object);
			cardLayout.show(this, ItemToItemMap.class.getName());
		} else
			cardLayout.show(this, "");
	}

	public void refresh() {
		showDetails(object);
	}

	private class TablePanel extends JPanel implements DocumentListener {

		private static final long	serialVersionUID	= -4393026616049677944L;
		private Table				table;
		private JLabel				nameLabel			= new JLabel("");
		private JLabel				rowCountLabel		= new JLabel("");
		private SimpleTableModel	fieldTable			= new SimpleTableModel("Field", "Type");
		private JTextArea			commentsArea		= new JTextArea();

		public TablePanel() {
			setLayout(new BorderLayout());

			JPanel generalInfoPanel = new JPanel();
			generalInfoPanel.setLayout(new GridLayout(0, 2));
			generalInfoPanel.setBorder(BorderFactory.createTitledBorder("General information"));

			generalInfoPanel.add(new JLabel("Table name: "));
			generalInfoPanel.add(nameLabel);

			generalInfoPanel.add(new JLabel("Number of rows: "));
			generalInfoPanel.add(rowCountLabel);
			add(generalInfoPanel, BorderLayout.NORTH);

			JTable table = new JTable(fieldTable);
			JScrollPane fieldListPanel = new JScrollPane(table);
			table.setFont(font);
			table.setRowHeight(24);
			fieldListPanel.setBorder(BorderFactory.createTitledBorder("Fields"));
			add(fieldListPanel, BorderLayout.CENTER);

			// JScrollPane commentsPanel = new JScrollPane(commentsArea);
			// commentsArea.setFont(font);
			// commentsArea.getDocument().addDocumentListener(this);
			// commentsArea.setWrapStyleWord(true);
			// commentsArea.setLineWrap(true);
			//
			// commentsPanel.setBorder(BorderFactory.createTitledBorder("Comments"));
			// commentsPanel.setPreferredSize(new Dimension(100, 200));
			// add(commentsPanel, BorderLayout.SOUTH);

		}

		public void showTable(Table table) {
			this.table = table;
			nameLabel.setText(table.getName());
			DecimalFormat formatter = new DecimalFormat("#,###");
			rowCountLabel.setText(formatter.format(table.getRowCount()));
			fieldTable.clear();
			for (Field field : table.getFields())
				fieldTable.add(field.getName(), field.getType());
			commentsArea.setText(table.getComment());
		}

		@Override
		public void changedUpdate(DocumentEvent e) {
			table.setComment(commentsArea.getText());
		}

		@Override
		public void insertUpdate(DocumentEvent e) {
			table.setComment(commentsArea.getText());
		}

		@Override
		public void removeUpdate(DocumentEvent e) {
			table.setComment(commentsArea.getText());
		}
	}

	private class FieldPanel extends JPanel implements DocumentListener {

		private static final long	serialVersionUID	= -4393026616049677944L;
		private JLabel				nameLabel			= new JLabel("");
		private JLabel				rowCountLabel		= new JLabel("");
		private SimpleTableModel	valueTable			= new SimpleTableModel("Value", "Frequency", "Percent of Total (%)");
		private JTextArea			commentsArea		= new JTextArea();
		private Field				field;

		public FieldPanel() {
			setLayout(new BorderLayout());

			JPanel generalInfoPanel = new JPanel();
			generalInfoPanel.setLayout(new GridLayout(0, 2));
			generalInfoPanel.setBorder(BorderFactory.createTitledBorder("General information"));

			generalInfoPanel.add(new JLabel("Field name: "));
			generalInfoPanel.add(nameLabel);

			generalInfoPanel.add(new JLabel("Field type: "));
			generalInfoPanel.add(rowCountLabel);

			add(generalInfoPanel, BorderLayout.NORTH);

			JTable table = new JTable(valueTable);
			JScrollPane fieldListPanel = new JScrollPane(table);
			table.setFont(font);
			table.setRowHeight(24);
			fieldListPanel.setBorder(BorderFactory.createTitledBorder("Fields"));
			add(fieldListPanel, BorderLayout.CENTER);

			JScrollPane commentsPanel = new JScrollPane(commentsArea);
			commentsArea.setFont(font);
			commentsArea.getDocument().addDocumentListener(this);
			commentsArea.setWrapStyleWord(true);
			commentsArea.setLineWrap(true);

			commentsPanel.setBorder(BorderFactory.createTitledBorder("Comments"));
			commentsPanel.setPreferredSize(new Dimension(100, 200));

			add(commentsPanel, BorderLayout.SOUTH);
		}

		public void showField(Field field) {
			this.field = field;
			nameLabel.setText(field.getName());
			rowCountLabel.setText(field.getType());
			valueTable.clear();
			if (field.getValueCounts() != null) {
				double valueCountTotal = 0.0;
				for (String[] total : field.getValueCounts()) {
					String temp = total[1];
					if (StringUtilities.isNumber(temp)) {
						double valueCountTemp = Double.parseDouble(temp);
						valueCountTotal += valueCountTemp;
					}
				}
				DecimalFormat formatter = new DecimalFormat("#,###");
				DecimalFormat formatterPercent = new DecimalFormat("#,##0.0");
				for (String[] valueCount : field.getValueCounts()) {
					String nr = valueCount[1];
					String vp = "";
					if (StringUtilities.isNumber(nr)) {
						double number = Double.parseDouble(nr);
						nr = formatter.format(number);
						double valueCountPercent = number / valueCountTotal * 100;
						if (valueCountPercent < 0.1) {
							vp = "< 0.1";
						}
						else if (valueCountPercent > 99) {
							vp = "> 99.0";
						}
						else {
							vp = formatterPercent.format(valueCountPercent);
						}
					}
					valueTable.add(valueCount[0], nr, vp);
				}
			}
			commentsArea.setText(field.getComment());
		}

		@Override
		public void changedUpdate(DocumentEvent e) {
			field.setComment(commentsArea.getText());
		}

		@Override
		public void insertUpdate(DocumentEvent e) {
			field.setComment(commentsArea.getText());
		}

		@Override
		public void removeUpdate(DocumentEvent e) {
			field.setComment(commentsArea.getText());
		}

	}

	private class ItemToItemMapPanel extends JPanel implements DocumentListener {

		private static final long	serialVersionUID	= -4393026616049677944L;
		private JLabel				sourceLabel			= new JLabel("");
		private JLabel				targetLabel			= new JLabel("");
		private JTextArea			logicArea			= new JTextArea();
		private JTextArea			commentsArea		= new JTextArea();
		// private FlexTable testTable = new FlexTable();
		private ItemToItemMap		itemToItemMap;

		public ItemToItemMapPanel() {
			setLayout(new BorderLayout());

			JPanel generalInfoPanel = new JPanel();
			generalInfoPanel.setLayout(new GridLayout(0, 2));
			generalInfoPanel.setBorder(BorderFactory.createTitledBorder("General information"));

			generalInfoPanel.add(new JLabel("Source: "));
			generalInfoPanel.add(sourceLabel);

			generalInfoPanel.add(new JLabel("Target: "));
			generalInfoPanel.add(targetLabel);

			add(generalInfoPanel, BorderLayout.NORTH);

			JScrollPane logicPanel = new JScrollPane(logicArea);
			logicArea.setFont(font);
			logicArea.getDocument().addDocumentListener(this);
			logicArea.setWrapStyleWord(true);
			logicArea.setLineWrap(true);

			logicPanel.setBorder(BorderFactory.createTitledBorder("Logic"));
			logicPanel.setPreferredSize(new Dimension(100, 200));

			add(logicPanel, BorderLayout.CENTER);

			JScrollPane commentsPanel = new JScrollPane(commentsArea);
			commentsArea.setFont(font);
			commentsArea.getDocument().addDocumentListener(this);
			commentsArea.setWrapStyleWord(true);
			commentsArea.setLineWrap(true);

			commentsPanel.setBorder(BorderFactory.createTitledBorder("Comments"));
			commentsPanel.setPreferredSize(new Dimension(100, 200));

			add(commentsPanel, BorderLayout.SOUTH);

			// testTable.setMinimumSize(new Dimension(200, 200));
			// JScrollPane testPanel = new JScrollPane(testTable);
			// testPanel.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
			// add(testPanel, BorderLayout.SOUTH);
		}

		public void showItemToItemMap(ItemToItemMap itemToItemMap) {
			this.itemToItemMap = itemToItemMap;
			sourceLabel.setText(itemToItemMap.getSourceItem().toString());
			targetLabel.setText(itemToItemMap.getCdmItem().toString());
			logicArea.setText(itemToItemMap.getLogic());
			commentsArea.setText(itemToItemMap.getComment());
		}

		@Override
		public void changedUpdate(DocumentEvent e) {
			if (e.getDocument() == logicArea.getDocument())
				itemToItemMap.setLogic(logicArea.getText());
			else
				itemToItemMap.setComment(commentsArea.getText());
		}

		@Override
		public void insertUpdate(DocumentEvent e) {
			if (e.getDocument() == logicArea.getDocument())
				itemToItemMap.setLogic(logicArea.getText());
			else
				itemToItemMap.setComment(commentsArea.getText());
		}

		@Override
		public void removeUpdate(DocumentEvent e) {
			if (e.getDocument() == logicArea.getDocument())
				itemToItemMap.setLogic(logicArea.getText());
			else
				itemToItemMap.setComment(commentsArea.getText());
		}

	}

	private class SimpleTableModel implements TableModel {

		private List<TableModelListener>	listeners	= new ArrayList<TableModelListener>();
		private List<List<String>>			data		= new ArrayList<List<String>>();
		private String[]					columnNames;

		public void clear() {
			data.clear();
			notifyListeners();
		}

		private void notifyListeners() {
			for (TableModelListener listener : listeners)
				listener.tableChanged(new TableModelEvent(this));
		}

		public void add(String... values) {
			List<String> row = new ArrayList<String>(values.length);
			for (int i = 0; i < values.length; i++)
				row.add(values[i]);
			data.add(row);
			notifyListeners();
		}

		public SimpleTableModel(String... columnNames) {
			this.columnNames = columnNames;
		}

		@Override
		public void addTableModelListener(TableModelListener l) {
			listeners.add(l);
		}

		@Override
		public Class<?> getColumnClass(int columnIndex) {
			return String.class;
		}

		@Override
		public int getColumnCount() {
			return columnNames.length;
		}

		@Override
		public String getColumnName(int columnIndex) {
			return columnNames[columnIndex];
		}

		@Override
		public int getRowCount() {
			return data.size();
		}

		@Override
		public Object getValueAt(int rowIndex, int columnIndex) {
			return data.get(rowIndex).get(columnIndex);
		}

		@Override
		public boolean isCellEditable(int rowIndex, int columnIndex) {
			return false;
		}

		@Override
		public void removeTableModelListener(TableModelListener l) {
			listeners.remove(l);
		}

		@Override
		public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
		}

	}
}
