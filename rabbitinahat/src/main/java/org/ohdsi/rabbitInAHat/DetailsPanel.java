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

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.*;
import javax.swing.border.MatteBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.TableColumnModelEvent;
import javax.swing.event.TableColumnModelListener;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableModel;
import javax.swing.text.Document;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoManager;

import org.ohdsi.rabbitInAHat.dataModel.*;

public class DetailsPanel extends JPanel implements DetailsListener {

	public static Font font = new Font("default", Font.PLAIN, 18);

	private static final long serialVersionUID = 4477553676983048468L;
	private Object object;
	private final TablePanel tablePanel;
	private final FieldPanel fieldPanel;
	private final TargetFieldPanel targetFieldPanel;
	private final ItemToItemMapPanel itemToItemMapPanel;
	private final CardLayout cardLayout = new CardLayout();
	private final NumberFormat numberFormat = NumberFormat.getNumberInstance();
	private final NumberFormat percentageFormat = NumberFormat.getPercentInstance();

	private final UndoManager undoManager;
	
	public DetailsPanel() {
		UIManager.put("Label.font", font);

		setLayout(cardLayout);

		tablePanel = new TablePanel();
		add(tablePanel, Table.class.getName());

		fieldPanel = new FieldPanel();
		add(fieldPanel, Field.class.getName());

		targetFieldPanel = new TargetFieldPanel();
		add(targetFieldPanel, "target" + Field.class.getName());

		itemToItemMapPanel = new ItemToItemMapPanel();
		add(itemToItemMapPanel, ItemToItemMap.class.getName());

		JPanel nullPanel = new JPanel();
		add(nullPanel, "");

		cardLayout.show(this, "");
		
		undoManager = new UndoManager();

		percentageFormat.setMinimumFractionDigits(1);
		
	}

	@Override
	public void showDetails(Object object, boolean isSource) {
		this.object = object;
		if (object instanceof Table) {
			tablePanel.showTable((Table) object);
			tablePanel.updateRowHeights();
			cardLayout.show(this, Table.class.getName());
		} else if (object instanceof Field) {
			if (isSource) {
				fieldPanel.showField((Field) object);
				cardLayout.show(this, Field.class.getName());
			} else {
				targetFieldPanel.showField((Field) object);
				cardLayout.show(this, "target" + Field.class.getName());
			}
		} else if (object instanceof ItemToItemMap) {
			itemToItemMapPanel.showItemToItemMap((ItemToItemMap) object);
			cardLayout.show(this, ItemToItemMap.class.getName());
		} else
			cardLayout.show(this, "");

		// Discard edits made by showing a new details view
		undoManager.discardAllEdits();
	}

	@Override
	public void showDetails(Object object) {
		showDetails(object, true);
	}

	public void refresh() {
		showDetails(object);
	}

	private void addUndoToTextArea(JTextArea jta){
		Document doc = jta.getDocument();
		doc.addUndoableEditListener(e -> undoManager.addEdit(e.getEdit()));
		
		InputMap im = jta.getInputMap(JComponent.WHEN_FOCUSED);
		ActionMap am = jta.getActionMap();

		im.put(KeyStroke.getKeyStroke(KeyEvent.VK_Z, Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()), "Undo");
		im.put(KeyStroke.getKeyStroke(KeyEvent.VK_Y, Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()), "Redo");

		am.put("Undo", new AbstractAction() {
			private static final long serialVersionUID = -3363877112423623107L;

			@Override
		    public void actionPerformed(ActionEvent e) {
		        try {
		            if (undoManager.canUndo()) {
		                undoManager.undo();
		            }
		        } catch (CannotUndoException exp) {
		            exp.printStackTrace();
		        }
		    }
		});
		
		am.put("Redo", new AbstractAction() {
			private static final long serialVersionUID = -5581878642285644039L;

			@Override
		    public void actionPerformed(ActionEvent e) {
		        try {
		            if (undoManager.canRedo()) {
		                undoManager.redo();
		            }
		        } catch (CannotUndoException exp) {
		            exp.printStackTrace();
		        }
		    }
		});
	}
	private class TablePanel extends JPanel implements DocumentListener {

		private static final long	serialVersionUID	= -4393026616049677944L;
		private Table				table;
		private final JLabel				nameLabel			= new JLabel("");
		private final DescriptionTextArea description			= new DescriptionTextArea ("");
		private final JLabel				rowCountLabel		= new JLabel("");
		private final SimpleTableModel	fieldTable			= new SimpleTableModel("Field", "Type", "Description");
		private final JTextArea			commentsArea		= new JTextArea();
		private final JTable 				displayTable 		= new JTable(fieldTable);
		

		public TablePanel() {
			setLayout(new BorderLayout());

			JPanel generalInfoPanel = new JPanel();
			generalInfoPanel.setLayout(new BorderLayout(5,5));
			generalInfoPanel.setBorder(BorderFactory.createTitledBorder("General information"));

			JPanel fieldInfo = new JPanel();
			fieldInfo.setLayout(new GridLayout(0,2));

			fieldInfo.add(new JLabel("Table name: "));
			fieldInfo.add(nameLabel);

			fieldInfo.add(new JLabel("Number of rows: "));
			fieldInfo.add(rowCountLabel);

			generalInfoPanel.add(fieldInfo, BorderLayout.NORTH);

			JPanel descriptionInfo = new JPanel();
			descriptionInfo.setLayout(new GridLayout(0,2));
			descriptionInfo.add(new JLabel("Description: "));
			descriptionInfo.add(description);
			generalInfoPanel.add(descriptionInfo, BorderLayout.SOUTH);

			add(generalInfoPanel, BorderLayout.NORTH);

			JScrollPane fieldListPanel = new JScrollPane(displayTable);
			
			// Updates row heights when column widths change
			displayTable.getColumnModel().addColumnModelListener(new TableColumnModelListener(){
				
				@Override
				public void columnMarginChanged(ChangeEvent e) {
					updateRowHeights();
					
				}
				
				@Override
				public void columnMoved(TableColumnModelEvent e){
					
				}

				@Override
				public void columnAdded(TableColumnModelEvent e) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void columnRemoved(TableColumnModelEvent e) {
					// TODO Auto-generated method stub
					
				}

				@Override
				public void columnSelectionChanged(ListSelectionEvent e) {
					// TODO Auto-generated method stub
					
				}
			});
			
			displayTable.setFont(font);		
			
			// Set cell renderer that wraps content
			for (int c = 0; c < displayTable.getColumnCount(); c++){				
					displayTable.getColumnModel().getColumn(c).setCellRenderer(new TableCellLongTextRenderer());
			}			
			
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
		
		private void updateRowHeights() {

		    /* 
		     * Auto adjust the height of rows in a JTable.
		     * The only way to know the row height for sure is to render each cell 
		     * to determine the rendered height. After your table is populated with 
		     * data you can do:         
		     *  
		     */        
		    for (int row = 0; row < displayTable.getRowCount(); row++) {
		        int rowHeight = displayTable.getRowHeight();
		        for (int column = 0; column < displayTable.getColumnCount(); column++)
		        {
		            Component comp = displayTable.prepareRenderer(displayTable.getCellRenderer(row, column), row, column);
		            
		            rowHeight = Math.max(rowHeight, comp.getPreferredSize().height);
		            
		        }
		        displayTable.setRowHeight(row, rowHeight);
		    }
		}
		
		public void showTable(Table table) {
			this.table = table;
			nameLabel.setText(table.getName());
			description.setText(table.getDescription());

			// Hide description if it's empty
			description.getParent().setVisible(!description.getText().isEmpty());

			if (table.getRowCount() > 0) {
				rowCountLabel.setText(numberFormat.format(table.getRowCount()));
			} else {
				rowCountLabel.setText(">= " + numberFormat.format(table.getRowsCheckedCount()));
			}

			fieldTable.clear();
			
			for (Field field : table.getFields()){
				fieldTable.add(field.outputName(), field.getType(),field.getDescription());
			}
			
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
		private static final long serialVersionUID = -4393026616049677944L;
		JLabel nameLabel;
		JLabel typeLabel;
		JLabel valueDetailLabel;
		JLabel rowCountLabel;
		DescriptionTextArea description;
		SimpleTableModel valueTable;
		JTextArea commentsArea;
		Boolean isTargetFieldPanel;
		private Field field;

		public FieldPanel() {
			nameLabel			= new JLabel("");
			typeLabel           = new JLabel("");
			valueDetailLabel    = new JLabel("");
			rowCountLabel		= new JLabel("");
			description			= new DescriptionTextArea ("");
			valueTable			= new SimpleTableModel("Value", "Frequency", "Percentage");
			commentsArea		= new JTextArea();
			isTargetFieldPanel  = false;
			initialise();
		}

		public void initialise() {
			setLayout(new BorderLayout());

			JPanel generalInfoPanel = new JPanel();
			generalInfoPanel.setLayout(new BorderLayout(5,5));
			generalInfoPanel.setBorder(BorderFactory.createTitledBorder("General information"));
			
			JPanel fieldInfo = new JPanel();
			fieldInfo.setLayout(new GridLayout(0,2));
			
			fieldInfo.add(new JLabel("Field name: "));
			fieldInfo.add(nameLabel);

			fieldInfo.add(new JLabel("Field type: "));
			fieldInfo.add(typeLabel);

			generalInfoPanel.add(fieldInfo, BorderLayout.NORTH);

			JPanel sourceDetailsPanel = new JPanel();
			sourceDetailsPanel.setLayout(new GridLayout(0,2));

			sourceDetailsPanel.add(new JLabel("Unique values: "));
			sourceDetailsPanel.add(valueDetailLabel);

			generalInfoPanel.add(sourceDetailsPanel);

			JPanel descriptionInfo = new JPanel();
			descriptionInfo.setLayout(new GridLayout(0,2));
			descriptionInfo.add(new JLabel("Description: "));
			descriptionInfo.add(description);
			generalInfoPanel.add(descriptionInfo,BorderLayout.SOUTH);
			
			add(generalInfoPanel, BorderLayout.NORTH);

			JTable table = new JTable(valueTable);
			JScrollPane fieldListPanel = new JScrollPane(table);
			table.setFont(font);
			table.setRowHeight(24);
			table.setBorder(new MatteBorder(1, 0, 1, 0, Color.BLACK));
			table.setCellSelectionEnabled(true);

			if (isTargetFieldPanel) {
				// Wide columns for concept name and class id
				table.getColumnModel().getColumn(1).setPreferredWidth(300);
				table.getColumnModel().getColumn(2).setPreferredWidth(100);
			} else {
				// Wide column for value name
				table.getColumnModel().getColumn(0).setPreferredWidth(500);
				// Right align the frequency and percentage
				DefaultTableCellRenderer rightRenderer = new DefaultTableCellRenderer();
				rightRenderer.setHorizontalAlignment(SwingConstants.RIGHT);
				table.getColumnModel().getColumn(1).setCellRenderer(rightRenderer);
				table.getColumnModel().getColumn(2).setCellRenderer(rightRenderer);
			}

			String title = isTargetFieldPanel ?
					"Concept ID Hints " + ObjectExchange.etl.getTargetDatabase().conceptIdHintsVocabularyVersion
					: "Value Counts";
			fieldListPanel.setBorder(BorderFactory.createTitledBorder(title));
			add(fieldListPanel, BorderLayout.CENTER);

			JScrollPane commentsPanel = new JScrollPane(commentsArea);
			commentsArea.setFont(font);
			commentsArea.getDocument().addDocumentListener(this);
			commentsArea.setWrapStyleWord(true);
			commentsArea.setLineWrap(true);
			addUndoToTextArea(commentsArea);

			commentsPanel.setBorder(BorderFactory.createTitledBorder("Comments"));
			commentsPanel.setPreferredSize(new Dimension(100, 200));

			add(commentsPanel, BorderLayout.SOUTH);
		}

		public void showField(Field field) {
			this.field = field;

			nameLabel.setText(field.getName());
			typeLabel.setText(field.getType());

			// Additional unique count and percentage empty. Hide when not given
			StringBuilder valueDetailText = new StringBuilder();
			if (field.getUniqueCount() != null) {
				valueDetailText.append(numberFormat.format(field.getUniqueCount()));
			}
			if (field.getFractionEmpty() != null) {
				String fractionEmptyFormatted;
				if (field.getFractionEmpty() > 0 && field.getFractionEmpty() < 0.001) {
					fractionEmptyFormatted = "<" + percentageFormat.format(0.001);
				} else {
					fractionEmptyFormatted = percentageFormat.format(field.getFractionEmpty());
				}
				valueDetailText.append(String.format(" (%s empty)", fractionEmptyFormatted));
			}
			valueDetailLabel.setText(valueDetailText.toString());
			valueDetailLabel.getParent().setVisible(!valueDetailLabel.getText().isEmpty());

			// Description. Hide when empty
			description.setText(field.getDescription());
			description.getParent().setVisible(!description.getText().isEmpty());

			this.createValueList(field);
			commentsArea.setText(field.getComment());
		}

		public void createValueList(Field field) {
			valueTable.clear();

			int rowsCheckedCount = field.getRowsCheckedCount();
			for (ValueCounts.ValueCount valueCount : field.getValueCounts().getAll()) {
				double valueCountPercent = valueCount.getFrequency() / (double) rowsCheckedCount;
				String valuePercent;
				if (valueCountPercent < 0.001) {
					valuePercent = "<" + percentageFormat.format(0.001);
				} else if (valueCountPercent > 0.99 && valueCountPercent < 1) {
					valuePercent = ">" + percentageFormat.format(0.99);
				} else {
					valuePercent = percentageFormat.format(valueCountPercent);
				}
				String valueNumber = numberFormat.format(valueCount.getFrequency());
				valueTable.add(valueCount.getValue(), valueNumber, valuePercent);
			}

			if (rowsCheckedCount != field.getValueCounts().getTotalFrequency()) {
				valueTable.add("Truncated...", "", "");
			}
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

	private class TargetFieldPanel extends FieldPanel {
		public TargetFieldPanel() {
			nameLabel			= new JLabel("");
			rowCountLabel		= new JLabel("");
			description			= new DescriptionTextArea ("");
			valueTable			= new SimpleTableModel("Concept ID", "Concept Name", "Class", "Standard?");
			commentsArea		= new JTextArea();
			isTargetFieldPanel  = true;
			super.initialise();
		}

		@Override
		public void createValueList(Field field) {
			valueTable.clear();
			if (field.getConceptIdHints() != null) {
				for (ConceptsMap.Concept conceptIdHint : field.getConceptIdHints()) {
					valueTable.add(
							conceptIdHint.getConceptId(),
							conceptIdHint.getConceptName(),
							conceptIdHint.getConceptClassId(),
							conceptIdHint.getStandardConcept()
					);
				}
			}
		}
	}

	private class ItemToItemMapPanel extends JPanel implements DocumentListener {

		private static final long serialVersionUID = -4393026616049677944L;
		private final JLabel sourceLabel = new JLabel("");
		private final JLabel targetLabel = new JLabel("");
		private final JTextArea	logicArea = new JTextArea();
		private final JTextArea commentsArea = new JTextArea();
		private ItemToItemMap itemToItemMap;

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
			addUndoToTextArea(logicArea);

			logicPanel.setBorder(BorderFactory.createTitledBorder("Logic"));
			logicPanel.setPreferredSize(new Dimension(100, 200));

			add(logicPanel, BorderLayout.CENTER);

			JScrollPane commentsPanel = new JScrollPane(commentsArea);
			commentsArea.setFont(font);
			commentsArea.getDocument().addDocumentListener(this);
			commentsArea.setWrapStyleWord(true);
			commentsArea.setLineWrap(true);
			addUndoToTextArea(commentsArea);

			commentsPanel.setBorder(BorderFactory.createTitledBorder("Comments"));
			commentsPanel.setPreferredSize(new Dimension(100, 200));

			add(commentsPanel, BorderLayout.SOUTH);
		}

		public void showItemToItemMap(ItemToItemMap itemToItemMap) {
			this.itemToItemMap = itemToItemMap;
			sourceLabel.setText(itemToItemMap.getSourceItem().toString());
			targetLabel.setText(itemToItemMap.getTargetItem().toString());
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

	private static class SimpleTableModel implements TableModel {

		private final List<TableModelListener> listeners = new ArrayList<>();
		private final List<List<String>> data = new ArrayList<>();
		private final String[] columnNames;

		public void clear() {
			data.clear();
			notifyListeners();
		}

		private void notifyListeners() {
			for (TableModelListener listener : listeners)
				listener.tableChanged(new TableModelEvent(this));
		}

		public void add(String... values) {
			List<String> row = new ArrayList<>(values.length);
			row.addAll(Arrays.asList(values));
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
