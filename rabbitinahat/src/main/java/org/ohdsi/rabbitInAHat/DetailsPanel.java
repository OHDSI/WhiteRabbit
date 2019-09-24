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
import javax.swing.event.UndoableEditEvent;
import javax.swing.event.UndoableEditListener;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableModel;
import javax.swing.text.Document;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoManager;

import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.ItemToItemMap;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.rabbitInAHat.dataModel.TableCellLongTextRenderer;

public class DetailsPanel extends JPanel implements DetailsListener {

	public static Font			font				= new Font("default", Font.PLAIN, 18);

	private static final long	serialVersionUID	= 4477553676983048468L;
	private Object				object;
	private TablePanel			tablePanel;
	private FieldPanel			fieldPanel;
	private ItemToItemMapPanel	itemToItemMapPanel;
	private CardLayout			cardLayout			= new CardLayout();
	private NumberFormat	    numberFormat 	    = NumberFormat.getNumberInstance();
	private NumberFormat	    percentageFormat    = NumberFormat.getPercentInstance();

	private UndoManager			undoManager;
	
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
		
		undoManager = new UndoManager();

		percentageFormat.setMinimumFractionDigits(1);
		
	}

	@Override
	public void showDetails(Object object) {
		this.object = object;
		if (object instanceof Table) {
			tablePanel.showTable((Table) object);
			tablePanel.updateRowHeights();
			cardLayout.show(this, Table.class.getName());
		} else if (object instanceof Field) {
			fieldPanel.showField((Field) object);
			cardLayout.show(this, Field.class.getName());
		} else if (object instanceof ItemToItemMap) {
			itemToItemMapPanel.showItemToItemMap((ItemToItemMap) object);
			cardLayout.show(this, ItemToItemMap.class.getName());
		} else
			cardLayout.show(this, "");
		
		// Discard edits made by showing a new details view
		undoManager.discardAllEdits();
	}

	public void refresh() {
		showDetails(object);
	}

	private void addUndoToTextArea(JTextArea jta){
		Document doc = jta.getDocument();
		doc.addUndoableEditListener(new UndoableEditListener() {
		    @Override
		    public void undoableEditHappened(UndoableEditEvent e) {
		        undoManager.addEdit(e.getEdit());
		    }
		});
		
		InputMap im = jta.getInputMap(JComponent.WHEN_FOCUSED);
		ActionMap am = jta.getActionMap();

		im.put(KeyStroke.getKeyStroke(KeyEvent.VK_Z, Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()), "Undo");
		im.put(KeyStroke.getKeyStroke(KeyEvent.VK_Y, Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()), "Redo");

		am.put("Undo", new AbstractAction() {
		    /**
			 * 
			 */
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
		    /**
			 * 
			 */
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
		private JLabel				nameLabel			= new JLabel("");
		private JLabel				rowCountLabel		= new JLabel("");
		private SimpleTableModel	fieldTable			= new SimpleTableModel("Field", "Type","Description");
		private JTextArea			commentsArea		= new JTextArea();
		private JTable 				displayTable 		= new JTable(fieldTable);
		

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

		private static final long	serialVersionUID	= -4393026616049677944L;
		private JLabel				nameLabel			= new JLabel("");
		private JLabel				rowCountLabel		= new JLabel("");
		private DescriptionTextArea description			= new DescriptionTextArea ("");
		private SimpleTableModel	valueTable			= new SimpleTableModel("Value", "Frequency", "Fraction");
		private JTextArea			commentsArea		= new JTextArea();
		private Field				field;

		public FieldPanel() {
			setLayout(new BorderLayout());

			JPanel generalInfoPanel = new JPanel();
			
			generalInfoPanel.setLayout(new BorderLayout(5,5));
			
			generalInfoPanel.setBorder(BorderFactory.createTitledBorder("General information"));
			
			JPanel fieldInfo = new JPanel();
			fieldInfo.setLayout(new GridLayout(0,2));
			
			fieldInfo.add(new JLabel("Field name: "));
			fieldInfo.add(nameLabel);

			fieldInfo.add(new JLabel("Field type: "));
			fieldInfo.add(rowCountLabel);
			
			generalInfoPanel.add(fieldInfo,BorderLayout.NORTH);
			
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
			// Right align the frequency and percentage
			DefaultTableCellRenderer rightRenderer = new DefaultTableCellRenderer();
			rightRenderer.setHorizontalAlignment(SwingConstants.RIGHT);
			table.getColumnModel().getColumn(1).setCellRenderer(rightRenderer);
			table.getColumnModel().getColumn(2).setCellRenderer(rightRenderer);

			fieldListPanel.setBorder(BorderFactory.createTitledBorder("Fields"));
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
			rowCountLabel.setText(field.getType());
			description.setText(field.getDescription());
			
			// Hide description if it's empty
			description.getParent().setVisible(!description.getText().isEmpty());

			valueTable.clear();
			
			if (field.getValueCounts() != null) {
				int valueCountTotal = field.getRowsCheckedCount();

				for (String[] valueCount : field.getValueCounts()) {
					String valueNumber = valueCount[1];
					String valuePercent = "";
					if (StringUtilities.isNumber(valueNumber)) {
						double number = Double.parseDouble(valueNumber);
						valueNumber = numberFormat.format(number);
						double valueCountPercent = number / (double) valueCountTotal;
						if (valueCountPercent < 0.001) {
							valuePercent = "<" + percentageFormat.format(0.001);
						}
						else if (valueCountPercent > 0.99) {
							valuePercent = ">" + percentageFormat.format(0.99);
						}
						else {
							valuePercent = percentageFormat.format(valueCountPercent);
						}
					}
					valueTable.add(valueCount[0], valueNumber, valuePercent);
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

			// testTable.setMinimumSize(new Dimension(200, 200));
			// JScrollPane testPanel = new JScrollPane(testTable);
			// testPanel.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
			// add(testPanel, BorderLayout.SOUTH);
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
