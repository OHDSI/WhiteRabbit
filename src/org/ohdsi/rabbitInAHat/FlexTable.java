/*******************************************************************************
 * Copyright 2016 Observational Health Data Sciences and Informatics
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

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.swing.JComponent;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.KeyStroke;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.TableModel;

public class FlexTable extends JTable implements ActionListener {
	
	private static final long	serialVersionUID	= 3017125804758269739L;
	
	private String				rowstring, value;
	private Clipboard			system;
	private StringSelection		stsel;
	private FlexTableModel		model				= new FlexTableModel();
	
	public FlexTable() {
		super();
		setModel(model);
		model.addTableModelListener(this);
		KeyStroke copy = KeyStroke.getKeyStroke(KeyEvent.VK_C, ActionEvent.CTRL_MASK, false);
		KeyStroke paste = KeyStroke.getKeyStroke(KeyEvent.VK_V, ActionEvent.CTRL_MASK, false);
		registerKeyboardAction(this, "Copy", copy, JComponent.WHEN_FOCUSED);
		registerKeyboardAction(this, "Paste", paste, JComponent.WHEN_FOCUSED);
		system = Toolkit.getDefaultToolkit().getSystemClipboard();
	}
	
	public void actionPerformed(ActionEvent e) {
		if (e.getActionCommand().compareTo("Copy") == 0) {
			StringBuilder stringBuilder = new StringBuilder();
			int numcols = getSelectedColumnCount();
			int numrows = getSelectedRowCount();
			int[] rowsselected = getSelectedRows();
			int[] colsselected = getSelectedColumns();
			if (!((numrows - 1 == rowsselected[rowsselected.length - 1] - rowsselected[0] && numrows == rowsselected.length) && (numcols - 1 == colsselected[colsselected.length - 1]
					- colsselected[0] && numcols == colsselected.length))) {
				JOptionPane.showMessageDialog(null, "Invalid Copy Selection", "Invalid Copy Selection", JOptionPane.ERROR_MESSAGE);
				return;
			}
			for (int i = 0; i < numrows; i++) {
				for (int j = 0; j < numcols; j++) {
					stringBuilder.append(getValueAt(rowsselected[i], colsselected[j]));
					if (j < numcols - 1)
						stringBuilder.append("\t");
				}
				stringBuilder.append("\n");
			}
			stsel = new StringSelection(stringBuilder.toString());
			system = Toolkit.getDefaultToolkit().getSystemClipboard();
			system.setContents(stsel, stsel);
		}
		if (e.getActionCommand().compareTo("Paste") == 0) {
			int startRow = (getSelectedRows())[0];
			int startCol = (getSelectedColumns())[0];
			try {
				String trstring = (String) (system.getContents(this).getTransferData(DataFlavor.stringFlavor));
				StringTokenizer tokenizer = new StringTokenizer(trstring, "\n");
				for (int i = 0; tokenizer.hasMoreTokens(); i++) {
					rowstring = tokenizer.nextToken();
					StringTokenizer tokenizer2 = new StringTokenizer(rowstring, "\t");
					for (int j = 0; tokenizer2.hasMoreTokens(); j++) {
						value = (String) tokenizer2.nextToken();
						setValueAt(value, startRow + i, startCol + j);
					}
				}
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
	
	private class FlexTableModel implements TableModel {
		private List<TableModelListener>	listeners	= new ArrayList<TableModelListener>();
		private List<List<String>>			rows		= new ArrayList<List<String>>();
		private int							columnCount	= 0;
		
		private void notifyListeners() {
			for (TableModelListener listener : listeners)
				listener.tableChanged(new TableModelEvent(this, TableModelEvent.HEADER_ROW));
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
			return columnCount + 1;
		}
		
		@Override
		public String getColumnName(int columnIndex) {
			return null;
		}
		
		@Override
		public int getRowCount() {
			return rows.size() + 1;
		}
		
		@Override
		public Object getValueAt(int rowIndex, int columnIndex) {
			if (rowIndex >= rows.size())
				return "";
			List<String> row = rows.get(rowIndex);
			if (row.size() <= columnIndex)
				return "";
			else
				return row.get(columnIndex);
		}
		
		@Override
		public boolean isCellEditable(int rowIndex, int columnIndex) {
			return true;
		}
		
		@Override
		public void removeTableModelListener(TableModelListener l) {
			listeners.remove(l);
		}
		
		@Override
		public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
			for (int i = rows.size(); i <= rowIndex; i++)
				rows.add(new ArrayList<String>());
			List<String> row = rows.get(rowIndex);
			for (int i = row.size(); i <= columnIndex; i++)
				row.add("");
			
			row.set(columnIndex, aValue.toString());
			if (columnIndex >= columnCount)
				columnCount = columnIndex + 1;
			notifyListeners();
		}
		
	}
}
