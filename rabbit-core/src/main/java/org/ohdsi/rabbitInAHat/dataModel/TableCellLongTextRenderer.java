/*******************************************************************************
 * Copyright 2023 Observational Health Data Sciences and Informatics
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
package org.ohdsi.rabbitInAHat.dataModel;

import java.awt.Component;
import java.awt.Insets;

import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;


/**
 * Setups the table to properly wrap in the table
 * 
 *
 * @author Paul Zepernick
 */
public class TableCellLongTextRenderer extends DefaultTableCellRenderer implements TableCellRenderer{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7920163334647774178L;

	@Override
	public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
		final JTextArea jtext = new JTextArea();
		jtext.setText((String)value);
		jtext.setWrapStyleWord(true);                    
		jtext.setLineWrap(true);   
		jtext.setFont(table.getFont());
		jtext.setSize(table.getColumn(table.getColumnName(column)).getWidth(), (int)jtext.getPreferredSize().getHeight());
		
		jtext.setMargin(new Insets(10,5,10,5));
      	
		return jtext;
	}
	
	

}
