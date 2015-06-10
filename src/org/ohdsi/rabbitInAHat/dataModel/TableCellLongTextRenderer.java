package org.ohdsi.rabbitInAHat.dataModel;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JTable;  
import javax.swing.JTextArea;  
import javax.swing.UIManager;
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
		
        if(isSelected){
        	jtext.setBackground((Color)UIManager.get("Table.selectionBackground"));
        }
      	
		return jtext;
	}
	
	

}
