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
package org.ohdsi.rabbitInAHat;

import java.awt.Container;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import org.ohdsi.rabbitInAHat.ResizeListener;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.SpringLayout;
import javax.swing.WindowConstants;

public class FilterDialog extends JDialog implements ActionListener, ResizeListener {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7009265246652874341L;
	private static FilterDialog instance;
	
	
	private JTextField		sourceSearchField; 
	private JTextField		targetSearchField; 
	private MappingPanel	filterPanel;

	SpringLayout layout = new SpringLayout();
	
	Container contentPane = this.getContentPane();
	
	public FilterDialog(Window parentWindow) throws ExceptionInInitializerError{
		super(parentWindow,"Filter",ModalityType.MODELESS);
		if (alreadyOpened()) {
			throw new ExceptionInInitializerError("An instance of FilterDialog already exists");
		}
		this.setResizable(false);
		this.setLocation(parentWindow.getX()+parentWindow.getWidth()/2, parentWindow.getY()+100);

		contentPane.setLayout(layout);
		sourceSearchField = new JTextField(30);
		sourceSearchField.setName("Source");
		
		targetSearchField = new JTextField(30);
		targetSearchField.setName("Target");

		
		// Add key listener to send search string as it's being typed
		sourceSearchField.addKeyListener(new SearchListener() );
		sourceSearchField.addFocusListener(new SearchFocusListener());		
		JLabel sourceLabel = new JLabel("Filter Source:",JLabel.TRAILING);
		JButton sourceClearBtn = new JButton("Clear");
		contentPane.add(sourceLabel);
		contentPane.add(sourceSearchField);
		sourceClearBtn.addActionListener(this);
		sourceClearBtn.setActionCommand("Clear Source");
		sourceClearBtn.setFocusable(false);
		contentPane.add(sourceClearBtn);
		
		targetSearchField.addKeyListener(new SearchListener() );
		targetSearchField.addFocusListener(new SearchFocusListener());
		JLabel targetLabel = new JLabel("Filter Target:",JLabel.TRAILING);
		JButton targetClearBtn = new JButton("Clear");
		contentPane.add(targetLabel);
		contentPane.add(targetSearchField);		
		targetClearBtn.addActionListener(this);
		targetClearBtn.setActionCommand("Clear Target");
		targetClearBtn.setFocusable(false);
		contentPane.add(targetClearBtn);
		
		layout.putConstraint(SpringLayout.WEST, sourceLabel, 5, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, sourceLabel, 5, SpringLayout.NORTH, contentPane);
		
		layout.putConstraint(SpringLayout.WEST, sourceSearchField, 5, SpringLayout.EAST, sourceLabel);
		layout.putConstraint(SpringLayout.NORTH, sourceSearchField, 5, SpringLayout.NORTH, contentPane);
		
		layout.putConstraint(SpringLayout.WEST, sourceClearBtn, 5, SpringLayout.EAST, sourceSearchField);
		layout.putConstraint(SpringLayout.NORTH, sourceClearBtn, 5, SpringLayout.NORTH, contentPane);
		
		layout.putConstraint(SpringLayout.WEST, targetLabel, 5, SpringLayout.WEST, contentPane);
		layout.putConstraint(SpringLayout.NORTH, targetLabel, 10, SpringLayout.SOUTH, sourceLabel);
			
		layout.putConstraint(SpringLayout.WEST, targetSearchField, 0, SpringLayout.WEST, sourceSearchField);
		layout.putConstraint(SpringLayout.NORTH, targetSearchField, 0, SpringLayout.NORTH, targetLabel);
		
		layout.putConstraint(SpringLayout.WEST, targetClearBtn, 5, SpringLayout.EAST, targetSearchField);
		layout.putConstraint(SpringLayout.NORTH, targetClearBtn, 0, SpringLayout.NORTH, targetSearchField);		

		
		layout.putConstraint(SpringLayout.SOUTH, contentPane, 5, SpringLayout.SOUTH, targetLabel);
		layout.putConstraint(SpringLayout.NORTH, contentPane, 5, SpringLayout.NORTH, sourceLabel);
		layout.putConstraint(SpringLayout.WEST, contentPane, 5, SpringLayout.WEST, sourceLabel);
		layout.putConstraint(SpringLayout.EAST, contentPane, 5, SpringLayout.EAST, targetClearBtn);
		
		this.pack();

		// Make sure the instance is reset to null when the search dialog window is closed
		this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
		this.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent arg0) {
				instance = null;
			}
		});

		instance = this;
	}

	public static boolean alreadyOpened(){
		return (instance != null);
	}

	public static void bringToFront(){
		if (alreadyOpened()) {
			instance.toFront();
		}
	}
	
	public void setFilterPanel(MappingPanel aFilterPanel){
		if (filterPanel != null) {
			filterPanel.removeResizeListener(this);
		}
		
		filterPanel = aFilterPanel;
		
		if (filterPanel != null) {
			aFilterPanel.addResizeListener(this);
		}
		
		setSearchFieldsToLastSearch();
	}
	
	public MappingPanel getFilterPanel(){
		if(filterPanel.isMinimized()){
			return filterPanel.getSlaveMappingPanel();
		}else{
			return filterPanel;
		}
	}
	
	public void doFilterPanel(String str,String panelName){		
		getFilterPanel().filterComponents(str,panelName=="Target");
	}
	
	@Override
	public void actionPerformed(ActionEvent event) {
		switch (event.getActionCommand()) {
			case "Clear Source":
				clearSourceFilter();
				break;
			
			case "Clear Target":
				clearTargetFilter();
				break;
				
		}
		
	}

	private void clearTargetFilter() {
		targetSearchField.setText("");
		doFilterPanel("","Target");
	}
	
	private void clearSourceFilter() {
		sourceSearchField.setText("");
		doFilterPanel("","Source");
	}
	
	private void setSearchFieldsToLastSearch(){
		sourceSearchField.setText(getFilterPanel().getLastSourceFilter());			
		targetSearchField.setText(getFilterPanel().getLastTargetFilter());
	}
	public void notifyResized(int height, boolean minimized, boolean maximized) {
		setSearchFieldsToLastSearch();
	}
	
	class SearchListener implements KeyListener{
			private String searchStrBuffer = "";
			
			@Override
			public void keyPressed(KeyEvent event) {						
				
			}

			@Override
			public void keyReleased(KeyEvent event) {
				
				switch (event.getComponent().getName()) {
					case "Source":
						searchStrBuffer = sourceSearchField.getText();
						break;
					case "Target":
						searchStrBuffer = targetSearchField.getText();
						break;
				}
				
				doFilterPanel(searchStrBuffer,event.getComponent().getName());
			}

			@Override
			public void keyTyped(KeyEvent event) {
				
			}
		
	}
	
	public class SearchFocusListener implements FocusListener {

		@Override
		public void focusGained(FocusEvent e) {
			// TODO Auto-generated method stub
			JTextField jtf = (JTextField) e.getComponent();
			jtf.selectAll();
		}

		@Override
		public void focusLost(FocusEvent e) {
			// TODO Auto-generated method stub

		}

	}
	
	
}

