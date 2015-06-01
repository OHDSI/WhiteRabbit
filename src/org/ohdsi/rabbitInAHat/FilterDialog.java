package org.ohdsi.rabbitInAHat;

import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import org.ohdsi.rabbitInAHat.ResizeListener;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.SpringLayout;

public class FilterDialog extends JDialog implements ActionListener, ResizeListener {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7009265246652874341L;
	
	
	private JTextField		sourceSearchField; 
	private JTextField		targetSearchField; 
	private MappingPanel	filterPanel;

	SpringLayout layout = new SpringLayout();
	
	JPanel mainView = new JPanel(layout);
	
	public FilterDialog(Window parentWindow){
		
		super(parentWindow,"Filter Results",ModalityType.MODELESS);		
						
		this.setLocation(parentWindow.getX()+parentWindow.getWidth()/2, parentWindow.getY()+100);
		this.setSize(700,120);
		
		sourceSearchField = new JTextField(40);
		sourceSearchField.setName("Source");
		
		targetSearchField = new JTextField(40);
		targetSearchField.setName("Target");

		
		// Add key listener to send search string as it's being typed
		sourceSearchField.addKeyListener(new SearchListener() );
		JLabel sourceLabel = new JLabel("Filter Source:",JLabel.TRAILING);
		JButton sourceClearBtn = new JButton("Clear");
		mainView.add(sourceLabel);
		mainView.add(sourceSearchField);
		sourceClearBtn.addActionListener(this);
		sourceClearBtn.setActionCommand("Clear Source");
		mainView.add(sourceClearBtn);
		
		targetSearchField.addKeyListener(new SearchListener() );
		JLabel targetLabel = new JLabel("Filter Target:",JLabel.TRAILING);
		JButton targetClearBtn = new JButton("Clear");
		mainView.add(targetLabel);
		mainView.add(targetSearchField);		
		targetClearBtn.addActionListener(this);
		targetClearBtn.setActionCommand("Clear Target");
		mainView.add(targetClearBtn);
		
		layout.putConstraint(SpringLayout.WEST, sourceLabel, 10, SpringLayout.WEST, mainView);
		layout.putConstraint(SpringLayout.NORTH, sourceLabel, 10, SpringLayout.NORTH, mainView);
		
		layout.putConstraint(SpringLayout.WEST, sourceSearchField, 5, SpringLayout.EAST, sourceLabel);
		layout.putConstraint(SpringLayout.NORTH, sourceSearchField, 10, SpringLayout.NORTH, mainView);
		
		layout.putConstraint(SpringLayout.WEST, sourceClearBtn, 5, SpringLayout.EAST, sourceSearchField);
		layout.putConstraint(SpringLayout.NORTH, sourceClearBtn, 10, SpringLayout.NORTH, mainView);
		
		layout.putConstraint(SpringLayout.WEST, targetLabel, 10, SpringLayout.WEST, mainView);
		layout.putConstraint(SpringLayout.NORTH, targetLabel, 10, SpringLayout.SOUTH, sourceLabel);
		
		layout.putConstraint(SpringLayout.WEST, targetSearchField, 0, SpringLayout.WEST, sourceSearchField);
		layout.putConstraint(SpringLayout.NORTH, targetSearchField, 0, SpringLayout.NORTH, targetLabel);
		
		layout.putConstraint(SpringLayout.WEST, targetClearBtn, 5, SpringLayout.EAST, targetSearchField);
		layout.putConstraint(SpringLayout.NORTH, targetClearBtn, 0, SpringLayout.NORTH, targetSearchField);
		
		this.add(mainView);
	};
	
	public void setFilterPanel(MappingPanel aFilterPanel){
		if (filterPanel != null) {
			filterPanel.removeResizeListener(this);
		}
		
		filterPanel = aFilterPanel;
		
		if (filterPanel != null) {
			aFilterPanel.addResizeListener(this);
		}
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
	
	public void notifyResized(int height, boolean minimized, boolean maximized) {
		clearSourceFilter();
		clearTargetFilter();
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
	
	
}

