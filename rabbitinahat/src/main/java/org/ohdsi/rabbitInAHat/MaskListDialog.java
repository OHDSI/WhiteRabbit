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


import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This Frame is used to select specific source tables
 * @author EIjo
 *
 */


public class MaskListDialog {
    private JLabel label;
    private JOptionPane optionPane;
    private JButton okButton, cancelButton;
    private ActionListener okEvent, cancelEvent, searchEvent;
    private JDialog dialog;
    private JScrollPane scrollPane;
    private JTextField searchField;
    private JButton searchSelectButton;
    private JButton searchDeselectButton;
    private JRadioButton matchCase, regexCase;
    private ButtonGroup searchRadioGroup;
    private JTable table;

    public MaskListDialog(List<String> listToDisplay, List<Integer> selectedIndices){

        DefaultListModel checkboxListModel = new DefaultListModel();
        JPanel listPanel = new JPanel();

        listPanel.setLayout(new BoxLayout(listPanel, BoxLayout.Y_AXIS));

        scrollPane = new JScrollPane();


        Object[] columnNames = {"source", "selected"};
        Object[][] data = new Object[listToDisplay.size()][2];

        for(int i = 0; i < listToDisplay.size(); i++){
            data[i][0] = listToDisplay.get(i);
            data[i][1] = selectedIndices.contains(i);
        }


        DefaultTableModel model = new DefaultTableModel(data, columnNames);
        table = new JTable(model) {
            /*@Override
            public Class getColumnClass(int column) {
            return getValueAt(0, column).getClass();
            }*/
            @Override
            public Class getColumnClass(int column) {
                if (column == 0) {
                    return String.class;
                }
                return Boolean.class;
            }
            @Override
            public boolean isCellEditable(int row, int column) {
                //Only the third column
                return column == 1;
            }
        };
        table.setEditingColumn(0);


        scrollPane = new JScrollPane(table);

        scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);


        label = new JLabel("select the source tables you would like to display");
        createAndDisplayOptionPane();
    }

    private void createAndDisplayOptionPane(){
        setupButtons();
        JPanel pane = layoutComponents();

        optionPane = new JOptionPane(pane);
        optionPane.setOptions(new Object[]{okButton, cancelButton});

        dialog = optionPane.createDialog("Hide Unwanted Tables");
    }

    private void setupButtons(){
        searchField = new JTextField(16);
        searchSelectButton = new JButton("Select");
        searchSelectButton.addActionListener(this::handleSelectField);
        searchDeselectButton = new JButton("Deselect");
        searchDeselectButton.addActionListener(this::handleDeselectField);


        matchCase = new JRadioButton("Match Case");
        matchCase.setSelected(true);
        regexCase = new JRadioButton("Regex");
        searchRadioGroup =new ButtonGroup();
        searchRadioGroup.add(matchCase);
        searchRadioGroup.add(regexCase);

        okButton = new JButton("Ok");
        okButton.addActionListener(this::handleOkButtonClick);

        cancelButton = new JButton("Cancel");
        cancelButton.addActionListener(this::handleCancelButtonClick);
    }

    private JPanel layoutComponents(){
        JPanel panel = new JPanel(new BorderLayout(5,5));
        panel.add(label, BorderLayout.NORTH);
        panel.add(scrollPane, BorderLayout.CENTER);


        JPanel searchPanel = new JPanel();
        searchPanel.setLayout(new FlowLayout(FlowLayout.CENTER));

        searchPanel.add(searchField);
        JPanel addRemoveButtonPanel = new JPanel();
        addRemoveButtonPanel.setLayout(new BoxLayout(addRemoveButtonPanel, BoxLayout.PAGE_AXIS));
        addRemoveButtonPanel.add(searchSelectButton);
        addRemoveButtonPanel.add(searchDeselectButton);
        searchPanel.add(addRemoveButtonPanel);

        JPanel caseButtonPanel = new JPanel();
        caseButtonPanel.setLayout(new BoxLayout(caseButtonPanel, BoxLayout.PAGE_AXIS));
        caseButtonPanel.add(matchCase);
        caseButtonPanel.add(regexCase);
        searchPanel.add(caseButtonPanel);

        panel.add(searchPanel, BorderLayout.SOUTH);

        return panel;
    }

    private void handleOkButtonClick(ActionEvent e){
        if(okEvent != null){ okEvent.actionPerformed(e); }
        hide();
    }

    private void handleCancelButtonClick(ActionEvent e){
        if(cancelEvent != null){ cancelEvent.actionPerformed(e);}
        hide();
    }

    private void handleSelectField(ActionEvent e){
        if(cancelEvent != null){ searchEvent.actionPerformed(e);}

        if(matchCase.isSelected()){
            toggleTableStatusMatchCase(true);
        } else if(regexCase.isSelected()){
            toggleTableStatusRegexCase(true);
        }
    }


    private void handleDeselectField(ActionEvent e) {
        if (cancelEvent != null) {
            searchEvent.actionPerformed(e);
        }
        if(matchCase.isSelected()){
            toggleTableStatusMatchCase(false);
        } else if(regexCase.isSelected()){
            toggleTableStatusRegexCase(false);
        }

    }

    // true to toggle elements on false to toggle off
    private void toggleTableStatusMatchCase(Boolean bool){
        for (int i = 0; i < table.getRowCount(); i++) {
            if (table.getModel().getValueAt(i, 0).toString().contains(searchField.getText())) {
                table.setValueAt(bool, i, 1);
            }
        }
    }


    // true to toggle elements on false to toggle off
    private void toggleTableStatusRegexCase(Boolean bool){
        Pattern pattern = Pattern.compile(searchField.getText());
        for (int i = 0; i < table.getRowCount(); i++) {
            Matcher matcher = pattern.matcher(table.getModel().getValueAt(i, 0).toString());
            if (matcher.find()) {
                table.setValueAt(bool, i, 1);
            }
        }
    }


    public void show(){ dialog.setVisible(true); }

    private void hide(){ dialog.setVisible(false); }


    public List<Integer> getSelectedIndices(){
        List<Integer> selectedIndices = new ArrayList<>();
        for(int i = 0; i < table.getRowCount(); i++){
            if((Boolean) table.getModel().getValueAt(i,1)){
                selectedIndices.add(i);
            }
        }
        return selectedIndices;
    }
}