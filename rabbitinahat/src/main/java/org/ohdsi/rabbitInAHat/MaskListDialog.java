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


import org.ohdsi.rabbitInAHat.dataModel.Table;
import javax.swing.*;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
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


public class MaskListDialog extends JDialog implements ActionListener, ResizeListener  {

    private static final long serialVersionUID = 7009265246652874341L;
    private static MaskListDialog instance;
    private MappingPanel mappingPanel;

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


    public final static String MATCH_BUTTON = "Match Button";
    public final static String REGEX_BUTTON = "Regex Button";

    public final static String SELECT_BUTTON = "Select Button";
    public final static String DESELECT_BUTTON = "Deselect Button";

    Container contentPane = this.getContentPane();

    public MaskListDialog(Window parentWindow) throws ExceptionInInitializerError{
        super(parentWindow,"Hide Tables",ModalityType.MODELESS);
        if (alreadyOpened()) {
            throw new ExceptionInInitializerError("An instance of FilterDialog already exists");
        }
        this.setResizable(false);
        this.setLocation(parentWindow.getX()+parentWindow.getWidth()/2, parentWindow.getY()+100);

        label = new JLabel("select the source tables you would like to display");

        contentPane.add(createPanel());

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
            instance.setVisible(true);
            instance.toFront();
        }
    }

    private JPanel createPanel() {
        setupTablePane();
        setupButtons();
        return layoutComponents();
    }

    private void setupTablePane() {
        JPanel listPanel = new JPanel();

        listPanel.setLayout(new BoxLayout(listPanel, BoxLayout.Y_AXIS));

        scrollPane = new JScrollPane();

        List<String> tableNames = new ArrayList<>(ObjectExchange.etl.getSourceDatabase().getUnmaskedTables().size());
        List<Integer> selectedIndices = ObjectExchange.etl.getSourceDatabase().getSelectedIndices();

        for(Table table : ObjectExchange.etl.getSourceDatabase().getUnmaskedTables()){
            tableNames.add(table.getName());
        }

        Object[] columnNames = {"source", "selected"};
        Object[][] data = new Object[selectedIndices.size()][2];

        for(int i = 0; i < tableNames.size(); i++){
            data[i][0] = tableNames.get(i);
            data[i][1] = selectedIndices.contains(i);
        }

        DefaultTableModel model = new DefaultTableModel(data, columnNames);
        table = new JTable(model) {
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
        table.getModel().addTableModelListener(new TableModelListener() {
            public void tableChanged(TableModelEvent e) {
                updateIndices();
            }
        });

        scrollPane = new JScrollPane(table);

        scrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
    }

    private void setupButtons() {
        searchField = new JTextField(32);
        searchSelectButton = new JButton("Select");
        searchSelectButton.addActionListener(this::handleSelectField);
        searchSelectButton.setName(SELECT_BUTTON);
        searchDeselectButton = new JButton("Deselect");
        searchDeselectButton.addActionListener(this::handleDeselectField);
        searchDeselectButton.setName(DESELECT_BUTTON);


        matchCase = new JRadioButton("Match Case");
        matchCase.setSelected(true);
        matchCase.setName(MATCH_BUTTON);
        regexCase = new JRadioButton("Regex");
        regexCase.setName(REGEX_BUTTON);
        searchRadioGroup = new ButtonGroup();
        searchRadioGroup.add(matchCase);
        searchRadioGroup.add(regexCase);

        okButton = new JButton("Ok");
        okButton.addActionListener(this::handleOkButtonClick);

        cancelButton = new JButton("Cancel");
        cancelButton.addActionListener(this::handleCancelButtonClick);
    }

    private JPanel layoutComponents() {
        JPanel panel = new JPanel(new BorderLayout(5, 5));
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


    public void setMaskListPanel(MappingPanel aMaskListPanel){
        if (mappingPanel != null) {
            mappingPanel.removeResizeListener(this);
        }

        mappingPanel = aMaskListPanel;

        if (aMaskListPanel != null) {
            aMaskListPanel.addResizeListener(this);
        }

    }


    private void handleOkButtonClick(ActionEvent e){
        if(okEvent != null){ okEvent.actionPerformed(e); }
        instance.setVisible(false);
    }

    private void handleCancelButtonClick(ActionEvent e){
        if(cancelEvent != null){ cancelEvent.actionPerformed(e);}
        instance.setVisible(false);
    }

    private void handleSelectField(ActionEvent e){
        if(cancelEvent != null){ searchEvent.actionPerformed(e);}

        if(matchCase.isSelected()){
            toggleTableStatusMatchCase(true);
        } else if(regexCase.isSelected()){
            toggleTableStatusRegexCase(true);
        }
        updateIndices();
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
        updateIndices();
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

    private void updateIndices(){
        ObjectExchange.etl.getSourceDatabase().setSelectedIndices(getSelectedIndices());
        mappingPanel.setMapping(ObjectExchange.etl.getTableToTableMapping());
    }

    public List<Integer> getSelectedIndices(){
        List<Integer> selectedIndices = new ArrayList<>();
        for(int i = 0; i < table.getRowCount(); i++){
            if((Boolean) table.getModel().getValueAt(i,1)){
                selectedIndices.add(i);
            }
        }
        return selectedIndices;
    }

    @Override
    public void actionPerformed(ActionEvent actionEvent) {
    }

    @Override
    public void notifyResized(int height, boolean minimized, boolean maximized) {

    }

}