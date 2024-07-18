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
import java.awt.*;
import java.awt.event.*;
import java.util.HashSet;

/**
 * This Frame is used to select specific source tables
 * @author EIjo
 *
 */
//public class MaskFrame extends JFrame {
//
//    final int MAX = 10;
//    // initialize list elements
//    String[] listElems = new String[MAX];
//    public void test1() {
//        JOptionPane.showInputDialog(null, "Please choose a name", "Example 1",
//                JOptionPane.QUESTION_MESSAGE, null, new Object[]{"Amanda",
//                        "Colin", "Don", "Fred", "Gordon", "Janet", "Jay",
//                        "Joe", "Judie", "Kerstin", "Lotus", "Maciek", "Mark",
//                        "Mike", "Mulhern", "Oliver", "Peter", "Quaxo", "Rita",
//                        "Sandro", "Tim", "Will"}, "Joe");
//    }
//    public void test(){
//
//    }
//
//    public void  init(){
//        for (int i = 0; i < MAX; i++) {
//            listElems[i] = "element " + i;
//        }
//        final JList list = new JList(listElems);
//        final JScrollPane pane = new JScrollPane(list);
//        final JFrame frame = new JFrame("JList Demo");
//
//        // create a button and add action listener
//        final JButton btnGet = new JButton("Get Selected");
//        btnGet.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent e) {
//                String selectedElem = "";
//                int selectedIndices[] = list.getSelectedIndices();
//                for (int j = 0; j < selectedIndices.length; j++) {
//                    String elem =
//                            (String) list.getModel().getElementAt(selectedIndices[j]);
//                    selectedElem += "\n" + elem;
//
//                }
//                JOptionPane.showMessageDialog(frame,
//                        "You've selected:" + selectedElem);
//            }// end actionPerformed
//        });

//        frame.setLayout(new BorderLayout());
//        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        frame.getContentPane().add(pane, BorderLayout.CENTER);
//        frame.getContentPane().add(btnGet, BorderLayout.SOUTH);
//        frame.setSize(250, 200);
//        frame.setVisible(true);
//    }
//}


public class MaskListDialog {
    private JList list;
    private JLabel label;
    private JOptionPane optionPane;
    private JButton okButton, cancelButton;
    private ActionListener okEvent, cancelEvent, searchEvent;
    private JDialog dialog;
    private JScrollPane scrollPane;
    private JTextField searchField;
    private JButton searchSubmitButton;
    private JRadioButton matchCase, regexCase;
    private ButtonGroup searchRadioGroup;

    public MaskListDialog(String message, JList listToDisplay){

        list = listToDisplay;
        scrollPane = new JScrollPane(list);
        label = new JLabel(message);
        createAndDisplayOptionPane();
    }

//    public MaskListDialog(String title, String message, JList listToDisplay){
//        this(message, listToDisplay);
//        dialog.setTitle(title);
//    }

    private void createAndDisplayOptionPane(){
        setupButtons();
        JPanel pane = layoutComponents();

        JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.PAGE_AXIS));
        buttonPanel.add(regexCase);
        buttonPanel.add(matchCase);
//        pane.add(buttonPanel, BorderLayout.EAST);


        optionPane = new JOptionPane(pane);
        optionPane.setOptions(new Object[]{searchField, searchSubmitButton, okButton, cancelButton});
        optionPane.add(buttonPanel, BorderLayout.EAST);
        dialog = optionPane.createDialog("Select option");
    }

    private void setupButtons(){
        searchField = new JTextField(16);
        searchSubmitButton = new JButton("submit");
        searchSubmitButton.addActionListener(this::handleSearchField);

        matchCase = new JRadioButton("Match Case");
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
        centerListElements();
        JPanel panel = new JPanel(new BorderLayout(5,5));
        panel.add(label, BorderLayout.NORTH);
        panel.add(scrollPane, BorderLayout.CENTER);

        return panel;
    }

    private void centerListElements(){
        DefaultListCellRenderer renderer = (DefaultListCellRenderer) list.getCellRenderer();
        renderer.setHorizontalAlignment(SwingConstants.CENTER);
    }

//    public void setOnOk(ActionListener event){
//        okEvent = event;
//    }
//
//    public void setOnClose(ActionListener event){
//        cancelEvent  = event;
//    }
//
//    public void setOnSearch(ActionListener event){
//        searchEvent  = event;
//    }


    private void handleOkButtonClick(ActionEvent e){
        if(okEvent != null){ okEvent.actionPerformed(e); }
        hide();
    }

    private void handleCancelButtonClick(ActionEvent e){
        if(cancelEvent != null){ cancelEvent.actionPerformed(e);}
        hide();
    }

    private void handleSearchField(ActionEvent e){
        if(cancelEvent != null){ searchEvent.actionPerformed(e);}

        HashSet<Integer> newIndices = new HashSet<Integer>();
        for(int index : list.getSelectedIndices()){
            newIndices.add(index);
        }

        for(int i = 0; i < list.getModel().getSize(); i ++){
            if (list.getModel().getElementAt(i).toString().contains(searchField.getText())){
                newIndices.add(i);
            }
        }
        list.setSelectedIndices(newIndices.stream().mapToInt(Number::intValue).toArray());
    }
    public void show(){ dialog.setVisible(true); }

    private void hide(){ dialog.setVisible(false); }

    public int[] getSelectedIndices(){
        return list.getSelectedIndices();
    }
}