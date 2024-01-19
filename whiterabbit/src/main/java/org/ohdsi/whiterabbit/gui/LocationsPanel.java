/*******************************************************************************
 * Copyright 2023 Observational Health Data Sciences and Informatics & The Hyve
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
package org.ohdsi.whiterabbit.gui;

import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.databases.configuration.DBConfiguration;
import org.ohdsi.whiterabbit.PanelsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.*;
import java.awt.event.ItemEvent;
import java.io.File;
import java.util.Objects;

import static org.ohdsi.whiterabbit.WhiteRabbitMain.LABEL_TEST_CONNECTION;

public class LocationsPanel extends JPanel {

    static Logger logger = LoggerFactory.getLogger(LocationsPanel.class);

    public static final String LABEL_LOCATIONS = "Locations";
    public static final String LABEL_SERVER_LOCATION = "Server location";
    public static final String NAME_SERVER_LOCATION = "ServerLocation";
    public static final String LABEL_USER_NAME = "User name";
    public static final String NAME_USER_NAME = "UserName";
    public static final String LABEL_PASSWORD = "Password";
    public static final String NAME_PASSWORD = "PasswordName";
    public static final String LABEL_DATABASE_NAME = "Database name";
    public static final String NAME_DATABASE_NAME = "DatabaseName";
    public static final String LABEL_DELIMITER = "Delimiter";
    public static final String NAME_DELIMITER = "DelimiterName";

    public static final String TOOLTIP_POSTGRESQL_SERVER = "For PostgreSQL servers this field contains the host name and database name (<host>/<database>)";

    private final JFrame parentFrame;
    private JTextField folderField;
    private JComboBox<String> sourceType;
    private JTextField sourceDelimiterField;
    private JTextField sourceServerField;
    private JTextField sourceUserField;
    private JTextField sourcePasswordField;
    private JTextField sourceDatabaseField;
    private DbType currentDbType = null;


    private SourcePanel sourcePanel;
    private boolean sourceIsFiles = true;
    private boolean sourceIsSas = false;

    private final transient PanelsManager panelsManager;

    public LocationsPanel(JFrame parentFrame, PanelsManager panelsManager) {
        super();
        this.parentFrame = parentFrame;
        this.panelsManager = panelsManager;
        this.createLocationsPanel();
    }

    private void createLocationsPanel() {
        JPanel panel = this;
        panel.setName(LABEL_LOCATIONS);

        panel.setLayout(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.weightx = 0.5;
        c.weighty = 0.8;

        JPanel folderPanel = new JPanel();
        folderPanel.setLayout(new BoxLayout(folderPanel, BoxLayout.X_AXIS));
        folderPanel.setBorder(BorderFactory.createTitledBorder("Working folder"));
        folderField = new JTextField();
        folderField.setName("FolderField");
        folderField.setText((new File("").getAbsolutePath()));
        folderField.setToolTipText("The folder where all output will be written");
        folderPanel.add(folderField);
        JButton pickButton = new JButton("Pick folder");
        pickButton.setToolTipText("Pick a different working folder");
        folderPanel.add(pickButton);
        pickButton.addActionListener(e -> pickFolder());
        panelsManager.getComponentsToDisableWhenRunning().add(pickButton);
        c.gridx = 0;
        c.gridy = 0;
        c.gridwidth = 1;
        panel.add(folderPanel, c);


        c.gridx = 0;
        c.gridy = 1;
        c.gridwidth = 1;
        this.sourcePanel = createSourcePanel();

        // make sure the sourcePanel has usable content by default
        createDatabaseFields(DbType.DELIMITED_TEXT_FILES.label());
        sourceType.setSelectedItem(DbType.DELIMITED_TEXT_FILES.label());

        panel.add(this.sourcePanel, c);

        JPanel testConnectionButtonPanel = new JPanel();
        testConnectionButtonPanel.setLayout(new BoxLayout(testConnectionButtonPanel, BoxLayout.X_AXIS));
        testConnectionButtonPanel.add(Box.createHorizontalGlue());

        JButton testConnectionButton = new JButton(LABEL_TEST_CONNECTION);
        testConnectionButton.setName(LABEL_TEST_CONNECTION);
        testConnectionButton.setBackground(new Color(151, 220, 141));
        testConnectionButton.setToolTipText("Test the connection");
        testConnectionButton.addActionListener(e -> this.runConnectionTest());
        panelsManager.getComponentsToDisableWhenRunning().add(testConnectionButton);
        testConnectionButtonPanel.add(testConnectionButton);

        c.gridx = 0;
        c.gridy = 2;
        c.gridwidth = 1;
        panel.add(testConnectionButtonPanel, c);
    }

    private void runConnectionTest() {
            panelsManager.runConnectionTest();

    }

    private void createDatabaseFields(ItemEvent itemEvent) {
        String selectedSourceType = itemEvent.getItem().toString();

        // remove existing DB related fields in sourcePanel
        sourcePanel.clear();

       currentDbType = DbType.getDbType(selectedSourceType);
        if (currentDbType.supportsStorageHandler()) {
            createDatabaseFields();
        } else {
            createDatabaseFields(selectedSourceType);
        }
        if (panelsManager.getAddAllButton() != null) {
            panelsManager.getAddAllButton().setEnabled(sourceIsDatabase(selectedSourceType));
        }
        this.revalidate();
    }

    @FunctionalInterface
    public interface SimpleDocumentListener extends DocumentListener {
        void update(DocumentEvent e);

        @Override
        default void insertUpdate(DocumentEvent e) {
            update(e);
        }
        @Override
        default void removeUpdate(DocumentEvent e) {
            update(e);
        }
        @Override
        default void changedUpdate(DocumentEvent e) {
            update(e);
        }
    }

    private void createDatabaseFields() {
        DBConfiguration currentConfiguration = this.currentDbType.getStorageHandler().getDBConfiguration();
        currentConfiguration.getFields().forEach(f -> {
            sourcePanel.addReplacable(new JLabel(f.label));
            JTextField field = new JTextField(f.getValueOrDefault());
            field.setName(f.name);
            field.setToolTipText(f.toolTip);
            sourcePanel.addReplacable(field);
            field.setEnabled(true);
            field.getDocument().addDocumentListener((SimpleDocumentListener) e -> {
                f.setValue(field.getText());
            });
        });
    }

    private boolean sourceIsFiles(String sourceType) {
        return sourceType.equalsIgnoreCase(DbType.DELIMITED_TEXT_FILES.label());
    }

    private boolean sourceIsSas(String sourceType) {
        return sourceType.equalsIgnoreCase(DbType.SAS7BDAT.label());
    }

    private boolean sourceIsDatabase(String sourceType) {
        return (!sourceIsFiles(sourceType) && !sourceIsSas(sourceType));
    }

    private void createDatabaseFields(String selectedSourceType) {
        sourceIsFiles = sourceIsFiles(selectedSourceType);
        sourceIsSas = sourceIsSas(selectedSourceType);
        boolean sourceIsDatabase = sourceIsDatabase(selectedSourceType);

        sourcePanel.addReplacable(new JLabel(LABEL_SERVER_LOCATION));
        sourceServerField = new JTextField("127.0.0.1");
        sourceServerField.setName(LABEL_SERVER_LOCATION);
        sourceServerField.setEnabled(false);
        sourcePanel.addReplacable(sourceServerField);
        sourcePanel.addReplacable(new JLabel(LABEL_USER_NAME));
        sourceUserField = new JTextField("");
        sourceUserField.setName(LABEL_USER_NAME);
        sourceUserField.setEnabled(false);
        sourcePanel.addReplacable(sourceUserField);
        sourcePanel.addReplacable(new JLabel(LABEL_PASSWORD));
        sourcePasswordField = new JPasswordField("");
        sourcePasswordField.setName(LABEL_PASSWORD);
        sourcePasswordField.setEnabled(false);
        sourcePanel.addReplacable(sourcePasswordField);
        sourcePanel.addReplacable(new JLabel(LABEL_DATABASE_NAME));
        sourceDatabaseField = new JTextField("");
        sourceDatabaseField.setName(LABEL_DATABASE_NAME);
        sourceDatabaseField.setEnabled(false);
        sourcePanel.addReplacable(sourceDatabaseField);

        sourcePanel.addReplacable(new JLabel(LABEL_DELIMITER));
        JTextField delimiterField = new JTextField(",");
        delimiterField.setName(NAME_DELIMITER);
        sourceDelimiterField = delimiterField;
        sourceDelimiterField.setToolTipText("The delimiter that separates values. Enter 'tab' for tab.");
        sourcePanel.addReplacable(sourceDelimiterField);
        sourceServerField.setEnabled(sourceIsDatabase);
        sourceUserField.setEnabled(sourceIsDatabase);
        sourcePasswordField.setEnabled(sourceIsDatabase);
        sourceDatabaseField.setEnabled(sourceIsDatabase && !selectedSourceType.equals(DbType.AZURE.label()));
        sourceDelimiterField.setEnabled(sourceIsFiles);

        if (sourceIsDatabase) {
            if (selectedSourceType.equals(DbType.ORACLE.label())) {
                sourceServerField.setToolTipText("For Oracle servers this field contains the SID, servicename, and optionally the port: '<host>/<sid>', '<host>:<port>/<sid>', '<host>/<service name>', or '<host>:<port>/<service name>'");
                sourceUserField.setToolTipText("For Oracle servers this field contains the name of the user used to log in");
                sourcePasswordField.setToolTipText("For Oracle servers this field contains the password corresponding to the user");
                sourceDatabaseField.setToolTipText("For Oracle servers this field contains the schema (i.e. 'user' in Oracle terms) containing the source tables");
            } else if (selectedSourceType.equals(DbType.POSTGRESQL.label())) {
                sourceServerField.setToolTipText(TOOLTIP_POSTGRESQL_SERVER);
                sourceUserField.setToolTipText("The user used to log in to the server");
                sourcePasswordField.setToolTipText("The password used to log in to the server");
                sourceDatabaseField.setToolTipText("For PostgreSQL servers this field contains the schema containing the source tables");
            } else if (selectedSourceType.equals(DbType.BIGQUERY.label())) {
                sourceServerField.setToolTipText("GBQ SA & UA:  ProjectID");
                sourceUserField.setToolTipText("GBQ SA only: OAuthServiceAccountEMAIL");
                sourcePasswordField.setToolTipText("GBQ SA only: OAuthPvtKeyPath");
                sourceDatabaseField.setToolTipText("GBQ SA & UA: Data Set within ProjectID");
            } else {
                if (selectedSourceType.equals(DbType.AZURE.label())) {
                    sourceServerField.setToolTipText("For Azure, this field contains the host name and database name (<host>;database=<database>)");
                } else {
                    sourceServerField.setToolTipText("This field contains the name or IP address of the database server");
                }
                if (selectedSourceType.equals(DbType.SQL_SERVER.label())) {
                    sourceUserField.setToolTipText("The user used to log in to the server. Optionally, the domain can be specified as <domain>/<user> (e.g. 'MyDomain/Joe')");
                } else {
                    sourceUserField.setToolTipText("The user used to log in to the server");
                }
                sourcePasswordField.setToolTipText("The password used to log in to the server");
                if (selectedSourceType.equals(DbType.AZURE.label())) {
                    sourceDatabaseField.setToolTipText("For Azure, leave this empty");
                } else {
                    sourceDatabaseField.setToolTipText("The name of the database containing the source tables");
                }
            }
        }
    }

    private SourcePanel createSourcePanel() {
        SourcePanel sourcePanel = new SourcePanel();
        sourcePanel.setLayout(new GridLayout(0, 2));
        sourcePanel.setBorder(BorderFactory.createTitledBorder("Source data location"));
        sourcePanel.add(new JLabel("Data type"));
        sourceType = new JComboBox<>(DbType.pickList());
        sourceType.setName("SourceType");
        sourceType.setToolTipText("Select the type of source data available");
        sourceType.addItemListener(event -> {
            if (event.getStateChange() == ItemEvent.SELECTED) {
                createDatabaseFields(event);
            }
        });
        sourcePanel.add(sourceType);

        return sourcePanel;
    }

    public JTextField getFolderField() {
        return folderField;
    }

    public String getSelectedSourceType() {
            return Objects.requireNonNull(sourceType.getSelectedItem()).toString();
    }

    public JTextField getSourceDelimiterField() {
        return sourceDelimiterField;
    }

    public boolean sourceIsFiles() {
        return sourceIsFiles;
    }

    public boolean sourceIsSas() {
        return sourceIsSas;
    }

    public String getSourceServerField() {
        return sourceServerField.getText();
    }

    public String getSourceUserField() {
        return sourceUserField.getText();
    }

    public String getSourcePasswordField() {
        return sourcePasswordField.getText();
    }
    public String getSourceDatabaseField() {
        return sourceDatabaseField.getText();
    }

    public boolean isSourceDatabaseFieldEnabled() {
        return sourceDatabaseField.isEnabled();
    }

    public DbType getCurrentDbChoice() {
        return this.currentDbType;
    }

    private void pickFolder() {
        JFileChooser fileChooser = new JFileChooser(new File(folderField.getText()));
        fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        int returnVal = fileChooser.showDialog(parentFrame, "Select folder");
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            File selectedDirectory = fileChooser.getSelectedFile();
            if (!selectedDirectory.exists()) {
                // When no directory is selected when approving, FileChooser incorrectly appends the current directory to the path.
                // Take the opened directory instead.
                selectedDirectory = fileChooser.getCurrentDirectory();
            }
            folderField.setText(selectedDirectory.getAbsolutePath());
        }
    }
}
