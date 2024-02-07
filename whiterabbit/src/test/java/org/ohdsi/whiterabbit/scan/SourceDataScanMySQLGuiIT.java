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
package org.ohdsi.whiterabbit.scan;

import com.github.caciocavallosilano.cacio.ctc.junit.CacioTest;
import org.assertj.swing.core.GenericTypeMatcher;
import org.assertj.swing.edt.GuiActionRunner;
import org.assertj.swing.finder.WindowFinder;
import org.assertj.swing.fixture.DialogFixture;
import org.assertj.swing.fixture.FrameFixture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.whiterabbit.Console;
import org.ohdsi.whiterabbit.WhiteRabbitMain;
import org.ohdsi.whiterabbit.gui.LocationsPanel;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;

import javax.swing.*;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.ohdsi.databases.configuration.DbType.MYSQL;
import static org.ohdsi.whiterabbit.scan.SourceDataScanMySQLIT.createMySQLContainer;

@ExtendWith(GUITestExtension.class)
@CacioTest
class SourceDataScanMySQLGuiIT {

    private static FrameFixture window;
    private static Console console;

    private final static int WIDTH = 1920;
    private final static int HEIGHT = 1080;
    @BeforeAll
    public static void setupOnce() {
        System.setProperty("cacio.managed.screensize", String.format("%sx%s", WIDTH, HEIGHT));
    }

    @BeforeEach
    public void onSetUp() {
        String[] args = {};
        WhiteRabbitMain whiteRabbitMain = GuiActionRunner.execute(() -> new WhiteRabbitMain(true, args));
        console = whiteRabbitMain.getConsole();
        window = new FrameFixture(whiteRabbitMain.getFrame());
        window.show(); // shows the frame to test
    }

    @Container
    public static MySQLContainer<?> mySQLContainer = createMySQLContainer();

    @ExtendWith(GUITestExtension.class)
    @Test
    void testConnectionAndSourceDataScan(@TempDir Path tempDir) throws IOException, URISyntaxException {
        URL referenceScanReport = TestSourceDataScanCsvGui.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");
        window.tabbedPane(WhiteRabbitMain.NAME_TABBED_PANE).selectTab(WhiteRabbitMain.LABEL_LOCATIONS);
        window.comboBox("SourceType").selectItem(DbType.MYSQL.label());
        window.textBox("FolderField").setText(tempDir.toAbsolutePath().toString());
        // verify one tooltip text, assume that all other tooltip texts will be fine too (fingers crossed)
        assertEquals(LocationsPanel.TOOLTIP_DATABASE_SERVER, window.textBox(LocationsPanel.LABEL_SERVER_LOCATION).target().getToolTipText());
        window.textBox(LocationsPanel.LABEL_SERVER_LOCATION).setText(String.format("%s:%s",
                mySQLContainer.getHost(),
                mySQLContainer.getFirstMappedPort()));
        window.textBox(LocationsPanel.LABEL_USER_NAME).setText(mySQLContainer.getUsername());
        window.textBox(LocationsPanel.LABEL_PASSWORD).setText(mySQLContainer.getPassword());
        window.textBox(LocationsPanel.LABEL_DATABASE_NAME).setText(mySQLContainer.getDatabaseName());

        // use the "Test connection" button
        window.button(WhiteRabbitMain.LABEL_TEST_CONNECTION).click();
        GenericTypeMatcher<JDialog> matcher = new GenericTypeMatcher<JDialog>(JDialog.class, true) {
            protected boolean isMatching(JDialog frame) {
                return WhiteRabbitMain.LABEL_CONNECTION_SUCCESSFUL.equals(frame.getTitle());
            }
        };
        DialogFixture frame = WindowFinder.findDialog(matcher).using(window.robot());
        frame.button().click();

        // switch to the scan panel, add all tables found and run the scan
        window.tabbedPane(WhiteRabbitMain.NAME_TABBED_PANE).selectTab(WhiteRabbitMain.LABEL_SCAN).click();
        window.button(WhiteRabbitMain.LABEL_ADD_ALL_IN_DB).click();
        window.button(WhiteRabbitMain.LABEL_SCAN_TABLES).click();

        // verify the generated scan report against the reference
        assertTrue(ScanTestUtils.isScanReportGeneratedAndMatchesReference(
                console,
                tempDir.resolve("ScanReport.xlsx"),
                Paths.get(referenceScanReport.toURI()),
                MYSQL));

        //window.close();
    }
}
