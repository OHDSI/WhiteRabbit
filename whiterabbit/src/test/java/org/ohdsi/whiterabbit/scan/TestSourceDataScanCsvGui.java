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
import org.assertj.swing.edt.GuiActionRunner;
import org.assertj.swing.fixture.FrameFixture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.whiterabbit.Console;
import org.ohdsi.whiterabbit.WhiteRabbitMain;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.extension.ExtendWith;
import org.ohdsi.whiterabbit.gui.LocationsPanel;

@ExtendWith(GUITestExtension.class)
@CacioTest
public class TestSourceDataScanCsvGui {
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

    @Test
    void testSourceDataScanFromGui(@TempDir Path tempDir) throws IOException, URISyntaxException {
        URL referenceScanReport = TestSourceDataScanCsvGui.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-csv.xlsx");
        Path personCsv = Paths.get(TestSourceDataScanCsvGui.class.getClassLoader().getResource("scan_data/person-header.csv").toURI());
        Path costCsv = Paths.get(TestSourceDataScanCsvGui.class.getClassLoader().getResource("scan_data/cost-header.csv").toURI());
        Files.copy(personCsv, tempDir.resolve("person.csv"));
        Files.copy(costCsv, tempDir.resolve("cost.csv"));
        window.tabbedPane("TabbedPane").selectTab(WhiteRabbitMain.LABEL_LOCATIONS);
        window.comboBox("SourceType").selectItem(DbType.DELIMITED_TEXT_FILES.label());
        window.textBox(LocationsPanel.NAME_DELIMITER).setText(",");
        window.textBox("FolderField").setText(tempDir.toAbsolutePath().toString());
        window.tabbedPane("TabbedPane").selectTab("Scan");
        window.button("Add").click();
        window.fileChooser("FileChooser").fileNameTextBox().setText("\"cost.csv\" \"person.csv\"");
        window.fileChooser("FileChooser").approveButton().click();
        window.button(WhiteRabbitMain.LABEL_SCAN_TABLES).click();

        assertTrue(ScanTestUtils.isScanReportGeneratedAndMatchesReference(
                console,
                tempDir.resolve("ScanReport.xlsx"),
                Paths.get(referenceScanReport.toURI()),
                DbType.DELIMITED_TEXT_FILES));
    }
}
