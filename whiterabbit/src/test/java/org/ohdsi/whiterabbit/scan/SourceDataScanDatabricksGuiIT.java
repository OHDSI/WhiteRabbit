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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.DatabricksHandler.DatabricksConfiguration;
import org.ohdsi.whiterabbit.Console;
import org.ohdsi.whiterabbit.WhiteRabbitMain;

import javax.swing.*;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.ohdsi.databases.configuration.DbType.DATABRICKS;
import static org.ohdsi.whiterabbit.scan.SourceDataScanDatabricksIT.*;

@ExtendWith(GUITestExtension.class)
@CacioTest
class SourceDataScanDatabricksGuiIT {

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
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("databricks.env"), "Databricks environment file not available");
        String[] args = {};
        WhiteRabbitMain whiteRabbitMain = GuiActionRunner.execute(() -> new WhiteRabbitMain(true, args));
        console = whiteRabbitMain.getConsole();
        window = new FrameFixture(whiteRabbitMain.getFrame());
        window.show(); // shows the frame to test
    }

    @ExtendWith(GUITestExtension.class)
    @Test
    void testConnectionAndSourceDataScan(@TempDir Path tempDir) throws IOException, URISyntaxException {
        Assumptions.assumeTrue(new ScanTestUtils.PropertiesFileChecker("databricks.env"), "Databricks environment file not available");
        URL referenceScanReport = TestSourceDataScanCsvGui.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sql.xlsx");
        window.tabbedPane(WhiteRabbitMain.NAME_TABBED_PANE).selectTab(WhiteRabbitMain.LABEL_LOCATIONS);
        window.comboBox("SourceType").selectItem(DATABRICKS.label());
        window.textBox("FolderField").setText(tempDir.toAbsolutePath().toString());

        // first use the test connection button, and expect a popup that informs us that several required fields are empty
        // use the "Test connection" button
        window.button(WhiteRabbitMain.LABEL_TEST_CONNECTION).click();
        GenericTypeMatcher<JDialog> matcher = new GenericTypeMatcher<JDialog>(JDialog.class, true) {
            protected boolean isMatching(JDialog frame) {
                return WhiteRabbitMain.TITLE_ERRORS_IN_DATABASE_CONFIGURATION.equals(frame.getTitle());
            }
        };
        DialogFixture frame = WindowFinder.findDialog(matcher).using(window.robot());
        frame.button().click(); // close the popup

        // check that the right tooltip is shown for the Databricks server field (cosmetic test)
        assertEquals(DatabricksConfiguration.TOOLTIP_DATABRICKS_SERVER, window.textBox(DatabricksConfiguration.DATABRICKS_SERVER).target().getToolTipText());
        // fill in all the required values and try again
        window.textBox(DatabricksConfiguration.DATABRICKS_SERVER).setText(ScanTestUtils.getPropertyOrFail(ENV_SERVER_LABEL));
        window.textBox(DatabricksConfiguration.DATABRICKS_HTTP_PATH).setText(ScanTestUtils.getPropertyOrFail(ENV_HTTP_PATH_LABEL));
        window.textBox(DatabricksConfiguration.DATABRICKS_PERSONAL_ACCESS_TOKEN).setText(ScanTestUtils.getPropertyOrFail(ENV_PERSONAL_ACCESS_TOKEN_LABEL));
        window.textBox(DatabricksConfiguration.DATABRICKS_CATALOG).setText(ScanTestUtils.getPropertyOrFail(ENV_CATALOG_LABEL));
        window.textBox(DatabricksConfiguration.DATABRICKS_SCHEMA).setText(ScanTestUtils.getPropertyOrFail(ENV_SCHEMA_LABEL));

        // use the "Test connection" button
        window.button(WhiteRabbitMain.LABEL_TEST_CONNECTION).click();
        matcher = new GenericTypeMatcher<JDialog>(JDialog.class, true) {
            protected boolean isMatching(JDialog frame) {
                return WhiteRabbitMain.LABEL_CONNECTION_SUCCESSFUL.equals(frame.getTitle());
            }
        };
        frame = WindowFinder.findDialog(matcher).using(window.robot());
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
                DATABRICKS));
    }
}
