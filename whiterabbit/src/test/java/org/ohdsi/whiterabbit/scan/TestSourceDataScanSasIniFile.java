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

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.ohdsi.databases.configuration.DbType;
import org.ohdsi.whiterabbit.WhiteRabbitMain;
import org.opentest4j.AssertionFailedError;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSourceDataScanSasIniFile {
    @Test
    void testSourceDataScanFromIniFile(@TempDir Path tempDir) throws URISyntaxException, IOException {
        Charset charset = StandardCharsets.UTF_8;
        Path iniFile = tempDir.resolve("sas.ini");
        URL iniTemplate = TestSourceDataScanSasIniFile.class.getClassLoader().getResource("scan_data/sas.ini.template");
        URL referenceScanReport = TestSourceDataScanSasIniFile.class.getClassLoader().getResource("scan_data/ScanReport-reference-v0.10.7-sas.xlsx");
        assertNotNull(iniTemplate);
        String content = new String(Files.readAllBytes(Paths.get(iniTemplate.toURI())), charset);
        content = content.replace("%WORKING_FOLDER%", tempDir.toString());
        FileUtils.copyDirectory(
                new File(Objects.requireNonNull(TestSourceDataScanSasIniFile.class.getClassLoader().getResource("examples/wr_input_sas")).toURI()),
                tempDir.toFile());
        Files.write(iniFile, content.getBytes(charset));
        WhiteRabbitMain wrMain = new WhiteRabbitMain(false, new String[]{"-ini", iniFile.toAbsolutePath().toString()});
        assertNotNull(referenceScanReport);
        assertTrue(ScanTestUtils.scanResultsSheetMatchesReference(tempDir.resolve("ScanReport.xlsx"), Paths.get(referenceScanReport.toURI()), DbType.SAS7BDAT));
    }
}
