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
package org.ohdsi.databases;

public interface ScanParameters {

    public boolean doCalculateNumericStats();

    public int getNumStatsSamplerSize();

    public int getMaxValues();

    public boolean doScanValues();

    public int getMinCellCount();

    public int getSampleSize();

    public static int	MAX_VALUES_IN_MEMORY				= 100000;
    public static int	MIN_CELL_COUNT_FOR_CSV				= 1000000;
    public static int	N_FOR_FREE_TEXT_CHECK				= 1000;
    public static int	MIN_AVERAGE_LENGTH_FOR_FREE_TEXT	= 100;
}
