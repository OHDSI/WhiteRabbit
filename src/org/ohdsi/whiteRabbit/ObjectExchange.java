/*******************************************************************************
 * Copyright 2017 Observational Health Data Sciences and Informatics
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
package org.ohdsi.whiteRabbit;

import javax.swing.JFrame;
import javax.swing.undo.UndoManager;

import org.ohdsi.rabbitInAHat.DetailsPanel;
import org.ohdsi.rabbitInAHat.MappingPanel;
import org.ohdsi.rabbitInAHat.dataModel.ETL;

/**
 * This class is used to hold global variables
 */
public class ObjectExchange {
	public static JFrame		frame;
	public static UndoManager	undoManager;
	public static MappingPanel	tableMappingPanel;
	public static MappingPanel	fieldMappingPanel;
	public static DetailsPanel	detailsPanel;
	public static Console		console;
	public static ETL			etl;
}
